import os
import pandas as pd
import numpy as np
import pyodbc
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Iterator
import logging
import urllib.parse
from sqlalchemy import create_engine, text
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SourceConnectionManager:    
    def __init__(self):
        self.server = os.getenv("SPACEPARTS_SERVER")
        self.database = os.getenv("SPACEPARTS_DATABASE") 
        self.username = os.getenv("SPACEPARTS_USERNAME")
        self.password = os.getenv("SPACEPARTS_PASSWORD")
        
        if not all([self.server, self.database, self.username, self.password]):
            missing = [var for var, val in [
                ("SPACEPARTS_SERVER", self.server),
                ("SPACEPARTS_DATABASE", self.database), 
                ("SPACEPARTS_USERNAME", self.username),
                ("SPACEPARTS_PASSWORD", self.password)
            ] if not val]
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def get_connection_string(self):
        return (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.server};DATABASE={self.database};"
            f"UID={self.username};PWD={self.password};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
    
    def get_sqlalchemy_url(self):
        odbc_str = self.get_connection_string()
        return f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
    
    def get_engine(self):
        return create_engine(self.get_sqlalchemy_url())

class PostgreSQLManager:    
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST")
        self.port = int(os.getenv("POSTGRES_PORT")) if os.getenv("POSTGRES_PORT") else None
        self.database = os.getenv("POSTGRES_DB")
        self.username = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        
        if not all([self.host, self.port, self.database, self.username, self.password]):
            missing = [var for var, val in [
                ("POSTGRES_HOST", self.host),
                ("POSTGRES_PORT", self.port),
                ("POSTGRES_DB", self.database),
                ("POSTGRES_USER", self.username),
                ("POSTGRES_PASSWORD", self.password)
            ] if not val]
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def get_connection_string(self):
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_engine(self):
        return create_engine(self.get_connection_string())

class VolumeAwareProcessor:
    """Determina estrategia de procesamiento basada en volumen de datos"""
    
    CHUNK_THRESHOLD = 1000000  # 1M registros
    LARGE_TABLE_CHUNK_SIZE = 50000
    MEDIUM_TABLE_CHUNK_SIZE = 100000
    SMALL_TABLE_FULL_LOAD = True
    
    @classmethod
    def get_processing_strategy(cls, row_count: int) -> Dict:
        """Determina estrategia de procesamiento según volumen"""
        if row_count >= cls.CHUNK_THRESHOLD:
            if row_count > 10000000:  # 10M+
                chunk_size = 25000
                strategy = "large_chunks"
            elif row_count > 5000000:  # 5M+
                chunk_size = 35000
                strategy = "medium_chunks"
            else:  # 1M-5M
                chunk_size = cls.LARGE_TABLE_CHUNK_SIZE
                strategy = "standard_chunks"
            
            return {
                "processing_mode": "streaming",
                "chunk_size": chunk_size,
                "strategy": strategy,
                "memory_optimization": True
            }
        else:
            return {
                "processing_mode": "full_load",
                "chunk_size": None,
                "strategy": "complete_table",
                "memory_optimization": False
            }

class ColumnNameCleaner:
    FORBIDDEN_CHARS = r"[ ,;{}\(\)\n\t=]+"
    RESERVED_WORDS = {
        "select", "from", "where", "group", "order", "by", "having", "limit", "offset",
        "and", "or", "not", "as", "on", "join", "inner", "left", "right", "full", "cross",
        "desc", "asc", "table", "column", "index", "view", "database", "schema", "create",
        "drop", "alter", "insert", "update", "delete", "merge", "into", "values", "set",
        "case", "when", "then", "else", "end", "union", "all", "distinct", "true", "false",
        "null"
    }
    
    @staticmethod
    def strip_accents(text: str) -> str:
        normalized = unicodedata.normalize("NFKD", str(text))
        return "".join([c for c in normalized if not unicodedata.combining(c)])
    
    @classmethod
    def clean_identifier(cls, name: str) -> str:
        if name is None:
            return "col"
        
        s = cls.strip_accents(str(name).strip())
        s = re.sub(cls.FORBIDDEN_CHARS, "_", s)
        s = s.replace(".", "_").replace("-", "_").replace("/", "_").replace("\\", "_")
        s = re.sub(r"[^0-9a-zA-Z_]", "", s)
        s = re.sub(r"_+", "_", s).strip("_").lower()
        
        if re.match(r"^[0-9]", s):
            s = "c_" + s
        
        if s in cls.RESERVED_WORDS:
            s = s + "_col"
        
        if not s:
            s = "col"
        
        return s[:63]
    
    @classmethod
    def clean_table_name(cls, schema: str, table: str) -> str:
        schema_clean = cls.clean_identifier(schema)
        table_clean = cls.clean_identifier(table)
        return f"bronze_{schema_clean}_{table_clean}"

class DataProfiler:
    def __init__(self, source_manager: SourceConnectionManager):
        self.source_manager = source_manager
    
    def list_tables(self) -> List[str]:
        query = """
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE='BASE TABLE' 
            AND TABLE_SCHEMA IN ('dim', 'fact')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        with self.source_manager.get_engine().connect() as conn:
            result = conn.execute(text(query))
            return [f"{row[0]}.{row[1]}" for row in result.fetchall()]
    
    def get_row_count(self, schema: str, table: str) -> int:
        query = f"""
            SELECT SUM(p.rows) as row_count
            FROM sys.objects o
            JOIN sys.indexes i ON i.object_id = o.object_id
            JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
            WHERE o.object_id = OBJECT_ID(N'[{schema}].[{table}]') 
            AND o.type = 'U'
            AND i.index_id IN (0,1)
        """
        
        try:
            engine = self.source_manager.get_engine()
            result = pd.read_sql(query, engine)
            return int(result.iloc[0]['row_count']) if not result.empty else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {schema}.{table}: {e}")
            return 0

class OptimizedDataExtractor:
    def __init__(self, source_manager: SourceConnectionManager):
        self.source_manager = source_manager
    
    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            for col in df.columns:
                col_type = df[col].dtype
                
                if col_type == 'object':
                    if len(df) > 0 and df[col].nunique() / len(df) < 0.5:
                        df[col] = df[col].astype('category')
                elif col_type == 'int64':
                    c_min = df[col].min()
                    c_max = df[col].max()
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                elif col_type == 'float64':
                    df[col] = pd.to_numeric(df[col], downcast='float')
        except Exception as e:
            logger.warning(f"Error optimizing dtypes: {e}")
        
        return df
    
    def extract_full_table(self, schema: str, table: str) -> pd.DataFrame:
        """Extrae tabla completa para tablas pequeñas"""
        query = f"SELECT * FROM [{schema}].[{table}]"
        
        try:
            engine = self.source_manager.get_engine()
            df = pd.read_sql(query, engine)
            df = self.optimize_dtypes(df)
            logger.info(f"Full load completed: {len(df)} rows")
            return df
                
        except Exception as e:
            logger.error(f"Error extracting full table {schema}.{table}: {e}")
            raise
    
    def extract_table_streaming(self, schema: str, table: str, chunk_size: int) -> Iterator[pd.DataFrame]:
        """Extrae tabla por chunks para tablas grandes"""
        query = f"SELECT * FROM [{schema}].[{table}]"
        
        try:
            engine = self.source_manager.get_engine()
            chunk_iterator = pd.read_sql(query, engine, chunksize=chunk_size)
            
            chunk_count = 0
            for chunk in chunk_iterator:
                chunk_count += 1
                logger.info(f"Processing chunk {chunk_count} with {len(chunk)} rows")
                
                chunk = self.optimize_dtypes(chunk)
                yield chunk
                
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error extracting streaming data from {schema}.{table}: {e}")
            raise

class AdaptiveDataLoader:
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.cleaner = ColumnNameCleaner()
        self.engine = None

    def get_engine(self):
        if self.engine is None:
            self.engine = self.postgres_manager.get_engine()
        return self.engine
    
    def safe_table_name(self, table_name: str) -> str:
        return f'"{table_name}"' if not table_name.startswith('"') else table_name

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepara DataFrame con metadatos estándar"""
        column_mapping = {col: self.cleaner.clean_identifier(col) for col in df.columns}
        df_clean = df.rename(columns=column_mapping)
        
        lower_cols = [c.lower() for c in df_clean.columns]
        if 'load_date' not in lower_cols:
            df_clean['load_date'] = datetime.now()
        if 'source_system' not in lower_cols:
            df_clean['source_system'] = 'spaceparts'
        
        return df_clean, column_mapping

    def load_full_table(self, df: pd.DataFrame, table_name: str) -> int:
        """Carga tabla completa para volúmenes pequeños"""
        try:
            engine = self.get_engine()
            df_clean, _ = self.prepare_dataframe(df)
            
            df_clean.to_sql(
                name=table_name,
                con=engine,
                schema=None,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=10000
            )
            
            logger.info(f"Full load completed: {len(df_clean):,} rows to {table_name}")
            return len(df_clean)
            
        except Exception as e:
            logger.error(f"Error in full load to {table_name}: {e}")
            raise

    def create_table_structure(self, df_sample: pd.DataFrame, table_name: str):
        """Crea estructura de tabla con muestra"""
        try:
            engine = self.get_engine()
            df_clean, column_mapping = self.prepare_dataframe(df_sample)
            
            df_clean.head(1).to_sql(
                name=table_name,
                con=engine,
                schema=None,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            with engine.begin() as conn:
                safe_name = self.safe_table_name(table_name)
                conn.execute(text(f"DELETE FROM {safe_name}"))
            
            logger.info(f"Table structure created: {table_name}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def load_chunk(self, df_chunk: pd.DataFrame, table_name: str, column_mapping: dict) -> int:
        """Carga un chunk individual"""
        try:
            engine = self.get_engine()
            df_clean = df_chunk.rename(columns=column_mapping)
            
            lower_cols = [c.lower() for c in df_clean.columns]
            if 'load_date' not in lower_cols:
                df_clean['load_date'] = datetime.now()
            if 'source_system' not in lower_cols:
                df_clean['source_system'] = 'spaceparts'
            
            df_clean.to_sql(
                name=table_name,
                con=engine,
                schema=None,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            return len(df_clean)
            
        except Exception as e:
            logger.error(f"Error loading chunk to {table_name}: {e}")
            raise

    def load_streaming_data(self, data_iterator: Iterator[pd.DataFrame], table_name: str) -> int:
        """Carga datos por streaming"""
        total_records = 0
        column_mapping = None
        first_chunk = True
        
        try:
            for chunk in data_iterator:
                if first_chunk:
                    column_mapping = self.create_table_structure(chunk, table_name)
                    first_chunk = False
                
                records_loaded = self.load_chunk(chunk, table_name, column_mapping)
                total_records += records_loaded
                
                logger.info(f"Chunk loaded: {records_loaded:,} rows. Total: {total_records:,}")
                
                del chunk
                gc.collect()
            
            logger.info(f"Streaming completed: {total_records:,} rows to {table_name}")
            return total_records
            
        except Exception as e:
            logger.error(f"Error in streaming load to {table_name}: {e}")
            raise

class BronzeProcessor:
    """Procesador principal Bronze con optimización por volumen"""
    
    def __init__(self):
        self.source_manager = SourceConnectionManager()
        self.postgres_manager = PostgreSQLManager()
        self.profiler = DataProfiler(self.source_manager)
        self.extractor = OptimizedDataExtractor(self.source_manager)
        self.loader = AdaptiveDataLoader(self.postgres_manager)
        self.cleaner = ColumnNameCleaner()
        self.volume_processor = VolumeAwareProcessor()
    
    def get_table_list(self) -> List[str]:
        """Obtiene lista de tablas a procesar"""
        try:
            tables = self.profiler.list_tables()
            logger.info(f"Found {len(tables)} tables to process")
            return tables
        except Exception as e:
            logger.error(f"Error getting table list: {e}")
            raise
    
    def process_single_table(self, table_name: str) -> Dict:
        """Procesa una tabla con estrategia adaptativa"""
        try:
            logger.info(f"Starting Bronze extraction for table: {table_name}")
            
            schema, table = table_name.split('.', 1)
            row_count = self.profiler.get_row_count(schema, table)
            logger.info(f"Table {table_name} has {row_count:,} rows")
            
            if row_count == 0:
                logger.warning(f"Skipping empty table: {table_name}")
                return {
                    'source_table': table_name,
                    'bronze_table': self.cleaner.clean_table_name(schema, table),
                    'record_count': 0,
                    'status': 'skipped',
                    'message': 'Empty table'
                }
            
            # Determinar estrategia de procesamiento
            strategy = self.volume_processor.get_processing_strategy(row_count)
            bronze_table_name = self.cleaner.clean_table_name(schema, table)
            
            logger.info(f"Processing strategy: {strategy['strategy']} (mode: {strategy['processing_mode']})")
            
            # Ejecutar según estrategia
            if strategy['processing_mode'] == 'full_load':
                # Tablas pequeñas: carga completa
                df = self.extractor.extract_full_table(schema, table)
                records_loaded = self.loader.load_full_table(df, bronze_table_name)
                
            else:
                # Tablas grandes: streaming por chunks
                chunk_size = strategy['chunk_size']
                logger.info(f"Using streaming with chunk size: {chunk_size:,}")
                
                data_iterator = self.extractor.extract_table_streaming(schema, table, chunk_size)
                records_loaded = self.loader.load_streaming_data(data_iterator, bronze_table_name)
            
            result = {
                'source_table': table_name,
                'bronze_table': bronze_table_name,
                'record_count': records_loaded,
                'status': 'success',
                'processing_strategy': strategy['strategy'],
                'processing_mode': strategy['processing_mode'],
                'extraction_time': datetime.now().isoformat()
            }
            
            if strategy['processing_mode'] == 'streaming':
                result['chunk_size_used'] = strategy['chunk_size']
            
            logger.info(f"Successfully processed {table_name}: {records_loaded:,} records using {strategy['strategy']}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")
            schema, table = table_name.split('.', 1)
            return {
                'source_table': table_name,
                'bronze_table': self.cleaner.clean_table_name(schema, table),
                'record_count': 0,
                'status': 'failed',
                'error': str(e),
                'extraction_time': datetime.now().isoformat()
            }
    
    def execute_bronze_pipeline(self) -> Dict:
        """Ejecuta pipeline Bronze con optimización automática"""
        try:
            logger.info("=" * 80)
            logger.info("INICIANDO BRONZE LAYER PIPELINE - VOLUME OPTIMIZED")
            logger.info("=" * 80)
            
            table_list = self.get_table_list()
            
            # Clasificar tablas por volumen para estadísticas
            large_tables = []
            small_tables = []
            
            for table_name in table_list:
                schema, table = table_name.split('.', 1)
                row_count = self.profiler.get_row_count(schema, table)
                if row_count >= VolumeAwareProcessor.CHUNK_THRESHOLD:
                    large_tables.append(table_name)
                else:
                    small_tables.append(table_name)
            
            logger.info(f"Processing plan: {len(large_tables)} large tables (streaming), {len(small_tables)} small tables (full load)")
            
            results = []
            total_records = 0
            successful_tables = 0
            failed_tables = 0
            streaming_tables = 0
            full_load_tables = 0
            
            # Procesar cada tabla
            for table_name in table_list:
                try:
                    result = self.process_single_table(table_name)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        successful_tables += 1
                        total_records += result['record_count']
                        
                        if result.get('processing_mode') == 'streaming':
                            streaming_tables += 1
                        else:
                            full_load_tables += 1
                    else:
                        failed_tables += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process {table_name}: {e}")
                    failed_tables += 1
                    results.append({
                        'source_table': table_name,
                        'bronze_table': f"bronze_{table_name.replace('.', '_').replace('-', '_')}",
                        'record_count': 0,
                        'status': 'failed',
                        'error': str(e)
                    })
            
            # Crear resumen
            pipeline_result = {
                'execution_id': datetime.now().isoformat(),
                'total_tables': len(table_list),
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'skipped_tables': len([r for r in results if r['status'] == 'skipped']),
                'streaming_tables': streaming_tables,
                'full_load_tables': full_load_tables,
                'total_records': total_records,
                'status': 'completed' if failed_tables == 0 else 'completed_with_errors',
                'layer': 'bronze',
                'volume_optimization': True,
                'results': results
            }
            
            logger.info("=" * 80)
            logger.info("BRONZE LAYER RESUMEN:")
            logger.info(f"  Tablas exitosas: {successful_tables}")
            logger.info(f"  Tablas fallidas: {failed_tables}")
            logger.info(f"  Procesamiento streaming: {streaming_tables}")
            logger.info(f"  Carga completa: {full_load_tables}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 80)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Bronze pipeline: {e}")
            raise