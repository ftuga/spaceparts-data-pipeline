import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Iterator
import logging
from sqlalchemy import create_engine, text
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class VolumeAnalyzer:
    """Analiza volumen de tablas para determinar estrategia de procesamiento"""
    
    CHUNK_THRESHOLD = 1000000  # 1M registros
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
    def get_table_row_count(self, table_name: str) -> int:
        """Obtiene conteo de filas de una tabla"""
        try:
            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            result = pd.read_sql(query, self.engine)
            return int(result.iloc[0]['row_count'])
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {e}")
            return 0
    
    def get_processing_strategy(self, table_name: str, row_count: int) -> Dict:
        """Determina estrategia de procesamiento"""
        if row_count >= self.CHUNK_THRESHOLD:
            if row_count > 10000000:  # 10M+
                chunk_size = 100000
                strategy = "large_volume_chunks"
            elif row_count > 5000000:  # 5M+
                chunk_size = 150000
                strategy = "medium_volume_chunks"
            else:  # 1M-5M
                chunk_size = 200000
                strategy = "standard_chunks"
            
            return {
                "processing_mode": "chunked",
                "chunk_size": chunk_size,
                "strategy": strategy,
                "requires_streaming": True
            }
        else:
            return {
                "processing_mode": "full_table",
                "chunk_size": None,
                "strategy": "complete_load",
                "requires_streaming": False
            }

class ChunkedDataReader:
    """Lector de datos por chunks desde PostgreSQL"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
    def read_table_chunks(self, table_name: str, chunk_size: int) -> Iterator[pd.DataFrame]:
        """Lee tabla por chunks usando OFFSET/LIMIT"""
        try:
            offset = 0
            chunk_count = 0
            
            while True:
                query = f"""
                    SELECT * FROM {table_name} 
                    ORDER BY load_date 
                    LIMIT {chunk_size} OFFSET {offset}
                """
                
                chunk = pd.read_sql(query, self.engine)
                
                if chunk.empty:
                    break
                
                chunk_count += 1
                logger.info(f"Read chunk {chunk_count} with {len(chunk)} rows from {table_name}")
                
                yield chunk
                
                offset += chunk_size
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error reading chunks from {table_name}: {e}")
            raise
    
    def read_full_table(self, table_name: str) -> pd.DataFrame:
        """Lee tabla completa para tablas pequeñas"""
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.engine)
            logger.info(f"Read full table {table_name}: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading full table {table_name}: {e}")
            raise

class SilverDataTransformer:
    """Transformador de datos para la capa Silver"""
    
    def __init__(self):
        pass
    
    def standardize_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Estandariza tipos de datos"""
        try:
            for col in df.columns:
                
                # Procesar fechas
                if col.lower().endswith('date') and df[col].dtype in ['int64', 'object']:
                    try:
                        if df[col].dtype == 'int64':
                            # Manejar timestamps Unix
                            mask_ns = df[col] > 1000000000000
                            df.loc[mask_ns, col] = pd.to_datetime(df.loc[mask_ns, col], unit='ns', errors='coerce')
                            df.loc[~mask_ns, col] = pd.to_datetime(df.loc[~mask_ns, col], unit='s', errors='coerce')
                        else:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Could not convert {col} to datetime: {e}")
                
                # Procesar columna month especial
                elif col.lower() == 'month' and df[col].dtype == 'int64':
                    try:
                        df[col] = pd.to_datetime(df[col], unit='ns', errors='coerce')
                    except:
                        pass
                
                # Procesar dwcreateddate
                elif col.lower() == 'dwcreateddate' and df[col].dtype != 'datetime64[ns]':
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        pass
                
                # Procesar columnas de llaves
                elif col.lower().endswith(('_key', 'key')) and df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip().str.upper()
                    df[col] = df[col].replace(['', 'NULL', 'N/A', 'UNKNOWN', 'NONE'], None)
                
                # Procesar strings generales
                elif df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace(['', 'NULL', 'N/A', 'UNKNOWN', 'NONE', '#N/A'], None)
                
                # Procesar valores numéricos
                elif df[col].dtype in ['float64', 'float32']:
                    df[col] = df[col].replace([np.inf, -np.inf], None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error standardizing data types for {table_name}: {e}")
            return df
    
    def remove_duplicates_chunk_aware(self, df: pd.DataFrame, table_name: str, is_final_chunk: bool = False) -> pd.DataFrame:
        """Elimina duplicados considerando que es procesamiento por chunks"""
        try:
            initial_count = len(df)
            
            if 'silver_dim_' in table_name:
                # Para dimensiones, eliminar duplicados dentro del chunk
                cols_except_created = [c for c in df.columns if 'created' not in c.lower() and 'load_date' not in c.lower()]
                if len(cols_except_created) > 0:
                    df = df.drop_duplicates(subset=cols_except_created, keep='last')
            
            elif 'silver_fact_' in table_name:
                # Para hechos, eliminar duplicados por columnas clave dentro del chunk
                key_columns = []
                for col in df.columns:
                    if any(pattern in col.lower() for pattern in ['_key', '_number', 'customer', 'product', 'document']):
                        key_columns.append(col)
                
                if key_columns:
                    df = df.drop_duplicates(subset=key_columns, keep='last')
                else:
                    df = df.drop_duplicates(keep='last')
            
            final_count = len(df)
            duplicates_removed = initial_count - final_count
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicates from chunk in {table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error removing duplicates from {table_name}: {e}")
            return df
    
    def data_quality_checks(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Aplica verificaciones de calidad de datos"""
        try:
            initial_count = len(df)
            
            # Eliminar filas completamente vacías
            df_clean = df.dropna(how='all')
            
            # Filtrar fechas futuras problemáticas
            future_cutoff = pd.Timestamp.now() + pd.DateOffset(years=2)
            
            for col in df_clean.select_dtypes(include=[np.datetime64]).columns:
                if col.lower() != 'load_date':
                    problematic_mask = df_clean[col] > future_cutoff
                    if problematic_mask.any():
                        problematic_count = problematic_mask.sum()
                        logger.info(f"Quarantined {problematic_count} records with future dates in {col}")
                        df_clean = df_clean[~problematic_mask]
            
            # Filtrar valores numéricos extremos
            for col in df_clean.select_dtypes(include=[np.number]).columns:
                if 'value' in col.lower() or 'amount' in col.lower():
                    extreme_mask = (df_clean[col] > 100000000) | (df_clean[col] < -10000000)
                    if extreme_mask.any():
                        extreme_count = extreme_mask.sum()
                        logger.info(f"Quarantined {extreme_count} records with extreme values in {col}")
                        df_clean = df_clean[~extreme_mask]
            
            final_count = len(df_clean)
            processed_rows = initial_count - final_count
            
            if processed_rows > 0:
                logger.info(f"Quality checks processed {processed_rows} problematic rows from chunk")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error in data quality checks for {table_name}: {e}")
            return df
    
    def optimize_chunk_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimiza memoria del chunk"""
        try:
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].nunique() / len(df) < 0.5:
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
            
            gc.collect()
            return df
            
        except Exception as e:
            logger.error(f"Error optimizing chunk memory: {e}")
            return df

class ChunkedSilverProcessor:
    """Procesador Silver con capacidad de streaming por chunks"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
        self.transformer = SilverDataTransformer()
    
    def create_silver_table_structure(self, sample_df: pd.DataFrame, silver_table_name: str):
        """Crea estructura de tabla Silver con muestra"""
        try:
            # Agregar metadatos Silver
            sample_df['silver_created_date'] = datetime.now()
            sample_df['silver_execution_id'] = datetime.now().isoformat()
            
            # Crear tabla con muestra
            sample_df.head(1).to_sql(
                silver_table_name,
                self.engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            # Limpiar muestra
            with self.engine.begin() as conn:
                conn.execute(text(f'DELETE FROM "{silver_table_name}"'))
            
            logger.info(f"Silver table structure created: {silver_table_name}")
            
        except Exception as e:
            logger.error(f"Error creating Silver table structure {silver_table_name}: {e}")
            raise
    
    def process_chunk_to_silver(self, chunk_df: pd.DataFrame, silver_table_name: str, is_final_chunk: bool = False) -> int:
        """Procesa un chunk individual a Silver"""
        try:
            # Aplicar transformaciones Silver
            processed_chunk = self.transformer.standardize_data_types(chunk_df.copy(), silver_table_name)
            processed_chunk = self.transformer.remove_duplicates_chunk_aware(processed_chunk, silver_table_name, is_final_chunk)
            processed_chunk = self.transformer.data_quality_checks(processed_chunk, silver_table_name)
            processed_chunk = self.transformer.optimize_chunk_memory(processed_chunk)
            
            # Agregar metadatos Silver
            processed_chunk['silver_created_date'] = datetime.now()
            processed_chunk['silver_execution_id'] = datetime.now().isoformat()
            
            # Guardar chunk
            record_count = len(processed_chunk)
            processed_chunk.to_sql(
                silver_table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000
            )
            
            # Limpiar memoria
            del processed_chunk
            gc.collect()
            
            return record_count
            
        except Exception as e:
            logger.error(f"Error processing chunk to Silver {silver_table_name}: {e}")
            raise
    
    def process_full_table_to_silver(self, bronze_df: pd.DataFrame, silver_table_name: str) -> int:
        """Procesa tabla completa a Silver para tablas pequeñas"""
        try:
            # Aplicar transformaciones Silver
            processed_df = self.transformer.standardize_data_types(bronze_df.copy(), silver_table_name)
            processed_df = self.transformer.remove_duplicates_chunk_aware(processed_df, silver_table_name, True)
            processed_df = self.transformer.data_quality_checks(processed_df, silver_table_name)
            processed_df = self.transformer.optimize_chunk_memory(processed_df)
            
            # Agregar metadatos Silver
            processed_df['silver_created_date'] = datetime.now()
            processed_df['silver_execution_id'] = datetime.now().isoformat()
            
            # Guardar tabla completa
            record_count = len(processed_df)
            processed_df.to_sql(
                silver_table_name,
                self.engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=10000
            )
            
            logger.info(f"Full table processed to Silver: {record_count:,} records")
            return record_count
            
        except Exception as e:
            logger.error(f"Error processing full table to Silver {silver_table_name}: {e}")
            raise

class SilverDataProcessor:
    """Procesador principal de datos Silver"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
        self.volume_analyzer = VolumeAnalyzer(postgres_manager)
        self.chunked_reader = ChunkedDataReader(postgres_manager)
        self.chunked_processor = ChunkedSilverProcessor(postgres_manager)
    
    def get_bronze_tables(self) -> List[str]:
        """Obtiene lista de tablas Bronze disponibles"""
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'bronze_%'
                AND table_name NOT IN ('bronze_execution_log', 'bronze_notebook_execution_summary')
                ORDER BY table_name
            """
            
            result = pd.read_sql(query, self.engine)
            tables = result['table_name'].tolist()
            
            logger.info(f"Found {len(tables)} Bronze tables to process")
            return tables
            
        except Exception as e:
            logger.error(f"Error getting Bronze tables: {e}")
            raise
    
    def process_bronze_to_silver(self, bronze_table_name: str) -> Dict:
        """Procesa tabla Bronze a Silver con estrategia adaptativa"""
        try:
            silver_table_name = bronze_table_name.replace('bronze_', 'silver_')
            logger.info(f"Processing {bronze_table_name} -> {silver_table_name}")
            
            # Analizar volumen para determinar estrategia
            row_count = self.volume_analyzer.get_table_row_count(bronze_table_name)
            strategy = self.volume_analyzer.get_processing_strategy(bronze_table_name, row_count)
            
            logger.info(f"Volume: {row_count:,} rows, Strategy: {strategy['strategy']}")
            
            if row_count == 0:
                logger.warning(f"Bronze table {bronze_table_name} is empty")
                return {
                    'bronze_table': bronze_table_name,
                    'silver_table': silver_table_name,
                    'record_count': 0,
                    'status': 'empty',
                    'message': 'Bronze table is empty'
                }
            
            total_records = 0
            
            if strategy['processing_mode'] == 'full_table':
                # Procesamiento completo para tablas pequeñas
                bronze_df = self.chunked_reader.read_full_table(bronze_table_name)
                total_records = self.chunked_processor.process_full_table_to_silver(bronze_df, silver_table_name)
                
            else:
                # Procesamiento por chunks para tablas grandes
                chunk_size = strategy['chunk_size']
                logger.info(f"Processing {bronze_table_name} in chunks of {chunk_size:,} rows")
                
                first_chunk = True
                chunk_count = 0
                
                for chunk in self.chunked_reader.read_table_chunks(bronze_table_name, chunk_size):
                    chunk_count += 1
                    
                    if first_chunk:
                        # Crear estructura con primer chunk
                        self.chunked_processor.create_silver_table_structure(chunk.copy(), silver_table_name)
                        first_chunk = False
                    
                    # Procesar chunk
                    chunk_records = self.chunked_processor.process_chunk_to_silver(
                        chunk, silver_table_name, False
                    )
                    total_records += chunk_records
                    
                    logger.info(f"Processed chunk {chunk_count}: {chunk_records:,} records. Total: {total_records:,}")
            
            # Post-procesamiento para eliminar duplicados globales en tablas grandes
            if strategy['processing_mode'] == 'chunked' and 'dim_' in silver_table_name:
                logger.info(f"Removing global duplicates from {silver_table_name}")
                self.remove_global_duplicates(silver_table_name)
            
            result = {
                'bronze_table': bronze_table_name,
                'silver_table': silver_table_name,
                'record_count': total_records,
                'status': 'success',
                'processing_strategy': strategy['strategy'],
                'processing_mode': strategy['processing_mode'],
                'processing_time': datetime.now().isoformat()
            }
            
            if strategy['processing_mode'] == 'chunked':
                result['chunks_processed'] = chunk_count
                result['chunk_size'] = chunk_size
            
            logger.info(f"Successfully processed {silver_table_name}: {total_records:,} records using {strategy['strategy']}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing {bronze_table_name}: {str(e)}")
            return {
                'bronze_table': bronze_table_name,
                'silver_table': bronze_table_name.replace('bronze_', 'silver_'),
                'record_count': 0,
                'status': 'failed',
                'error': str(e),
                'processing_time': datetime.now().isoformat()
            }
    
    def remove_global_duplicates(self, silver_table_name: str):
        """Elimina duplicados globales después del procesamiento por chunks"""
        try:
            if 'dim_' in silver_table_name:
                # Para dimensiones, usar ROW_NUMBER para eliminar duplicados
                dedup_query = f"""
                    DELETE FROM "{silver_table_name}" 
                    WHERE ctid NOT IN (
                        SELECT ctid FROM (
                            SELECT ctid, ROW_NUMBER() OVER (
                                PARTITION BY customer_key, product_key 
                                ORDER BY silver_created_date DESC
                            ) as rn
                            FROM "{silver_table_name}"
                        ) t WHERE t.rn = 1
                    )
                """
                
                with self.engine.begin() as conn:
                    result = conn.execute(text(dedup_query))
                    logger.info(f"Global deduplication completed for {silver_table_name}")
                    
        except Exception as e:
            logger.warning(f"Error in global deduplication for {silver_table_name}: {e}")

class SilverProcessor:
    """Procesador principal de la capa Silver"""
    
    def __init__(self):
        self.postgres_manager = PostgreSQLManager()
        self.data_processor = SilverDataProcessor(self.postgres_manager)
    
    def execute_silver_pipeline(self, bronze_results: Dict = None) -> Dict:
        """Ejecuta el pipeline completo de Silver con optimización por volumen"""
        try:
            logger.info("=" * 80)
            logger.info("INICIANDO SILVER LAYER PIPELINE - CHUNK OPTIMIZED")
            logger.info("=" * 80)
            
            # Validar que Bronze esté disponible
            if bronze_results and bronze_results.get('successful_tables', 0) == 0:
                raise ValueError("No hay tablas Bronze exitosas disponibles")
            
            # Obtener lista de tablas Bronze
            bronze_tables = self.data_processor.get_bronze_tables()
            
            if not bronze_tables:
                raise ValueError("No se encontraron tablas Bronze para procesar")
            
            # Analizar volúmenes para planificación
            large_tables = []
            small_tables = []
            
            for bronze_table in bronze_tables:
                row_count = self.data_processor.volume_analyzer.get_table_row_count(bronze_table)
                if row_count >= VolumeAnalyzer.CHUNK_THRESHOLD:
                    large_tables.append(bronze_table)
                else:
                    small_tables.append(bronze_table)
            
            logger.info(f"Processing plan: {len(large_tables)} large tables (chunked), {len(small_tables)} small tables (full)")
            
            results = []
            total_records = 0
            successful_tables = 0
            failed_tables = 0
            chunked_tables = 0
            full_load_tables = 0
            
            # Procesar cada tabla Bronze a Silver
            for bronze_table in bronze_tables:
                try:
                    result = self.data_processor.process_bronze_to_silver(bronze_table)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        successful_tables += 1
                        total_records += result['record_count']
                        
                        if result.get('processing_mode') == 'chunked':
                            chunked_tables += 1
                        else:
                            full_load_tables += 1
                    else:
                        failed_tables += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process {bronze_table}: {e}")
                    failed_tables += 1
                    results.append({
                        'bronze_table': bronze_table,
                        'silver_table': bronze_table.replace('bronze_', 'silver_'),
                        'record_count': 0,
                        'status': 'failed',
                        'error': str(e)
                    })
            
            # Crear resumen del pipeline
            pipeline_result = {
                'execution_id': datetime.now().isoformat(),
                'total_tables': len(bronze_tables),
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'empty_tables': len([r for r in results if r['status'] == 'empty']),
                'chunked_tables': chunked_tables,
                'full_load_tables': full_load_tables,
                'total_records': total_records,
                'status': 'completed' if failed_tables == 0 else 'completed_with_errors',
                'layer': 'silver',
                'volume_optimization': True,
                'results': results
            }
            
            logger.info("=" * 80)
            logger.info("SILVER LAYER RESUMEN:")
            logger.info(f"  Tablas exitosas: {successful_tables}")
            logger.info(f"  Tablas fallidas: {failed_tables}")
            logger.info(f"  Procesamiento chunked: {chunked_tables}")
            logger.info(f"  Carga completa: {full_load_tables}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 80)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Silver pipeline: {e}")
            raise