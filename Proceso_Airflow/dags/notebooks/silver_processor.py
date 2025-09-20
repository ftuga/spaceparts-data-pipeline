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

class SilverChunkProcessor:
    """Procesador de chunks para transformaciones Silver"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
    def get_table_row_count(self, table_name: str) -> int:
        """Obtiene el número de filas de una tabla"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = pd.read_sql(query, self.engine)
            return int(result.iloc[0]['count'])
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {e}")
            return 0
    
    def read_table_chunks(self, table_name: str, chunk_size: int = 50000) -> Iterator[pd.DataFrame]:
        """Lee tabla en chunks para procesamiento por lotes"""
        try:
            query = f"SELECT * FROM {table_name}"
            chunk_iterator = pd.read_sql(query, self.engine, chunksize=chunk_size)
            
            chunk_count = 0
            for chunk in chunk_iterator:
                chunk_count += 1
                logger.info(f"Reading chunk {chunk_count} with {len(chunk)} rows from {table_name}")
                yield chunk
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error reading chunks from {table_name}: {e}")
            raise
    
    def standardize_chunk_data_types(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Estandariza tipos de datos en un chunk"""
        try:
            for col in chunk.columns:
                
                # Procesar fechas
                if col.lower().endswith('date') and chunk[col].dtype in ['int64', 'object']:
                    try:
                        if chunk[col].dtype == 'int64':
                            # Detectar si son nanosegundos o segundos
                            mask_ns = chunk[col] > 1000000000000
                            chunk.loc[mask_ns, col] = pd.to_datetime(chunk.loc[mask_ns, col], unit='ns', errors='coerce')
                            chunk.loc[~mask_ns, col] = pd.to_datetime(chunk.loc[~mask_ns, col], unit='s', errors='coerce')
                        else:
                            chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
                    except:
                        pass
                
                # Procesar columna month especial
                elif col.lower() == 'month' and chunk[col].dtype == 'int64':
                    try:
                        chunk[col] = pd.to_datetime(chunk[col], unit='ns', errors='coerce')
                    except:
                        pass
                
                # Procesar dwcreateddate
                elif col.lower() == 'dwcreateddate' and chunk[col].dtype != 'datetime64[ns]':
                    try:
                        chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
                    except:
                        pass
                
                # Procesar columnas de llaves
                elif col.lower().endswith(('_key', 'key')) and chunk[col].dtype == 'object':
                    chunk[col] = chunk[col].astype(str).str.strip().str.upper()
                    chunk[col] = chunk[col].replace(['', 'NULL', 'N/A', 'UNKNOWN', 'NONE'], None)
                
                # Procesar strings generales
                elif chunk[col].dtype == 'object':
                    chunk[col] = chunk[col].astype(str).str.strip()
                    chunk[col] = chunk[col].replace(['', 'NULL', 'N/A', 'UNKNOWN', 'NONE', '#N/A'], None)
                
                # Procesar valores numéricos
                elif chunk[col].dtype in ['float64', 'float32']:
                    chunk[col] = chunk[col].replace([np.inf, -np.inf], None)
            
            return chunk
            
        except Exception as e:
            logger.error(f"Error standardizing chunk data types: {e}")
            return chunk
    
    def apply_chunk_quality_checks(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Aplica verificaciones de calidad en un chunk"""
        try:
            # Eliminar filas completamente vacías
            chunk_clean = chunk.dropna(how='all')
            
            # Filtrar fechas futuras problemáticas
            future_cutoff = pd.Timestamp.now() + pd.DateOffset(years=2)
            
            for col in chunk_clean.select_dtypes(include=[np.datetime64]).columns:
                if col.lower() not in ['load_date', 'silver_created_date']:
                    problematic_mask = chunk_clean[col] > future_cutoff
                    if problematic_mask.any():
                        chunk_clean = chunk_clean[~problematic_mask]
            
            # Filtrar valores numéricos extremos
            for col in chunk_clean.select_dtypes(include=[np.number]).columns:
                if 'value' in col.lower() or 'amount' in col.lower():
                    extreme_mask = (chunk_clean[col] > 100000000) | (chunk_clean[col] < -10000000)
                    if extreme_mask.any():
                        chunk_clean = chunk_clean[~extreme_mask]
            
            return chunk_clean
            
        except Exception as e:
            logger.error(f"Error in chunk quality checks: {e}")
            return chunk
    
    def create_silver_table_structure(self, sample_chunk: pd.DataFrame, silver_table_name: str):
        """Crea la estructura de la tabla Silver"""
        try:
            # Agregar metadatos Silver
            sample_with_metadata = sample_chunk.copy()
            sample_with_metadata['silver_created_date'] = datetime.now()
            sample_with_metadata['silver_execution_id'] = datetime.now().isoformat()
            
            # Crear tabla con una fila de muestra
            sample_with_metadata.head(1).to_sql(
                silver_table_name,
                self.engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            # Limpiar la fila de muestra
            with self.engine.begin() as conn:
                conn.execute(text(f'DELETE FROM "{silver_table_name}"'))
            
            logger.info(f"Silver table structure created: {silver_table_name}")
            
        except Exception as e:
            logger.error(f"Error creating Silver table structure {silver_table_name}: {e}")
            raise
    
    def write_silver_chunk(self, processed_chunk: pd.DataFrame, silver_table_name: str) -> int:
        """Escribe un chunk procesado a la tabla Silver"""
        try:
            # Agregar metadatos Silver
            processed_chunk['silver_created_date'] = datetime.now()
            processed_chunk['silver_execution_id'] = datetime.now().isoformat()
            
            # Escribir chunk
            processed_chunk.to_sql(
                silver_table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            return len(processed_chunk)
            
        except Exception as e:
            logger.error(f"Error writing chunk to {silver_table_name}: {e}")
            raise
    
    def process_table_streaming(self, bronze_table_name: str, chunk_size: int = 50000) -> Dict:
        """Procesa una tabla completa por streaming chunks"""
        try:
            silver_table_name = bronze_table_name.replace('bronze_', 'silver_')
            logger.info(f"Processing {bronze_table_name} -> {silver_table_name} (streaming)")
            
            row_count = self.get_table_row_count(bronze_table_name)
            if row_count == 0:
                logger.warning(f"Bronze table {bronze_table_name} is empty")
                return {
                    'bronze_table': bronze_table_name,
                    'silver_table': silver_table_name,
                    'record_count': 0,
                    'status': 'empty'
                }
            
            logger.info(f"Table {bronze_table_name} has {row_count:,} rows, processing with chunk size {chunk_size:,}")
            
            total_processed = 0
            chunk_count = 0
            first_chunk = True
            
            # Procesar chunk por chunk
            for chunk in self.read_table_chunks(bronze_table_name, chunk_size):
                chunk_count += 1
                
                # Aplicar transformaciones Silver
                processed_chunk = self.standardize_chunk_data_types(chunk)
                processed_chunk = self.apply_chunk_quality_checks(processed_chunk)
                
                # Crear estructura de tabla en el primer chunk
                if first_chunk:
                    self.create_silver_table_structure(processed_chunk, silver_table_name)
                    first_chunk = False
                
                # Escribir chunk procesado
                if not processed_chunk.empty:
                    records_written = self.write_silver_chunk(processed_chunk, silver_table_name)
                    total_processed += records_written
                    logger.info(f"Chunk {chunk_count}: processed {records_written:,} records. Total: {total_processed:,}")
                
                # Limpiar memoria
                del chunk, processed_chunk
                gc.collect()
            
            logger.info(f"Completed {silver_table_name}: {total_processed:,} records from {chunk_count} chunks")
            
            return {
                'bronze_table': bronze_table_name,
                'silver_table': silver_table_name,
                'record_count': total_processed,
                'chunks_processed': chunk_count,
                'status': 'success',
                'processing_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in streaming processing of {bronze_table_name}: {e}")
            return {
                'bronze_table': bronze_table_name,
                'silver_table': bronze_table_name.replace('bronze_', 'silver_'),
                'record_count': 0,
                'status': 'failed',
                'error': str(e)
            }

class SilverProcessor:
    """Procesador principal de la capa Silver con soporte para grandes volúmenes"""
    
    def __init__(self):
        self.postgres_manager = PostgreSQLManager()
        self.chunk_processor = SilverChunkProcessor(self.postgres_manager)
    
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
            
            result = pd.read_sql(query, self.postgres_manager.get_engine())
            tables = result['table_name'].tolist()
            
            logger.info(f"Found {len(tables)} Bronze tables to process")
            return tables
            
        except Exception as e:
            logger.error(f"Error getting Bronze tables: {e}")
            raise
    
    def determine_chunk_size(self, table_name: str) -> int:
        """Determina el tamaño de chunk óptimo basado en el volumen de datos"""
        try:
            row_count = self.chunk_processor.get_table_row_count(table_name)
            
            if row_count > 15000000:  # >15M filas
                return 25000
            elif row_count > 5000000:  # >5M filas
                return 40000
            elif row_count > 1000000:  # >1M filas
                return 75000
            else:
                return 100000
                
        except Exception as e:
            logger.warning(f"Error determining chunk size for {table_name}: {e}")
            return 50000  # Default
    
    def execute_silver_pipeline(self, bronze_results: Dict = None) -> Dict:
        """Ejecuta el pipeline completo de Silver con procesamiento por chunks"""
        try:
            logger.info("=" * 60)
            logger.info("INICIANDO SILVER LAYER PIPELINE (STREAMING)")
            logger.info("=" * 60)
            
            # Validar Bronze disponible
            if bronze_results and bronze_results.get('successful_tables', 0) == 0:
                raise ValueError("No hay tablas Bronze exitosas disponibles")
            
            # Obtener tablas Bronze
            bronze_tables = self.get_bronze_tables()
            
            if not bronze_tables:
                raise ValueError("No se encontraron tablas Bronze para procesar")
            
            results = []
            total_records = 0
            successful_tables = 0
            failed_tables = 0
            
            # Procesar cada tabla por streaming
            for bronze_table in bronze_tables:
                try:
                    # Determinar chunk size óptimo
                    chunk_size = self.determine_chunk_size(bronze_table)
                    
                    # Procesar tabla completa por chunks
                    result = self.chunk_processor.process_table_streaming(bronze_table, chunk_size)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        successful_tables += 1
                        total_records += result['record_count']
                        logger.info(f"✓ {result['silver_table']}: {result['record_count']:,} registros")
                    else:
                        failed_tables += 1
                        logger.error(f"✗ {bronze_table}: {result.get('error', 'Unknown error')}")
                        
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
                'total_records': total_records,
                'status': 'completed' if failed_tables == 0 else 'completed_with_errors',
                'layer': 'silver',
                'processing_mode': 'streaming_chunks',
                'results': results
            }
            
            logger.info("=" * 60)
            logger.info("SILVER LAYER RESUMEN:")
            logger.info(f"  Tablas exitosas: {successful_tables}")
            logger.info(f"  Tablas fallidas: {failed_tables}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Modo: Streaming por chunks")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 60)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Silver pipeline: {e}")
            raise