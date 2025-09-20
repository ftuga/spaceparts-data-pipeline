import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from sqlalchemy import create_engine
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

class SilverDataProcessor:
    """Procesador de datos para la capa Silver"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
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
    
    def standardize_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Estandariza tipos de datos"""
        try:
            for col in df.columns:
                
                # Procesar fechas
                if col.lower().endswith('date') and df[col].dtype in ['int64', 'object']:
                    try:
                        # Convertir timestamps Unix o fechas string
                        if df[col].dtype == 'int64':
                            # Si son números grandes, asumir nanosegundos
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
    
    def remove_duplicates(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Elimina duplicados basado en tipo de tabla"""
        try:
            initial_count = len(df)
            
            if 'silver_dim_' in table_name:
                # Para dimensiones, eliminar duplicados por todas las columnas excepto fechas de creación
                cols_except_created = [c for c in df.columns if 'created' not in c.lower() and 'load_date' not in c.lower()]
                if len(cols_except_created) > 0:
                    df = df.drop_duplicates(subset=cols_except_created, keep='last')
            
            elif 'silver_fact_' in table_name:
                # Para hechos, eliminar duplicados por columnas clave
                key_columns = []
                for col in df.columns:
                    if any(pattern in col.lower() for pattern in ['_key', '_number', 'customer', 'product', 'document']):
                        key_columns.append(col)
                
                if key_columns:
                    df = df.drop_duplicates(subset=key_columns, keep='last')
                else:
                    # Si no hay columnas clave identificables, eliminar duplicados completos
                    df = df.drop_duplicates(keep='last')
            
            final_count = len(df)
            duplicates_removed = initial_count - final_count
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicates from {table_name}")
            
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
            
            # Filtrar fechas futuras problemáticas (más de 2 años en el futuro)
            future_cutoff = pd.Timestamp.now() + pd.DateOffset(years=2)
            
            for col in df_clean.select_dtypes(include=[np.datetime64]).columns:
                if col.lower() != 'load_date':  # No filtrar load_date
                    problematic_mask = df_clean[col] > future_cutoff
                    if problematic_mask.any():
                        problematic_count = problematic_mask.sum()
                        logger.info(f"Quarantined {problematic_count} records with future dates in {col}")
                        df_clean = df_clean[~problematic_mask]
            
            # Filtrar valores numéricos extremos en columnas de valor
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
                logger.info(f"Quality checks processed {processed_rows} problematic rows from {table_name}")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error in data quality checks for {table_name}: {e}")
            return df
    
    def optimize_partitioning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimiza el DataFrame para mejor rendimiento"""
        try:
            # Liberar memoria innecesaria
            gc.collect()
            
            # Optimizar tipos de datos categóricos
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].nunique() / len(df) < 0.5:  # Si menos del 50% son únicos
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
            
            return df
            
        except Exception as e:
            logger.error(f"Error optimizing DataFrame: {e}")
            return df
    
    def process_bronze_to_silver(self, bronze_table_name: str) -> Dict:
        """Procesa una tabla Bronze a Silver"""
        try:
            silver_table_name = bronze_table_name.replace('bronze_', 'silver_')
            logger.info(f"Processing {bronze_table_name} -> {silver_table_name}")
            
            # Leer datos de Bronze
            bronze_df = pd.read_sql(f"SELECT * FROM {bronze_table_name}", self.engine)
            
            if bronze_df.empty:
                logger.warning(f"Bronze table {bronze_table_name} is empty")
                return {
                    'bronze_table': bronze_table_name,
                    'silver_table': silver_table_name,
                    'record_count': 0,
                    'status': 'empty',
                    'message': 'Bronze table is empty'
                }
            
            # Aplicar transformaciones Silver
            df_processed = bronze_df.copy()
            df_processed = self.standardize_data_types(df_processed, silver_table_name)
            df_processed = self.remove_duplicates(df_processed, silver_table_name)
            df_processed = self.data_quality_checks(df_processed, silver_table_name)
            df_processed = self.optimize_partitioning(df_processed)
            
            # Agregar metadatos Silver
            df_processed['silver_created_date'] = datetime.now()
            df_processed['silver_execution_id'] = datetime.now().isoformat()
            
            # Guardar en Silver
            record_count = len(df_processed)
            df_processed.to_sql(
                silver_table_name, 
                self.engine, 
                if_exists='replace', 
                index=False,
                method='multi',
                chunksize=5000
            )
            
            logger.info(f"Successfully processed {silver_table_name}: {record_count:,} records")
            
            # Limpiar memoria
            del bronze_df, df_processed
            gc.collect()
            
            return {
                'bronze_table': bronze_table_name,
                'silver_table': silver_table_name,
                'record_count': record_count,
                'status': 'success',
                'processing_time': datetime.now().isoformat()
            }
            
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

class SilverProcessor:
    """Procesador principal de la capa Silver"""
    
    def __init__(self):
        self.postgres_manager = PostgreSQLManager()
        self.data_processor = SilverDataProcessor(self.postgres_manager)
    
    def execute_silver_pipeline(self, bronze_results: Dict = None) -> Dict:
        """Ejecuta el pipeline completo de Silver"""
        try:
            logger.info("=" * 60)
            logger.info("INICIANDO SILVER LAYER PIPELINE")
            logger.info("=" * 60)
            
            # Validar que Bronze esté disponible
            if bronze_results and bronze_results.get('successful_tables', 0) == 0:
                raise ValueError("No hay tablas Bronze exitosas disponibles")
            
            # Obtener lista de tablas Bronze
            bronze_tables = self.data_processor.get_bronze_tables()
            
            if not bronze_tables:
                raise ValueError("No se encontraron tablas Bronze para procesar")
            
            results = []
            total_records = 0
            successful_tables = 0
            failed_tables = 0
            
            # Procesar cada tabla Bronze a Silver
            for bronze_table in bronze_tables:
                try:
                    result = self.data_processor.process_bronze_to_silver(bronze_table)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        successful_tables += 1
                        total_records += result['record_count']
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
                'total_records': total_records,
                'status': 'completed' if failed_tables == 0 else 'completed_with_errors',
                'layer': 'silver',
                'results': results
            }
            
            logger.info("=" * 60)
            logger.info("SILVER LAYER RESUMEN:")
            logger.info(f"  Tablas exitosas: {successful_tables}")
            logger.info(f"  Tablas fallidas: {failed_tables}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 60)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Silver pipeline: {e}")
            raise