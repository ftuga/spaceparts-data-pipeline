from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
import pyodbc
import re
import unicodedata
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
from typing import Dict, List, Tuple, Optional, Iterator
import logging
import urllib.parse
from sqlalchemy import inspect as sa_inspect
import gc

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# UTILITY CLASSES AND FUNCTIONS

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
    
    def get_connection(self):
        return pyodbc.connect(self.get_connection_string())
    
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
        """Removes accents from text"""
        normalized = unicodedata.normalize("NFKD", str(text))
        return "".join([c for c in normalized if not unicodedata.combining(c)])
    
    @classmethod
    def clean_identifier(cls, name: str) -> str:
        if name is None:
            return "col"
        
        # Strip accents and normalize
        s = cls.strip_accents(str(name).strip())
        
        # Replace forbidden characters
        s = re.sub(cls.FORBIDDEN_CHARS, "_", s)
        s = s.replace(".", "_").replace("-", "_").replace("/", "_").replace("\\", "_")
        
        # Remove non-alphanumeric characters except underscore
        s = re.sub(r"[^0-9a-zA-Z_]", "", s)
        
        # Consolidate underscores
        s = re.sub(r"_+", "_", s).strip("_").lower()
        
        # Handle numbers at start
        if re.match(r"^[0-9]", s):
            s = "c_" + s
        
        # Handle reserved words
        if s in cls.RESERVED_WORDS:
            s = s + "_col"
        
        if not s:
            s = "col"
        
        return s[:63]  # PostgreSQL identifier limit
    
    @classmethod
    def clean_table_name(cls, schema: str, table: str) -> str:
        """Creates a clean table name for PostgreSQL (schema público)"""
        schema_clean = cls.clean_identifier(schema)
        table_clean = cls.clean_identifier(table)
        return f"bronze_{schema_clean}_{table_clean}"

class DataProfiler:
    """Profiles source tables and generates metadata"""
    
    def __init__(self, source_manager: SourceConnectionManager):
        self.source_manager = source_manager
    
    def list_tables(self) -> List[str]:
        """Lists all tables in dim and fact schemas"""
        query = """
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE='BASE TABLE' 
            AND TABLE_SCHEMA IN ('dim', 'fact')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        with self.source_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
    
    def get_row_count(self, schema: str, table: str) -> int:
        """Gets row count for a table"""
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

class MemoryEfficientDataExtractor:    
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
            logger.warning(f"Error optimizing dtypes: {e}. Continuing with original dtypes.")
        
        return df
    
    def extract_table_streaming(self, schema: str, table: str, chunk_size: int = 25000) -> Iterator[pd.DataFrame]:
        """Extrae datos en chunks sin cargar todo en memoria"""
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
            logger.error(f"Error extracting data from {schema}.{table}: {e}")
            raise

class StreamingDataLoader:
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.cleaner = ColumnNameCleaner()
        self.engine = None

    def get_engine(self):
        """Obtiene la conexión de forma lazy"""
        if self.engine is None:
            self.engine = self.postgres_manager.get_engine()
        return self.engine
    
    def safe_table_name(self, table_name: str) -> str:
        return f'"{table_name}"' if not table_name.startswith('"') else table_name

    def create_table_if_not_exists(self, df_sample: pd.DataFrame, table_name: str):
        try:
            engine = self.get_engine()
            
            column_mapping = {col: self.cleaner.clean_identifier(col) for col in df_sample.columns}
            df_clean = df_sample.rename(columns=column_mapping)
            lower_cols = [c.lower() for c in df_clean.columns]
            if 'load_date' not in lower_cols:
                df_clean['load_date'] = datetime.now()
            if 'source_system' not in lower_cols:
                df_clean['source_system'] = 'spaceparts'
            
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
            
            logger.info(f"Table {table_name} created successfully")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def load_chunk(self, df_chunk: pd.DataFrame, table_name: str, column_mapping: dict) -> int:
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

    def load_data_streaming(self, data_iterator: Iterator[pd.DataFrame], table_name: str) -> int:
        """Carga datos usando streaming"""
        total_records = 0
        column_mapping = None
        first_chunk = True
        
        try:
            for chunk in data_iterator:
                if first_chunk:
                    column_mapping = self.create_table_if_not_exists(chunk, table_name)
                    first_chunk = False
                
                records_loaded = self.load_chunk(chunk, table_name, column_mapping)
                total_records += records_loaded
                
                logger.info(f"Loaded chunk: {records_loaded:,} rows. Total: {total_records:,}")
                
                # Liberar memoria
                del chunk
                gc.collect()
            
            logger.info(f"Completed loading {total_records:,} total rows to {table_name}")
            return total_records
            
        except Exception as e:
            logger.error(f"Error in streaming load to {table_name}: {e}")
            raise

# DAG TASK FUNCTIONS

def get_table_list(**context):
    try:
        source_manager = SourceConnectionManager()
        profiler = DataProfiler(source_manager)
        
        tables = profiler.list_tables()
        logger.info(f"Found {len(tables)} tables to process: {tables}")
        
        context['task_instance'].xcom_push(key='table_list', value=tables)
        
        return tables
    except Exception as e:
        logger.error(f"Error getting table list: {e}")
        raise

def extract_and_load_table_optimized(table_name: str, **context):
    """Función optimizada para extraer y cargar con manejo eficiente de memoria"""
    try:
        logger.info(f"Starting optimized extraction for table: {table_name}")
        
        schema, table = table_name.split('.', 1)
        source_manager = SourceConnectionManager()
        postgres_manager = PostgreSQLManager()
        profiler = DataProfiler(source_manager)
        extractor = MemoryEfficientDataExtractor(source_manager)
        loader = StreamingDataLoader(postgres_manager)
        cleaner = ColumnNameCleaner()
        
        row_count = profiler.get_row_count(schema, table)
        logger.info(f"Table {table_name} has {row_count:,} rows")
        
        if row_count == 0:
            logger.warning(f"Skipping empty table: {table_name}")
            result = {
                'source_table': table_name,
                'bronze_table': cleaner.clean_table_name(schema, table),
                'record_count': 0,
                'status': 'skipped',
                'message': 'Empty table'
            }
            context['task_instance'].xcom_push(key=f'result_{table_name}', value=result)
            return result
        
        if row_count > 10000000:  # 10M+ rows
            chunk_size = 15000
        elif row_count > 1000000:  # 1M+ rows
            chunk_size = 25000
        else:
            chunk_size = 50000
        
        logger.info(f"Using chunk size: {chunk_size:,} for {row_count:,} rows")
        
        bronze_table_name = cleaner.clean_table_name(schema, table)
        
        logger.info(f"Starting streaming extraction and load for {table_name}")
        data_iterator = extractor.extract_table_streaming(schema, table, chunk_size=chunk_size)
        records_loaded = loader.load_data_streaming(data_iterator, bronze_table_name)
        
        result = {
            'source_table': table_name,
            'bronze_table': bronze_table_name,
            'record_count': records_loaded,
            'status': 'success',
            'extraction_time': datetime.now().isoformat(),
            'chunk_size_used': chunk_size
        }
        
        logger.info(f"Successfully loaded {records_loaded:,} records to {bronze_table_name}")
        
        context['task_instance'].xcom_push(key=f'result_{table_name}', value=result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        
        cleaner = ColumnNameCleaner()
        schema, table = table_name.split('.', 1)
        result = {
            'source_table': table_name,
            'bronze_table': cleaner.clean_table_name(schema, table),
            'record_count': 0,
            'status': 'failed',
            'error': str(e),
            'extraction_time': datetime.now().isoformat()
        }
    
        context['task_instance'].xcom_push(key=f'result_{table_name}', value=result)
        
        raise

def create_execution_summary(**context):
    try:
        table_list = context['task_instance'].xcom_pull(key='table_list')
        
        results = []
        total_records = 0
        successful_tables = 0
        failed_tables = 0
        
        for table_name in table_list:
            result = context['task_instance'].xcom_pull(key=f'result_{table_name}')
            if result:
                results.append(result)
                if result['status'] == 'success':
                    successful_tables += 1
                    total_records += result['record_count']
                else:
                    failed_tables += 1
        
        execution_id = context['run_id']
        execution_summary = {
            'execution_id': execution_id,
            'execution_date': context['execution_date'].isoformat(),
            'total_tables': len(table_list),
            'successful_tables': successful_tables,
            'failed_tables': failed_tables,
            'skipped_tables': len([r for r in results if r['status'] in ['skipped', 'no_data']]),
            'total_records': total_records,
            'status': 'completed' if failed_tables == 0 else 'completed_with_errors',
            'results': results
        }
        
        logger.info(f"Execution Summary for {execution_id}:")
        logger.info(f"  Successful tables: {successful_tables}")
        logger.info(f"  Failed tables: {failed_tables}")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Status: {execution_summary['status']}")
        
        context['task_instance'].xcom_push(key='execution_summary', value=execution_summary)
        
        return execution_summary
        
    except Exception as e:
        logger.error(f"Error creating execution summary: {e}")
        raise

# DAG DEFINITION

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'bronze_full_load_spaceparts_optimized',
    default_args=default_args,
    description='Optimized full load of SpaceParts data from Azure SQL to PostgreSQL (public schema)',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['spaceparts', 'bronze', 'full-load', 'etl', 'optimized']
)

TABLE_LIST = [
    'dim.Brands',
    'dim.Budget-Rate', 
    'dim.Customers',
    'dim.Employees',
    'dim.Exchange-Rate',
    'dim.Invoice-DocType',
    'dim.Order-DocType',
    'dim.Order-Status',
    'dim.Products',
    'dim.Regions',
    'fact.Budget',
    'fact.Forecast',
    'fact.Invoices',
    'fact.Orders'
]

start_task = DummyOperator(
    task_id='start_bronze_full_load',
    dag=dag
)

get_tables_task = PythonOperator(
    task_id='get_table_list',
    python_callable=get_table_list,
    dag=dag
)

extraction_tasks = []
for table_name in TABLE_LIST:
    task_id = f"extract_load_{table_name.replace('.', '_').replace('-', '_')}"
    
    task = PythonOperator(
        task_id=task_id,
        python_callable=extract_and_load_table_optimized,
        op_kwargs={'table_name': table_name},
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    extraction_tasks.append(task)

summary_task = PythonOperator(
    task_id='create_execution_summary',
    python_callable=create_execution_summary,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE  
)

end_task = DummyOperator(
    task_id='end_bronze_full_load',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# Set task dependencies
start_task >> get_tables_task >> extraction_tasks >> summary_task >> end_task