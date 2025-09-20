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

class GoldVolumeAnalyzer:
    """Analiza volúmenes para estrategia Gold híbrida"""
    
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
    
    def classify_tables_by_volume(self) -> Dict:
        """Clasifica tablas Silver por volumen para estrategia Gold"""
        try:
            # Tablas que típicamente son pequeñas (lookup/dimension)
            small_tables = [
                'silver_dim_customers', 'silver_dim_regions', 'silver_dim_employees',
                'silver_dim_products', 'silver_dim_brands', 'silver_dim_budget_rate',
                'silver_dim_invoice_doctype', 'silver_dim_order_doctype', 'silver_dim_order_status'
            ]
            
            # Tablas que típicamente son grandes (fact tables)
            large_tables = [
                'silver_fact_invoices', 'silver_fact_orders', 'silver_fact_budget', 'silver_fact_forecast'
            ]
            
            classification = {
                'small_tables': {},
                'large_tables': {},
                'processing_strategy': {}
            }
            
            # Verificar tablas pequeñas
            for table in small_tables:
                try:
                    row_count = self.get_table_row_count(table)
                    classification['small_tables'][table] = row_count
                    classification['processing_strategy'][table] = 'full_load'
                except:
                    logger.warning(f"Table {table} not found or empty")
            
            # Verificar tablas grandes
            for table in large_tables:
                try:
                    row_count = self.get_table_row_count(table)
                    classification['large_tables'][table] = row_count
                    
                    if row_count >= self.CHUNK_THRESHOLD:
                        classification['processing_strategy'][table] = 'chunked'
                    else:
                        classification['processing_strategy'][table] = 'full_load'
                except:
                    logger.warning(f"Table {table} not found or empty")
            
            logger.info(f"Table classification completed: {len(classification['small_tables'])} small, {len(classification['large_tables'])} large")
            return classification
            
        except Exception as e:
            logger.error(f"Error classifying tables: {e}")
            raise

class ChunkedGoldReader:
    """Lector optimizado para procesamiento Gold por chunks"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
    def load_small_tables(self, table_names: List[str]) -> Dict[str, pd.DataFrame]:
        """Carga tablas pequeñas completas en memoria para joins"""
        small_tables = {}
        
        try:
            for table_name in table_names:
                try:
                    df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
                    small_tables[table_name] = df
                    logger.info(f"Loaded {table_name}: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"Could not load {table_name}: {e}")
                    small_tables[table_name] = pd.DataFrame()
            
            return small_tables
            
        except Exception as e:
            logger.error(f"Error loading small tables: {e}")
            raise
    
    def read_large_table_chunks(self, table_name: str, chunk_size: int, columns: List[str] = None) -> Iterator[pd.DataFrame]:
        """Lee tabla grande por chunks"""
        try:
            column_list = "*" if not columns else ", ".join(columns)
            offset = 0
            chunk_count = 0
            
            while True:
                query = f"""
                    SELECT {column_list} FROM {table_name} 
                    ORDER BY silver_created_date 
                    LIMIT {chunk_size} OFFSET {offset}
                """
                
                chunk = pd.read_sql(query, self.engine)
                
                if chunk.empty:
                    break
                
                chunk_count += 1
                logger.info(f"Read chunk {chunk_count} from {table_name}: {len(chunk)} rows")
                
                yield chunk
                
                offset += chunk_size
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error reading chunks from {table_name}: {e}")
            raise

class GoldBusinessModelsOptimized:
    """Creador optimizado de modelos de negocio Gold"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
        self.volume_analyzer = GoldVolumeAnalyzer(postgres_manager)
        self.chunked_reader = ChunkedGoldReader(postgres_manager)
    
    def verify_silver_tables(self) -> bool:
        """Verifica disponibilidad de tablas Silver"""
        required_tables = [
            'silver_dim_customers', 'silver_dim_regions', 'silver_dim_employees',
            'silver_dim_products', 'silver_dim_brands', 'silver_fact_invoices',
            'silver_dim_budget_rate', 'silver_dim_invoice_doctype', 'silver_fact_orders',
            'silver_fact_budget'
        ]
        
        try:
            available_tables = []
            for table in required_tables:
                try:
                    result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table} LIMIT 1", self.engine)
                    if not result.empty:
                        available_tables.append(table)
                except:
                    logger.warning(f"Table {table} not available")
            
            logger.info(f"Silver tables available: {len(available_tables)}/{len(required_tables)}")
            return len(available_tables) >= len(required_tables) * 0.8  # 80% disponible
            
        except Exception as e:
            logger.error(f"Error verifying Silver tables: {e}")
            return False
    
    def create_gold_dim_customer(self, small_tables: Dict[str, pd.DataFrame]) -> int:
        """Crea dimensión consolidada de clientes usando tablas pre-cargadas"""
        try:
            logger.info("Creando gold_dim_customer")
            
            customers_df = small_tables.get('silver_dim_customers', pd.DataFrame())
            regions_df = small_tables.get('silver_dim_regions', pd.DataFrame())
            employees_df = small_tables.get('silver_dim_employees', pd.DataFrame())
            
            if customers_df.empty:
                logger.warning("No customer data available")
                return 0
            
            # Filtrar empleados por rol
            am_df = employees_df[employees_df['role'] == 'Account Manager'][['employee_name', 'employee_email']].copy() if not employees_df.empty else pd.DataFrame()
            kam_df = employees_df[employees_df['role'] == 'Key Account Manager'][['employee_name', 'employee_email']].copy() if not employees_df.empty else pd.DataFrame()
            
            # Realizar joins
            consolidated_customer = customers_df.copy()
            
            if not regions_df.empty:
                consolidated_customer = consolidated_customer.merge(
                    regions_df, 
                    left_on='station', 
                    right_on='station', 
                    how='left', 
                    suffixes=('', '_r')
                )
            
            if not am_df.empty:
                consolidated_customer = consolidated_customer.merge(
                    am_df,
                    left_on='account_manager',
                    right_on='employee_name',
                    how='left',
                    suffixes=('', '_am')
                )
            
            if not kam_df.empty:
                consolidated_customer = consolidated_customer.merge(
                    kam_df,
                    left_on='key_account_manager', 
                    right_on='employee_name',
                    how='left',
                    suffixes=('', '_kam')
                )
            
            # Seleccionar columnas finales
            base_columns = ['customer_key', 'customer_sold_to_name', 'account_name', 'key_account_name',
                           'transaction_type', 'account_type', 'account_manager', 'key_account_manager']
            
            # Agregar columnas disponibles del join
            if 'system' in consolidated_customer.columns:
                base_columns.append('system')
            if 'interplanetary_region' in consolidated_customer.columns:
                base_columns.append('interplanetary_region')
            if 'territory' in consolidated_customer.columns:
                base_columns.append('territory')
            if 'station' in consolidated_customer.columns:
                base_columns.append('station')
            if 'tax_rate' in consolidated_customer.columns:
                base_columns.append('tax_rate')
            
            available_columns = [col for col in base_columns if col in consolidated_customer.columns]
            final_df = consolidated_customer[available_columns].copy()
            
            # Renombrar sistema para claridad
            if 'system' in final_df.columns:
                final_df = final_df.rename(columns={'system': 'customer_system'})
            
            record_count = len(final_df)
            final_df.to_sql('gold_dim_customer', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_dim_customer: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_dim_customer: {str(e)}")
            raise
    
    def create_gold_dim_product(self, small_tables: Dict[str, pd.DataFrame]) -> int:
        """Crea dimensión consolidada de productos usando tablas pre-cargadas"""
        try:
            logger.info("Creando gold_dim_product")
            
            products_df = small_tables.get('silver_dim_products', pd.DataFrame())
            brands_df = small_tables.get('silver_dim_brands', pd.DataFrame())
            
            if products_df.empty:
                logger.warning("No product data available")
                return 0
            
            # Join productos con marcas si están disponibles
            if not brands_df.empty:
                consolidated_product = products_df.merge(
                    brands_df,
                    left_on='sub_brand_name',
                    right_on='sub_brand', 
                    how='left',
                    suffixes=('', '_b')
                )
            else:
                consolidated_product = products_df.copy()
            
            # Seleccionar columnas disponibles
            base_columns = ['product_key', 'product_name', 'type', 'subtype', 'sub_brand_name']
            
            # Agregar columnas opcionales
            optional_columns = ['ship_class_for_part', 'weight_tonnes', 'color', 'material', 
                              'flagship', 'class', 'brand', 'product_business_line_leader', 'product_brand_vp']
            
            for col in optional_columns:
                if col in consolidated_product.columns:
                    base_columns.append(col)
            
            available_columns = [col for col in base_columns if col in consolidated_product.columns]
            final_df = consolidated_product[available_columns].copy()
            
            # Renombrar columnas para consistencia
            column_renames = {
                'type': 'product_type',
                'subtype': 'product_subtype', 
                'flagship': 'brand_flagship',
                'class': 'brand_class'
            }
            
            for old_col, new_col in column_renames.items():
                if old_col in final_df.columns:
                    final_df = final_df.rename(columns={old_col: new_col})
            
            record_count = len(final_df)
            final_df.to_sql('gold_dim_product', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_dim_product: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_dim_product: {str(e)}")
            raise
    
    def create_gold_fact_sales_chunked(self, small_tables: Dict[str, pd.DataFrame], chunk_size: int = 200000) -> int:
        """Crea tabla de hechos de ventas procesando por chunks"""
        try:
            logger.info("Creando gold_fact_sales con procesamiento por chunks")
            
            budget_rate_df = small_tables.get('silver_dim_budget_rate', pd.DataFrame())
            doc_types_df = small_tables.get('silver_dim_invoice_doctype', pd.DataFrame())
            
            # Crear estructura de tabla con primer chunk
            first_chunk = True
            total_records = 0
            chunk_count = 0
            
            for chunk in self.chunked_reader.read_large_table_chunks('silver_fact_invoices', chunk_size):
                chunk_count += 1
                logger.info(f"Processing sales chunk {chunk_count}")
                
                # Aplicar joins con tablas pequeñas
                processed_chunk = chunk.copy()
                
                # Join con tasas de cambio
                if not budget_rate_df.empty:
                    processed_chunk = processed_chunk.merge(
                        budget_rate_df,
                        left_on='local_currency',
                        right_on='from_currency',
                        how='left'
                    )
                    processed_chunk['rate'] = processed_chunk['rate'].fillna(1.0)
                else:
                    processed_chunk['rate'] = 1.0
                
                # Join con tipos de documento
                if not doc_types_df.empty:
                    processed_chunk = processed_chunk.merge(
                        doc_types_df,
                        on='billing_document_type_code',
                        how='left'
                    )
                
                # Calcular valores en EUR
                monetary_columns = ['net_invoice_value', 'net_invoice_cogs', 'delivery_cost', 'freight', 'taxes_commercial_fees']
                eur_columns = ['sales_eur', 'cogs_eur', 'delivery_cost_eur', 'freight_eur', 'taxes_eur']
                
                for orig_col, eur_col in zip(monetary_columns, eur_columns):
                    if orig_col in processed_chunk.columns:
                        processed_chunk[eur_col] = processed_chunk[orig_col] * processed_chunk['rate']
                
                # Convertir fechas
                date_columns = ['billing_date', 'ship_date']
                for col in date_columns:
                    if col in processed_chunk.columns:
                        processed_chunk[col] = pd.to_datetime(processed_chunk[col]).dt.date
                
                # Procesar categoría de documentos
                if 'group_col' in processed_chunk.columns:
                    processed_chunk['document_category'] = processed_chunk['group_col'].fillna('Unclassified')
                    processed_chunk['document_category'] = processed_chunk['document_category'].replace('Invoice', 'Sale')
                else:
                    processed_chunk['document_category'] = 'Sale'
                
                # Seleccionar columnas finales
                final_columns = [
                    'customer_key', 'product_key', 'billing_date', 'ship_date',
                    'billing_document_number', 'billing_document_line_item_number',
                    'sales_eur', 'cogs_eur', 'delivery_cost_eur', 'freight_eur', 'taxes_eur',
                    'net_invoice_quantity', 'document_category', 'billing_document_type_code'
                ]
                
                available_columns = [col for col in final_columns if col in processed_chunk.columns]
                chunk_final = processed_chunk[available_columns].copy()
                
                # Renombrar columnas
                column_renames = {
                    'billing_document_number': 'invoice_number',
                    'billing_document_line_item_number': 'line_item',
                    'net_invoice_quantity': 'quantity',
                    'billing_document_type_code': 'source_doc_type_code'
                }
                
                for old_col, new_col in column_renames.items():
                    if old_col in chunk_final.columns:
                        chunk_final = chunk_final.rename(columns={old_col: new_col})
                
                # Agregar indicador OTD si está disponible
                if 'otd_indicator' in processed_chunk.columns:
                    chunk_final['on_time_delivery'] = processed_chunk['otd_indicator'].astype(bool)
                
                # Guardar chunk
                chunk_records = len(chunk_final)
                if_exists_mode = 'replace' if first_chunk else 'append'
                
                chunk_final.to_sql('gold_fact_sales', self.engine, if_exists=if_exists_mode, index=False)
                
                total_records += chunk_records
                first_chunk = False
                
                logger.info(f"Processed sales chunk {chunk_count}: {chunk_records:,} records. Total: {total_records:,}")
                
                # Limpiar memoria
                del processed_chunk, chunk_final
                gc.collect()
            
            logger.info(f"gold_fact_sales: {total_records:,} registros")
            return total_records
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_sales: {str(e)}")
            raise
    
    def create_gold_fact_orders_chunked(self, small_tables: Dict[str, pd.DataFrame], chunk_size: int = 200000) -> int:
        """Crea tabla de hechos de órdenes procesando por chunks"""
        try:
            logger.info("Creando gold_fact_orders con procesamiento por chunks")
            
            budget_rate_df = small_tables.get('silver_dim_budget_rate', pd.DataFrame())
            
            first_chunk = True
            total_records = 0
            chunk_count = 0
            
            for chunk in self.chunked_reader.read_large_table_chunks('silver_fact_orders', chunk_size):
                chunk_count += 1
                logger.info(f"Processing orders chunk {chunk_count}")
                
                processed_chunk = chunk.copy()
                
                # Join con tasas de cambio
                if not budget_rate_df.empty:
                    processed_chunk = processed_chunk.merge(
                        budget_rate_df,
                        left_on='local_currency',
                        right_on='from_currency',
                        how='left'
                    )
                    processed_chunk['rate'] = processed_chunk['rate'].fillna(1.0)
                else:
                    processed_chunk['rate'] = 1.0
                
                # Calcular valor en EUR
                processed_chunk['order_value_eur'] = processed_chunk['net_order_value'] * processed_chunk['rate']
                
                # Convertir fechas
                date_columns = ['order_date', 'ship_date', 'request_goods_receipt_date']
                for col in date_columns:
                    if col in processed_chunk.columns:
                        processed_chunk[col] = pd.to_datetime(processed_chunk[col]).dt.date
                
                # Seleccionar columnas finales
                final_columns = [
                    'customer_key', 'product_key', 'order_date', 'ship_date', 'request_goods_receipt_date',
                    'sales_order_document_number', 'sales_order_document_line_item_number',
                    'order_value_eur', 'net_order_quantity', 'sales_order_document_line_item_status'
                ]
                
                available_columns = [col for col in final_columns if col in processed_chunk.columns]
                chunk_final = processed_chunk[available_columns].copy()
                
                # Renombrar columnas
                column_renames = {
                    'request_goods_receipt_date': 'requested_date',
                    'sales_order_document_number': 'order_number',
                    'sales_order_document_line_item_number': 'line_item',
                    'net_order_quantity': 'quantity',
                    'sales_order_document_line_item_status': 'order_status'
                }
                
                for old_col, new_col in column_renames.items():
                    if old_col in chunk_final.columns:
                        chunk_final = chunk_final.rename(columns={old_col: new_col})
                
                # Guardar chunk
                chunk_records = len(chunk_final)
                if_exists_mode = 'replace' if first_chunk else 'append'
                
                chunk_final.to_sql('gold_fact_orders', self.engine, if_exists=if_exists_mode, index=False)
                
                total_records += chunk_records
                first_chunk = False
                
                logger.info(f"Processed orders chunk {chunk_count}: {chunk_records:,} records. Total: {total_records:,}")
                
                # Limpiar memoria
                del processed_chunk, chunk_final
                gc.collect()
            
            logger.info(f"gold_fact_orders: {total_records:,} registros")
            return total_records
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_orders: {str(e)}")
            raise
    
    def create_gold_fact_budget(self, small_tables: Dict[str, pd.DataFrame]) -> int:
        """Crea tabla de hechos de presupuesto (tabla típicamente pequeña)"""
        try:
            logger.info("Creando gold_fact_budget")
            
            # Verificar si necesita procesamiento por chunks
            budget_row_count = self.volume_analyzer.get_table_row_count('silver_fact_budget')
            
            if budget_row_count >= self.volume_analyzer.CHUNK_THRESHOLD:
                return self.create_gold_fact_budget_chunked(chunk_size=200000)
            else:
                # Procesamiento completo para tabla pequeña
                budget_df = pd.read_sql("SELECT * FROM silver_fact_budget", self.engine)
                
                if 'month' in budget_df.columns:
                    budget_df['budget_month'] = pd.to_datetime(budget_df['month']).dt.date
                
                final_columns = ['customer_key', 'product_key', 'budget_month', 'total_budget']
                available_columns = [col for col in final_columns if col in budget_df.columns]
                final_df = budget_df[available_columns].copy()
                
                if 'total_budget' in final_df.columns:
                    final_df = final_df.rename(columns={'total_budget': 'budget_eur'})
                
                final_df = final_df.dropna(subset=['budget_month'])
                
                record_count = len(final_df)
                final_df.to_sql('gold_fact_budget', self.engine, if_exists='replace', index=False)
                
                logger.info(f"gold_fact_budget: {record_count:,} registros")
                return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_budget: {str(e)}")
            raise
    
    def create_gold_fact_budget_chunked(self, chunk_size: int = 200000) -> int:
        """Versión por chunks para presupuesto si es tabla grande"""
        try:
            logger.info("Creando gold_fact_budget con procesamiento por chunks")
            
            first_chunk = True
            total_records = 0
            chunk_count = 0
            
            for chunk in self.chunked_reader.read_large_table_chunks('silver_fact_budget', chunk_size):
                chunk_count += 1
                
                if 'month' in chunk.columns:
                    chunk['budget_month'] = pd.to_datetime(chunk['month']).dt.date
                
                final_columns = ['customer_key', 'product_key', 'budget_month', 'total_budget']
                available_columns = [col for col in final_columns if col in chunk.columns]
                chunk_final = chunk[available_columns].copy()
                
                if 'total_budget' in chunk_final.columns:
                    chunk_final = chunk_final.rename(columns={'total_budget': 'budget_eur'})
                
                chunk_final = chunk_final.dropna(subset=['budget_month'])
                
                chunk_records = len(chunk_final)
                if_exists_mode = 'replace' if first_chunk else 'append'
                
                chunk_final.to_sql('gold_fact_budget', self.engine, if_exists=if_exists_mode, index=False)
                
                total_records += chunk_records
                first_chunk = False
                
                logger.info(f"Processed budget chunk {chunk_count}: {chunk_records:,} records. Total: {total_records:,}")
                
                del chunk_final
                gc.collect()
            
            return total_records
            
        except Exception as e:
            logger.error(f"Error en gold_fact_budget chunked: {str(e)}")
            raise

class GoldProcessor:
    """Procesador principal Gold con optimización híbrida"""
    
    def __init__(self):
        self.postgres_manager = PostgreSQLManager()
        self.business_models = GoldBusinessModelsOptimized(self.postgres_manager)
    
    def execute_gold_pipeline(self, silver_results: Dict = None) -> Dict:
        """Ejecuta pipeline Gold con estrategia híbrida"""
        try:
            logger.info("=" * 80)
            logger.info("INICIANDO GOLD LAYER PIPELINE - HYBRID PROCESSING")
            logger.info("=" * 80)
            
            # Validar Silver
            if silver_results and silver_results.get('successful_tables', 0) == 0:
                raise ValueError("No hay tablas Silver exitosas disponibles")
            
            if not self.business_models.verify_silver_tables():
                raise ValueError("Las tablas Silver requeridas no están disponibles")
            
            # Analizar volúmenes y clasificar tablas
            table_classification = self.business_models.volume_analyzer.classify_tables_by_volume()
            
            # Cargar tablas pequeñas en memoria para joins eficientes
            small_table_names = list(table_classification['small_tables'].keys())
            logger.info(f"Loading {len(small_table_names)} small tables in memory for joins")
            
            small_tables = self.business_models.chunked_reader.load_small_tables(small_table_names)
            
            results = []
            total_records = 0
            
            # Definir modelos Gold a crear con sus estrategias
            gold_models = [
                ('gold_dim_customer', self.business_models.create_gold_dim_customer, 'small_table_joins'),
                ('gold_dim_product', self.business_models.create_gold_dim_product, 'small_table_joins'),
                ('gold_fact_sales', self.business_models.create_gold_fact_sales_chunked, 'chunked_processing'),
                ('gold_fact_orders', self.business_models.create_gold_fact_orders_chunked, 'chunked_processing'),
                ('gold_fact_budget', self.business_models.create_gold_fact_budget, 'adaptive')
            ]
            
            # Procesar cada modelo Gold
            for model_name, create_function, strategy in gold_models:
                try:
                    logger.info(f"Creating {model_name} using {strategy}")
                    
                    if strategy == 'small_table_joins':
                        record_count = create_function(small_tables)
                    elif strategy == 'chunked_processing':
                        record_count = create_function(small_tables)
                    else:  # adaptive
                        record_count = create_function(small_tables)
                    
                    results.append({
                        'table': model_name, 
                        'records': record_count, 
                        'status': 'success',
                        'processing_strategy': strategy,
                        'processing_time': datetime.now().isoformat()
                    })
                    total_records += record_count
                    logger.info(f"Gold model {model_name}: {record_count:,} registros using {strategy}")
                    
                except Exception as e:
                    results.append({
                        'table': model_name, 
                        'records': 0, 
                        'status': 'failed', 
                        'error': str(e),
                        'processing_strategy': strategy,
                        'processing_time': datetime.now().isoformat()
                    })
                    logger.error(f"Error creating {model_name}: {str(e)}")
            
            # Liberar memoria de tablas pequeñas
            del small_tables
            gc.collect()
            
            # Calcular métricas
            successful = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'failed']
            
            chunked_models = len([r for r in results if r.get('processing_strategy') == 'chunked_processing'])
            small_table_models = len([r for r in results if r.get('processing_strategy') == 'small_table_joins'])
            
            pipeline_result = {
                'execution_id': datetime.now().isoformat(),
                'total_tables': len(gold_models),
                'successful_tables': len(successful),
                'failed_tables': len(failed),
                'chunked_models': chunked_models,
                'small_table_models': small_table_models,
                'total_records': total_records,
                'status': 'completed' if len(failed) == 0 else 'completed_with_errors',
                'layer': 'gold',
                'hybrid_optimization': True,
                'results': results
            }
            
            logger.info("=" * 80)
            logger.info("GOLD LAYER RESUMEN:")
            logger.info(f"  Modelos exitosos: {len(successful)}/{len(gold_models)}")
            logger.info(f"  Modelos fallidos: {len(failed)}")
            logger.info(f"  Procesamiento chunked: {chunked_models}")
            logger.info(f"  Procesamiento small tables: {small_table_models}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 80)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Gold pipeline: {e}")
            raise