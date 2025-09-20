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

class GoldBusinessModels:
    """Creador de modelos de negocio para la capa Gold"""
    
    def __init__(self, postgres_manager: PostgreSQLManager):
        self.postgres_manager = postgres_manager
        self.engine = postgres_manager.get_engine()
    
    def verify_silver_tables(self) -> bool:
        """Verifica que las tablas Silver necesarias existan"""
        required_tables = [
            'silver_dim_customers', 'silver_dim_regions', 'silver_dim_employees',
            'silver_dim_products', 'silver_dim_brands', 'silver_fact_invoices',
            'silver_dim_budget_rate', 'silver_dim_invoice_doctype', 'silver_fact_orders',
            'silver_fact_budget'
        ]
        
        try:
            for table in required_tables:
                result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table} LIMIT 1", self.engine)
                if result.empty:
                    logger.warning(f"Table {table} is empty")
                    
            logger.info("Silver tables verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying Silver tables: {e}")
            return False
    
    def create_gold_dim_customer(self) -> int:
        """Crea dimensión consolidada de clientes"""
        try:
            logger.info("Creando gold_dim_customer")
            
            # Leer tablas necesarias
            customers_query = "SELECT * FROM silver_dim_customers"
            regions_query = "SELECT * FROM silver_dim_regions" 
            employees_query = "SELECT * FROM silver_dim_employees"
            
            customers_df = pd.read_sql(customers_query, self.engine)
            regions_df = pd.read_sql(regions_query, self.engine)
            employees_df = pd.read_sql(employees_query, self.engine)
            
            # Filtrar empleados por rol
            am_df = employees_df[employees_df['role'] == 'Account Manager'][['employee_name', 'employee_email']].copy()
            kam_df = employees_df[employees_df['role'] == 'Key Account Manager'][['employee_name', 'employee_email']].copy()
            
            # Realizar joins
            consolidated_customer = customers_df.merge(
                regions_df, 
                left_on='station', 
                right_on='station', 
                how='left', 
                suffixes=('', '_r')
            )
            
            consolidated_customer = consolidated_customer.merge(
                am_df,
                left_on='account_manager',
                right_on='employee_name',
                how='left',
                suffixes=('', '_am')
            )
            
            consolidated_customer = consolidated_customer.merge(
                kam_df,
                left_on='key_account_manager', 
                right_on='employee_name',
                how='left',
                suffixes=('', '_kam')
            )
            
            # Seleccionar columnas finales
            final_columns = [
                'customer_key', 'customer_sold_to_name', 'account_name', 'key_account_name',
                'transaction_type', 'account_type', 'system', 'interplanetary_region',
                'territory', 'station', 'tax_rate', 'account_manager', 'key_account_manager'
            ]
            
            # Verificar que las columnas existan
            available_columns = [col for col in final_columns if col in consolidated_customer.columns]
            final_df = consolidated_customer[available_columns].copy()
            
            # Renombrar columnas para claridad
            if 'system' in final_df.columns:
                final_df = final_df.rename(columns={'system': 'customer_system'})
            
            # Agregar emails si están disponibles
            if 'employee_email_am' in consolidated_customer.columns:
                final_df['account_manager_email'] = consolidated_customer['employee_email_am']
            if 'employee_email_kam' in consolidated_customer.columns:
                final_df['key_account_manager_email'] = consolidated_customer['employee_email_kam']
            
            record_count = len(final_df)
            final_df.to_sql('gold_dim_customer', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_dim_customer: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_dim_customer: {str(e)}")
            raise
    
    def create_gold_dim_product(self) -> int:
        """Crea dimensión consolidada de productos"""
        try:
            logger.info("Creando gold_dim_product")
            
            products_query = "SELECT * FROM silver_dim_products"
            brands_query = "SELECT * FROM silver_dim_brands"
            
            products_df = pd.read_sql(products_query, self.engine)
            brands_df = pd.read_sql(brands_query, self.engine)
            
            # Join productos con marcas
            consolidated_product = products_df.merge(
                brands_df,
                left_on='sub_brand_name',
                right_on='sub_brand', 
                how='left',
                suffixes=('', '_b')
            )
            
            # Seleccionar columnas finales
            final_columns = [
                'product_key', 'product_name', 'type', 'subtype', 'ship_class_for_part',
                'weight_tonnes', 'color', 'material', 'flagship', 'class', 'brand',
                'sub_brand_name', 'product_business_line_leader', 'product_brand_vp'
            ]
            
            available_columns = [col for col in final_columns if col in consolidated_product.columns]
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
    
    def create_gold_fact_sales(self) -> int:
        """Crea tabla de hechos de ventas"""
        try:
            logger.info("Creando gold_fact_sales")
            
            invoices_query = "SELECT * FROM silver_fact_invoices"
            budget_rate_query = "SELECT * FROM silver_dim_budget_rate"
            doc_types_query = "SELECT * FROM silver_dim_invoice_doctype"
            
            invoices_df = pd.read_sql(invoices_query, self.engine)
            budget_rate_df = pd.read_sql(budget_rate_query, self.engine)
            doc_types_df = pd.read_sql(doc_types_query, self.engine)
            
            # Join con tasas de cambio
            gold_fact_sales = invoices_df.merge(
                budget_rate_df,
                left_on='local_currency',
                right_on='from_currency',
                how='left'
            )
            
            # Join con tipos de documento
            gold_fact_sales = gold_fact_sales.merge(
                doc_types_df,
                on='billing_document_type_code',
                how='left'
            )
            
            # Calcular valores en EUR
            gold_fact_sales['rate'] = gold_fact_sales['rate'].fillna(1.0)
            
            # Conversiones monetarias
            monetary_columns = ['net_invoice_value', 'net_invoice_cogs', 'delivery_cost', 'freight', 'taxes_commercial_fees']
            eur_columns = ['sales_eur', 'cogs_eur', 'delivery_cost_eur', 'freight_eur', 'taxes_eur']
            
            for orig_col, eur_col in zip(monetary_columns, eur_columns):
                if orig_col in gold_fact_sales.columns:
                    gold_fact_sales[eur_col] = gold_fact_sales[orig_col] * gold_fact_sales['rate']
            
            # Convertir fechas
            date_columns = ['billing_date', 'ship_date']
            for col in date_columns:
                if col in gold_fact_sales.columns:
                    gold_fact_sales[col] = pd.to_datetime(gold_fact_sales[col]).dt.date
            
            # Procesar categoría de documentos
            if 'group_col' in gold_fact_sales.columns:
                gold_fact_sales['document_category'] = gold_fact_sales['group_col'].fillna('Unclassified')
                gold_fact_sales['document_category'] = gold_fact_sales['document_category'].replace('Invoice', 'Sale')
            else:
                gold_fact_sales['document_category'] = 'Sale'
            
            # Seleccionar columnas finales
            final_columns = [
                'customer_key', 'product_key', 'billing_date', 'ship_date',
                'billing_document_number', 'billing_document_line_item_number',
                'sales_eur', 'cogs_eur', 'delivery_cost_eur', 'freight_eur', 'taxes_eur',
                'net_invoice_quantity', 'otd_indicator', 'document_category',
                'billing_document_type_code'
            ]
            
            available_columns = [col for col in final_columns if col in gold_fact_sales.columns]
            final_df = gold_fact_sales[available_columns].copy()
            
            # Renombrar columnas para consistencia
            column_renames = {
                'billing_document_number': 'invoice_number',
                'billing_document_line_item_number': 'line_item',
                'net_invoice_quantity': 'quantity',
                'billing_document_type_code': 'source_doc_type_code'
            }
            
            for old_col, new_col in column_renames.items():
                if old_col in final_df.columns:
                    final_df = final_df.rename(columns={old_col: new_col})
            
            # Agregar descripción de tipo de documento si está disponible
            if 'text' in gold_fact_sales.columns:
                final_df['document_type_description'] = gold_fact_sales['text']
            
            # Convertir indicador OTD a boolean
            if 'otd_indicator' in final_df.columns:
                final_df['on_time_delivery'] = final_df['otd_indicator'].astype(bool)
                final_df = final_df.drop('otd_indicator', axis=1)
            
            record_count = len(final_df)
            final_df.to_sql('gold_fact_sales', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_fact_sales: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_sales: {str(e)}")
            raise
    
    def create_gold_fact_orders(self) -> int:
        """Crea tabla de hechos de órdenes"""
        try:
            logger.info("Creando gold_fact_orders")
            
            orders_query = "SELECT * FROM silver_fact_orders"
            budget_rate_query = "SELECT * FROM silver_dim_budget_rate"
            
            orders_df = pd.read_sql(orders_query, self.engine)
            budget_rate_df = pd.read_sql(budget_rate_query, self.engine)
            
            # Join con tasas de cambio
            gold_fact_orders = orders_df.merge(
                budget_rate_df,
                left_on='local_currency',
                right_on='from_currency',
                how='left'
            )
            
            # Calcular valor en EUR
            gold_fact_orders['rate'] = gold_fact_orders['rate'].fillna(1.0)
            gold_fact_orders['order_value_eur'] = gold_fact_orders['net_order_value'] * gold_fact_orders['rate']
            
            # Convertir fechas
            date_columns = ['order_date', 'ship_date', 'request_goods_receipt_date']
            for col in date_columns:
                if col in gold_fact_orders.columns:
                    gold_fact_orders[col] = pd.to_datetime(gold_fact_orders[col]).dt.date
            
            # Seleccionar columnas finales
            final_columns = [
                'customer_key', 'product_key', 'order_date', 'ship_date', 'request_goods_receipt_date',
                'sales_order_document_number', 'sales_order_document_line_item_number',
                'order_value_eur', 'net_order_quantity', 'sales_order_document_line_item_status'
            ]
            
            available_columns = [col for col in final_columns if col in gold_fact_orders.columns]
            final_df = gold_fact_orders[available_columns].copy()
            
            # Renombrar columnas para consistencia
            column_renames = {
                'request_goods_receipt_date': 'requested_date',
                'sales_order_document_number': 'order_number',
                'sales_order_document_line_item_number': 'line_item',
                'net_order_quantity': 'quantity',
                'sales_order_document_line_item_status': 'order_status'
            }
            
            for old_col, new_col in column_renames.items():
                if old_col in final_df.columns:
                    final_df = final_df.rename(columns={old_col: new_col})
            
            record_count = len(final_df)
            final_df.to_sql('gold_fact_orders', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_fact_orders: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_orders: {str(e)}")
            raise
    
    def create_gold_fact_budget(self) -> int:
        """Crea tabla de hechos de presupuesto"""
        try:
            logger.info("Creando gold_fact_budget")
            
            budget_query = "SELECT * FROM silver_fact_budget"
            budget_df = pd.read_sql(budget_query, self.engine)
            
            # Convertir columna month a fecha
            if 'month' in budget_df.columns:
                budget_df['budget_month'] = pd.to_datetime(budget_df['month']).dt.date
            
            # Seleccionar columnas finales
            final_columns = ['customer_key', 'product_key', 'budget_month', 'total_budget']
            available_columns = [col for col in final_columns if col in budget_df.columns]
            final_df = budget_df[available_columns].copy()
            
            # Renombrar para consistencia
            if 'total_budget' in final_df.columns:
                final_df = final_df.rename(columns={'total_budget': 'budget_eur'})
            
            # Filtrar registros válidos
            final_df = final_df.dropna(subset=['budget_month'])
            
            record_count = len(final_df)
            final_df.to_sql('gold_fact_budget', self.engine, if_exists='replace', index=False)
            
            logger.info(f"gold_fact_budget: {record_count:,} registros")
            return record_count
            
        except Exception as e:
            logger.error(f"Error creando gold_fact_budget: {str(e)}")
            raise

class GoldProcessor:
    """Procesador principal de la capa Gold"""
    
    def __init__(self):
        self.postgres_manager = PostgreSQLManager()
        self.business_models = GoldBusinessModels(self.postgres_manager)
    
    def execute_gold_pipeline(self, silver_results: Dict = None) -> Dict:
        """Ejecuta el pipeline completo de Gold"""
        try:
            logger.info("=" * 60)
            logger.info("INICIANDO GOLD LAYER PIPELINE")
            logger.info("=" * 60)
            
            # Validar que Silver esté disponible
            if silver_results and silver_results.get('successful_tables', 0) == 0:
                raise ValueError("No hay tablas Silver exitosas disponibles")
            
            # Verificar tablas Silver necesarias
            if not self.business_models.verify_silver_tables():
                raise ValueError("Las tablas Silver requeridas no están disponibles")
            
            results = []
            total_records = 0
            
            # Definir tablas Gold a crear
            gold_tables = [
                ('gold_dim_customer', self.business_models.create_gold_dim_customer),
                ('gold_dim_product', self.business_models.create_gold_dim_product),
                ('gold_fact_sales', self.business_models.create_gold_fact_sales),
                ('gold_fact_orders', self.business_models.create_gold_fact_orders),
                ('gold_fact_budget', self.business_models.create_gold_fact_budget)
            ]
            
            # Procesar cada tabla Gold
            for table_name, create_function in gold_tables:
                try:
                    record_count = create_function()
                    results.append({
                        'table': table_name, 
                        'records': record_count, 
                        'status': 'success',
                        'processing_time': datetime.now().isoformat()
                    })
                    total_records += record_count
                    logger.info(f"Gold table {table_name}: {record_count:,} registros")
                    
                except Exception as e:
                    results.append({
                        'table': table_name, 
                        'records': 0, 
                        'status': 'failed', 
                        'error': str(e),
                        'processing_time': datetime.now().isoformat()
                    })
                    logger.error(f"Error creating {table_name}: {str(e)}")
            
            # Calcular métricas del pipeline
            successful = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'failed']
            
            pipeline_result = {
                'execution_id': datetime.now().isoformat(),
                'total_tables': len(gold_tables),
                'successful_tables': len(successful),
                'failed_tables': len(failed),
                'total_records': total_records,
                'status': 'completed' if len(failed) == 0 else 'completed_with_errors',
                'layer': 'gold',
                'results': results
            }
            
            logger.info("=" * 60)
            logger.info("GOLD LAYER RESUMEN:")
            logger.info(f"  Tablas exitosas: {len(successful)}/{len(gold_tables)}")
            logger.info(f"  Tablas fallidas: {len(failed)}")
            logger.info(f"  Total registros: {total_records:,}")
            logger.info(f"  Status: {pipeline_result['status']}")
            logger.info("=" * 60)
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"Error en Gold pipeline: {e}")
            raise