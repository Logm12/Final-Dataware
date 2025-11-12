"""
Aggregate Builder ETL Script
Đọc DataCo Supply Chain CSV, tạo Star Schema, và build Aggregate Tables
Author: MiAI Demo
Date: 2025-11-12
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AggregateBuilder:
    """ETL Pipeline để xây dựng Star Schema và Aggregate Tables"""
    
    def __init__(self, csv_path, db_connection_string):
        """
        Args:
            csv_path: Path tới file CSV
            db_connection_string: PostgreSQL connection string
                Example: 'postgresql://user:password@localhost:5432/dwh_demo'
        """
        self.csv_path = csv_path
        self.engine = create_engine(db_connection_string)
        self.df = None
        
    def load_csv(self):
        """Load CSV file vào pandas DataFrame"""
        logger.info(f"Loading CSV from {self.csv_path}")
        try:
            self.df = pd.read_csv(self.csv_path, encoding='latin-1')
            logger.info(f"Loaded {len(self.df)} rows, {len(self.df.columns)} columns")
            return True
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return False
    
    def create_star_schema(self):
        """Tạo các bảng dimension và fact table"""
        logger.info("Creating Star Schema tables...")
        
        with open('D:/1st (24-25)/DW/MiAI_Airflow/demos/sql/01_create_star_schema.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        with self.engine.connect() as conn:
            # Execute từng statement (split by semicolon)
            for statement in sql_script.split(';'):
                if statement.strip():
                    try:
                        conn.execute(text(statement))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"SQL execution warning: {e}")
        
        logger.info("Star Schema created successfully!")
    
    def populate_dim_date(self, start_year=2015, end_year=2025):
        """Populate Dim_Date dimension"""
        logger.info("Populating Dim_Date...")
        
        date_range = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        
        date_data = []
        for date in date_range:
            date_key = int(date.strftime('%Y%m%d'))
            date_data.append({
                'date_key': date_key,
                'full_date': date,
                'day_of_week': date.strftime('%A'),
                'day_of_month': date.day,
                'month_number': date.month,
                'month_name': date.strftime('%B'),
                'quarter_number': (date.month - 1) // 3 + 1,
                'year': date.year,
                'is_weekend': date.weekday() >= 5
            })
        
        dim_date_df = pd.DataFrame(date_data)
        dim_date_df.to_sql('dim_date', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(dim_date_df)} dates into Dim_Date")
    
    def populate_dim_department(self):
        """Populate Dim_Department"""
        logger.info("Populating Dim_Department...")
        
        dept_df = self.df[['Department Id', 'Department Name']].drop_duplicates()
        dept_df.columns = ['department_id', 'department_name']
        
        dept_df.to_sql('dim_department', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(dept_df)} departments")
    
    def populate_dim_category(self):
        """Populate Dim_Category"""
        logger.info("Populating Dim_Category...")
        
        # Get department_key mapping
        dept_mapping = pd.read_sql("SELECT department_id, department_key FROM dim_department", self.engine)
        
        cat_df = self.df[['Category Id', 'Category Name', 'Department Id']].drop_duplicates()
        cat_df = cat_df.merge(dept_mapping, left_on='Department Id', right_on='department_id', how='left')
        cat_df = cat_df[['Category Id', 'Category Name', 'department_key']].copy()
        cat_df.columns = ['category_id', 'category_name', 'department_key']
        
        cat_df.to_sql('dim_category', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(cat_df)} categories")
    
    def populate_dim_product(self):
        """Populate Dim_Product"""
        logger.info("Populating Dim_Product...")
        
        # Get category_key mapping
        cat_mapping = pd.read_sql("SELECT category_id, category_key FROM dim_category", self.engine)
        
        prod_df = self.df[['Product Card Id', 'Product Name', 'Product Description', 
                          'Product Price', 'Product Image', 'Product Status', 
                          'Category Id']].drop_duplicates(subset=['Product Card Id'])
        
        prod_df = prod_df.merge(cat_mapping, left_on='Category Id', right_on='category_id', how='left')
        prod_df = prod_df[['Product Card Id', 'Product Name', 'Product Description',
                          'Product Price', 'Product Image', 'Product Status', 'category_key']].copy()
        prod_df.columns = ['product_card_id', 'product_name', 'product_description',
                          'product_price', 'product_image', 'product_status', 'category_key']
        
        prod_df.to_sql('dim_product', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(prod_df)} products")
    
    def populate_dim_customer(self):
        """Populate Dim_Customer"""
        logger.info("Populating Dim_Customer...")
        
        cust_df = self.df[['Customer Id', 'Customer Fname', 'Customer Lname', 'Customer Email',
                          'Customer Segment', 'Customer City', 'Customer State', 
                          'Customer Country', 'Customer Zipcode', 'Customer Street']].drop_duplicates(subset=['Customer Id'])
        
        cust_df.columns = ['customer_id', 'customer_fname', 'customer_lname', 'customer_email',
                          'customer_segment', 'customer_city', 'customer_state',
                          'customer_country', 'customer_zipcode', 'customer_street']
        
        cust_df.to_sql('dim_customer', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(cust_df)} customers")
    
    def populate_dim_geography(self):
        """Populate Dim_Geography"""
        logger.info("Populating Dim_Geography...")
        
        # Customer geography
        cust_geo = self.df[['Customer City', 'Customer State', 'Customer Country', 
                           'Order Region', 'Market', 'Latitude', 'Longitude']].copy()
        cust_geo.columns = ['city', 'state', 'country', 'region', 'market', 'latitude', 'longitude']
        
        # Order geography
        order_geo = self.df[['Order City', 'Order State', 'Order Country',
                            'Order Region', 'Market', 'Latitude', 'Longitude']].copy()
        order_geo.columns = ['city', 'state', 'country', 'region', 'market', 'latitude', 'longitude']
        
        geo_df = pd.concat([cust_geo, order_geo]).drop_duplicates(subset=['city', 'state', 'country', 'market'])
        
        geo_df.to_sql('dim_geography', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(geo_df)} geography records")
    
    def populate_dim_shipping(self):
        """Populate Dim_Shipping"""
        logger.info("Populating Dim_Shipping...")
        
        ship_df = self.df[['Shipping Mode', 'Delivery Status', 'Late_delivery_risk']].drop_duplicates()
        ship_df.columns = ['shipping_mode', 'delivery_status', 'late_delivery_risk']
        
        ship_df.to_sql('dim_shipping', self.engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(ship_df)} shipping combinations")
    
    def parse_date_key(self, date_str):
        """Convert date string to date_key (YYYYMMDD)"""
        try:
            if pd.isna(date_str):
                return None
            dt = pd.to_datetime(date_str)
            return int(dt.strftime('%Y%m%d'))
        except:
            return None
    
    def populate_fact_order_items(self):
        """Populate Fact_Order_Items"""
        logger.info("Populating Fact_Order_Items...")
        
        # Load all dimension mappings
        customer_map = pd.read_sql("SELECT customer_id, customer_key FROM dim_customer", self.engine)
        product_map = pd.read_sql("SELECT product_card_id, product_key FROM dim_product", self.engine)
        shipping_map = pd.read_sql("SELECT shipping_mode, delivery_status, late_delivery_risk, shipping_key FROM dim_shipping", self.engine)
        
        # Geography mapping
        geo_map = pd.read_sql("SELECT city, state, country, market, geography_key FROM dim_geography", self.engine)
        
        # Prepare fact data
        fact_df = self.df.copy()
        
        # Join customer
        fact_df = fact_df.merge(customer_map, left_on='Customer Id', right_on='customer_id', how='left')
        
        # Join product
        fact_df = fact_df.merge(product_map, left_on='Product Card Id', right_on='product_card_id', how='left')
        
        # Join shipping
        fact_df = fact_df.merge(shipping_map, 
                                left_on=['Shipping Mode', 'Delivery Status', 'Late_delivery_risk'],
                                right_on=['shipping_mode', 'delivery_status', 'late_delivery_risk'],
                                how='left')
        
        # Join customer geography
        fact_df = fact_df.merge(geo_map,
                                left_on=['Customer City', 'Customer State', 'Customer Country', 'Market'],
                                right_on=['city', 'state', 'country', 'market'],
                                how='left',
                                suffixes=('', '_cust'))
        fact_df.rename(columns={'geography_key': 'customer_geography_key'}, inplace=True)
        
        # Join order geography
        fact_df = fact_df.merge(geo_map,
                                left_on=['Order City', 'Order State', 'Order Country', 'Market'],
                                right_on=['city', 'state', 'country', 'market'],
                                how='left',
                                suffixes=('', '_order'))
        fact_df.rename(columns={'geography_key': 'order_geography_key'}, inplace=True)
        
        # Parse date keys
        fact_df['order_date_key'] = fact_df['order date (DateOrders)'].apply(self.parse_date_key)
        fact_df['shipping_date_key'] = fact_df['shipping date (DateOrders)'].apply(self.parse_date_key)
        
        # Select fact columns
        fact_columns = {
            'customer_key': 'customer_key',
            'order_date_key': 'order_date_key',
            'shipping_date_key': 'shipping_date_key',
            'customer_geography_key': 'customer_geography_key',
            'order_geography_key': 'order_geography_key',
            'shipping_key': 'shipping_key',
            'product_key': 'product_key',
            'Order Id': 'order_id',
            'Order Item Id': 'order_item_id',
            'Order Status': 'order_status',
            'Order Zipcode': 'order_zipcode',
            'Order Item Quantity': 'order_item_quantity',
            'Sales': 'sales',
            'Order Item Discount': 'order_item_discount',
            'Order Profit Per Order': 'order_profit_per_order',
            'Order Item Discount Rate': 'order_item_discount_rate',
            'Order Item Profit Ratio': 'order_item_profit_ratio',
            'Order Item Product Price': 'order_item_product_price',
            'Order Item Total': 'order_item_total',
            'Benefit per order': 'benefit_per_order',
            'Sales per customer': 'sales_per_customer'
        }
        
        fact_final = fact_df[list(fact_columns.keys())].copy()
        fact_final.columns = list(fact_columns.values())
        
        # Load in batches
        batch_size = 1000
        total_rows = len(fact_final)
        
        for i in range(0, total_rows, batch_size):
            batch = fact_final.iloc[i:i+batch_size]
            batch.to_sql('fact_order_items', self.engine, if_exists='append', index=False, method='multi')
            logger.info(f"Inserted batch {i//batch_size + 1}/{(total_rows//batch_size)+1}")
        
        logger.info(f"Inserted {total_rows} fact records")
    
    def create_aggregates(self):
        """Tạo các Aggregate Tables"""
        logger.info("Creating Aggregate Tables...")
        
        with open('D:/1st (24-25)/DW/MiAI_Airflow/demos/sql/02_create_aggregates.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        with self.engine.connect() as conn:
            for statement in sql_script.split(';'):
                if statement.strip():
                    try:
                        logger.info(f"Executing: {statement[:100]}...")
                        conn.execute(text(statement))
                        conn.commit()
                    except Exception as e:
                        logger.error(f"Error: {e}")
        
        logger.info("Aggregate Tables created!")
    
    def run_full_etl(self):
        """Chạy full ETL pipeline"""
        logger.info("=" * 60)
        logger.info("STARTING FULL ETL PIPELINE")
        logger.info("=" * 60)
        
        steps = [
            ("Load CSV", self.load_csv),
            ("Create Star Schema", self.create_star_schema),
            ("Populate Dim_Date", self.populate_dim_date),
            ("Populate Dim_Department", self.populate_dim_department),
            ("Populate Dim_Category", self.populate_dim_category),
            ("Populate Dim_Product", self.populate_dim_product),
            ("Populate Dim_Customer", self.populate_dim_customer),
            ("Populate Dim_Geography", self.populate_dim_geography),
            ("Populate Dim_Shipping", self.populate_dim_shipping),
            ("Populate Fact_Order_Items", self.populate_fact_order_items),
            ("Create Aggregates", self.create_aggregates)
        ]
        
        for step_name, step_func in steps:
            try:
                logger.info(f"\n>>> Step: {step_name}")
                step_func()
                logger.info(f"✓ {step_name} completed")
            except Exception as e:
                logger.error(f"✗ {step_name} failed: {e}")
                return False
        
        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        return True


def main():
    """Main execution"""
    # Configuration
    CSV_PATH = "D:/1st (24-25)/DW/DataCoSupplyChainDataset - DataCoSupplyChainDataset_6k.csv"
    DB_CONN = "postgresql://postgres:postgres@localhost:5432/dwh_demo"
    
    # Run ETL
    builder = AggregateBuilder(CSV_PATH, DB_CONN)
    success = builder.run_full_etl()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
