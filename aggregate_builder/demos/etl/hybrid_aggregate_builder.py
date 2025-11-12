"""
Aggregate Builder ETL - Hybrid Schema Version
Load DataCo CSV → Staging → DWH (dwh schema) → Aggregates
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HybridAggregateBuilder:
    """ETL Pipeline cho Hybrid Schema: staging → dwh → aggregates"""
    
    def __init__(self, csv_path, db_connection_string):
        self.csv_path = csv_path
        self.engine = create_engine(db_connection_string)
        self.df = None
        
    def load_csv(self):
        """Load CSV"""
        logger.info(f"Loading CSV from {self.csv_path}")
        self.df = pd.read_csv(self.csv_path, encoding='latin-1')
        logger.info(f"Loaded {len(self.df)} rows")
        return True
    
    def init_schemas(self):
        """Run 01_init_hybrid_schema.sql"""
        logger.info("Initializing hybrid schema...")
        with open('D:/1st (24-25)/DW/MiAI_Airflow/demos/sql/01_init_hybrid_schema.sql', 'r', encoding='utf-8') as f:
            sql = f.read()
        
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        logger.info("✓ Schemas initialized")
    
    def populate_dim_date(self, start_year=2015, end_year=2025):
        """Populate dwh.dim_date"""
        # Check if already populated
        date_count = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_date", self.engine).iloc[0]['cnt']
        if date_count > 0:
            logger.info(f"✓ dim_date already populated ({date_count} rows), skipping...")
            return
        
        logger.info("Populating dwh.dim_date...")
        
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
        dim_date_df.to_sql('dim_date', self.engine, schema='dwh', if_exists='append', index=False, method='multi')
        logger.info(f"✓ Inserted {len(dim_date_df)} dates")
    
    def populate_dimensions(self):
        """Populate all dimension tables in dwh schema"""
        
        # Check if dimensions already populated
        dim_count = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_product", self.engine).iloc[0]['cnt']
        if dim_count > 0:
            logger.info(f"✓ Dimensions already populated (dim_product has {dim_count} rows), skipping...")
            return
        
        # dwh.dim_department
        logger.info("Populating dwh.dim_department...")
        dept_df = self.df[['Department Id', 'Department Name']].drop_duplicates()
        dept_df.columns = ['department_id', 'department_name']
        dept_df.to_sql('dim_department', self.engine, schema='dwh', if_exists='append', index=False)
        
        # dwh.dim_category
        logger.info("Populating dwh.dim_category...")
        dept_map = pd.read_sql("SELECT department_id, department_key FROM dwh.dim_department", self.engine)
        cat_df = self.df[['Category Id', 'Category Name', 'Department Id']].drop_duplicates()
        cat_df = cat_df.merge(dept_map, left_on='Department Id', right_on='department_id', how='left')
        cat_df = cat_df[['Category Id', 'Category Name', 'department_key']].copy()
        cat_df.columns = ['category_id', 'category_name', 'department_key']
        cat_df.to_sql('dim_category', self.engine, schema='dwh', if_exists='append', index=False)
        
        # dwh.dim_product
        logger.info("Populating dwh.dim_product...")
        cat_map = pd.read_sql("SELECT category_id, category_key FROM dwh.dim_category", self.engine)
        prod_df = self.df[['Product Card Id', 'Product Name', 'Product Price', 
                          'Product Image', 'Category Id']].drop_duplicates(subset=['Product Card Id'])
        prod_df = prod_df.merge(cat_map, left_on='Category Id', right_on='category_id', how='left')
        prod_df['product_id'] = prod_df['Product Card Id']
        prod_df = prod_df[['category_key', 'product_id', 'Product Name', 'Product Card Id',
                          'Product Price', 'Product Image']].copy()
        prod_df.columns = ['category_key', 'product_id', 'product_name', 'product_card_id',
                          'product_price', 'product_image']
        prod_df.to_sql('dim_product', self.engine, schema='dwh', if_exists='append', index=False)
        
        # dwh.dim_customer
        logger.info("Populating dwh.dim_customer...")
        cust_df = self.df[['Customer Id', 'Customer Fname', 'Customer Lname',
                          'Customer Segment', 'Customer City', 'Customer State',
                          'Customer Country']].drop_duplicates(subset=['Customer Id'])
        cust_df.columns = ['customer_id', 'customer_fname', 'customer_lname',
                          'customer_segment', 'customer_city', 'customer_state', 'customer_country']
        cust_df.to_sql('dim_customer', self.engine, schema='dwh', if_exists='append', index=False)
        
        # dwh.dim_geography
        logger.info("Populating dwh.dim_geography...")
        cust_geo = self.df[['Customer City', 'Customer State', 'Customer Country',
                           'Order Region', 'Market', 'Latitude', 'Longitude']].copy()
        cust_geo.columns = ['city', 'state', 'country', 'region', 'market', 'latitude', 'longitude']
        
        order_geo = self.df[['Order City', 'Order State', 'Order Country',
                            'Order Region', 'Market', 'Latitude', 'Longitude']].copy()
        order_geo.columns = ['city', 'state', 'country', 'region', 'market', 'latitude', 'longitude']
        
        geo_df = pd.concat([cust_geo, order_geo]).drop_duplicates(subset=['city', 'state', 'country', 'market'])
        geo_df.to_sql('dim_geography', self.engine, schema='dwh', if_exists='append', index=False)
        
        # dwh.dim_shipping
        logger.info("Populating dwh.dim_shipping...")
        ship_df = self.df[['Shipping Mode', 'Delivery Status', 'Late_delivery_risk']].drop_duplicates()
        ship_df.columns = ['shipping_mode', 'delivery_status', 'delivery_risk']
        ship_df['delivery_risk'] = ship_df['delivery_risk'].astype(str)
        ship_df.to_sql('dim_shipping', self.engine, schema='dwh', if_exists='append', index=False)
        
        logger.info("✓ All dimensions populated")
    
    def parse_date_key(self, date_str):
        """Convert date string to YYYYMMDD"""
        try:
            if pd.isna(date_str):
                return None
            dt = pd.to_datetime(date_str)
            return int(dt.strftime('%Y%m%d'))
        except:
            return None
    
    def populate_fact_table(self):
        """Populate dwh.order_items"""
        logger.info("Populating dwh.order_items...")
        
        # Load dimension mappings
        customer_map = pd.read_sql("SELECT customer_id, customer_key FROM dwh.dim_customer", self.engine)
        product_map = pd.read_sql("SELECT product_card_id, product_key FROM dwh.dim_product", self.engine)
        shipping_map = pd.read_sql("SELECT shipping_mode, delivery_status, delivery_risk, shipping_key FROM dwh.dim_shipping", self.engine)
        geo_map = pd.read_sql("SELECT city, state, country, market, geography_key FROM dwh.dim_geography", self.engine)
        
        fact_df = self.df.copy()

        # --- Normalise column names and provide fallbacks ---
        # Some datasets (e.g. the 13-column variant) use slightly different column names.
        # We'll map common variants to the canonical names the rest of the code expects.
        def find_col(df, candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            # case-insensitive exact
            cols_lower = {col.lower(): col for col in df.columns}
            for c in candidates:
                if c.lower() in cols_lower:
                    return cols_lower[c.lower()]
            # loose match (remove non-alphanum)
            def norm(s):
                import re
                return re.sub(r"[^0-9a-z]","", s.lower())
            norm_map = {norm(col): col for col in df.columns}
            for c in candidates:
                nc = norm(c)
                if nc in norm_map:
                    return norm_map[nc]
            return None

        # mapping of canonical name -> candidate column names
        candidates_map = {
            'Customer Id': ['Customer Id', 'Order Customer Id', 'customer id', 'OrderCustomerId'],
            'Product Card Id': ['Product Card Id', 'Order Item Cardprod Id', 'Order Item Cardprod Id', 'product card id'],
            'Shipping Mode': ['Shipping Mode', 'shipping mode'],
            'Delivery Status': ['Delivery Status', 'delivery status'],
            'Late_delivery_risk': ['Late_delivery_risk', 'Late delivery risk', 'late_delivery_risk'],
            'Customer City': ['Customer City', 'CustomerCity', 'customer city'],
            'Customer State': ['Customer State', 'customer state'],
            'Customer Country': ['Customer Country', 'customer country'],
            'Order City': ['Order City', 'OrderCity'],
            'Order State': ['Order State', 'order state'],
            'Order Country': ['Order Country', 'order country'],
            'Market': ['Market', 'market'],
            'order date (DateOrders)': ['order date (DateOrders)', 'order date', 'order_date', 'orderdate'],
            'shipping date (DateOrders)': ['shipping date (DateOrders)', 'shipping date', 'shipping_date', 'shippingdate'],
            'Order Id': ['Order Id', 'OrderId', 'order id'],
            'Order Item Id': ['Order Item Id', 'OrderItemId', 'Order Item Id'],
            'Order Status': ['Order Status', 'order status'],
            'Order Item Quantity': ['Order Item Quantity', 'Order Item Quantity'],
            'Sales': ['Sales', 'sales', 'Sales per customer'],
            'Order Item Discount': ['Order Item Discount', 'Order Item Discount'],
            'Order Profit Per Order': ['Order Profit Per Order', 'Order Profit Per Order', 'Order Profit Per Order'],
            'Order Item Discount Rate': ['Order Item Discount Rate', 'Order Item Discount Rate'],
            'Order Item Profit Ratio': ['Order Item Profit Ratio', 'Order Item Profit Ratio']
        }

        # Create canonical columns where possible (copy from found candidate)
        for canon, cands in candidates_map.items():
            found = find_col(fact_df, cands)
            if found and found != canon:
                fact_df[canon] = fact_df[found]
            if canon not in fact_df.columns:
                # ensure column exists to avoid KeyError later
                fact_df[canon] = pd.NA

        # Convert data types to match
        fact_df['Product Card Id'] = fact_df['Product Card Id'].astype(str)
        product_map['product_card_id'] = product_map['product_card_id'].astype(str)
        
        # Join dimensions
        fact_df = fact_df.merge(customer_map, left_on='Customer Id', right_on='customer_id', how='left')
        fact_df = fact_df.merge(product_map, left_on='Product Card Id', right_on='product_card_id', how='left')
        
        fact_df['delivery_risk_str'] = fact_df['Late_delivery_risk'].astype(str)
        fact_df = fact_df.merge(shipping_map,
                                left_on=['Shipping Mode', 'Delivery Status', 'delivery_risk_str'],
                                right_on=['shipping_mode', 'delivery_status', 'delivery_risk'],
                                how='left')
        
        # Customer geography
        fact_df = fact_df.merge(geo_map,
                                left_on=['Customer City', 'Customer State', 'Customer Country', 'Market'],
                                right_on=['city', 'state', 'country', 'market'],
                                how='left', suffixes=('', '_cust'))
        fact_df.rename(columns={'geography_key': 'customer_geography_key'}, inplace=True)
        
        # Order geography
        fact_df = fact_df.merge(geo_map,
                                left_on=['Order City', 'Order State', 'Order Country', 'Market'],
                                right_on=['city', 'state', 'country', 'market'],
                                how='left', suffixes=('', '_order'))
        fact_df.rename(columns={'geography_key': 'order_geography_key'}, inplace=True)
        
        # Date keys
        fact_df['order_date_key'] = fact_df['order date (DateOrders)'].apply(self.parse_date_key)
        fact_df['shipping_date_key'] = fact_df['shipping date (DateOrders)'].apply(self.parse_date_key)
        
        # Select fact columns
        fact_final = pd.DataFrame({
            'customer_key': fact_df['customer_key'],
            'order_date_key': fact_df['order_date_key'],
            'shipping_date_key': fact_df['shipping_date_key'],
            'customer_geography_key': fact_df['customer_geography_key'],
            'order_geography_key': fact_df['order_geography_key'],
            'shipping_key': fact_df['shipping_key'],
            'product_key': fact_df['product_key'],
            'order_id': fact_df['Order Id'],
            'order_item_id': fact_df['Order Item Id'],
            'order_status': fact_df['Order Status'],
            'order_item_quantity': fact_df['Order Item Quantity'],
            'sales': fact_df['Sales'],
            'order_item_discount': fact_df['Order Item Discount'],
            'order_item_profit': fact_df['Order Profit Per Order'],
            'order_item_discount_rate': fact_df['Order Item Discount Rate'],
            'order_item_profit_ratio': fact_df['Order Item Profit Ratio']
        })
        
        # Batch insert
        batch_size = 1000
        for i in range(0, len(fact_final), batch_size):
            batch = fact_final.iloc[i:i+batch_size]
            batch.to_sql('order_items', self.engine, schema='dwh', if_exists='append', index=False)
            logger.info(f"Batch {i//batch_size + 1}/{(len(fact_final)//batch_size)+1}")
        
        logger.info(f"✓ Inserted {len(fact_final)} fact records")
    
    def create_aggregates(self):
        """Create aggregate tables in dwh schema"""
        logger.info("Creating aggregates...")
        
        with open('D:/1st (24-25)/DW/MiAI_Airflow/demos/sql/02_create_aggregates.sql', 'r', encoding='utf-8') as f:
            sql = f.read()
        
        with self.engine.connect() as conn:
            for statement in sql.split(';'):
                if statement.strip():
                    try:
                        conn.execute(text(statement))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Statement warning: {e}")
        
        logger.info("✓ Aggregates created")
    
    def run_full_pipeline(self):
        """Run complete ETL"""
        logger.info("="*60)
        logger.info("HYBRID AGGREGATE BUILDER - FULL PIPELINE")
        logger.info("="*60)
        
        steps = [
            ("Load CSV", self.load_csv),
            ("Initialize Schemas", self.init_schemas),
            ("Populate Dim_Date", self.populate_dim_date),
            ("Populate Dimensions", self.populate_dimensions),
            ("Populate Fact Table", self.populate_fact_table),
            ("Create Aggregates", self.create_aggregates)
        ]
        
        for step_name, step_func in steps:
            try:
                logger.info(f"\n>>> {step_name}")
                step_func()
            except Exception as e:
                logger.error(f"✗ {step_name} failed: {e}")
                raise
        
        logger.info("="*60)
        logger.info("✓ PIPELINE COMPLETED!")
        logger.info("="*60)


if __name__ == "__main__":
    # Use original dataset path (restored per user request)
    CSV_PATH = "D:/1st (24-25)/DW/DataCoSupplyChainDataset - DataCoSupplyChainDataset_6k.csv"
    DB_CONN = "postgresql://root:root@localhost:2345/dwh_demo"

    builder = HybridAggregateBuilder(CSV_PATH, DB_CONN)
    builder.run_full_pipeline()
