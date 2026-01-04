
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine import Engine

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"
DATA_PATH = "/opt/airflow/data"

CSV_CONFIGS = {
    "customers": {
        "csv_file": "customer.csv",
        "staging_table": "stg_customers",
        "columns": {
            "id": "customer_id",
            "company": "company",
            "last_name": "last_name",
            "first_name": "first_name",
            "email_address": "email_address",
            "job_title": "job_title",
            "business_phone": "business_phone",
            "home_phone": "home_phone",
            "mobile_phone": "mobile_phone",
            "fax_number": "fax_number",
            "address": "address",
            "city": "city",
            "state_province": "state_province",
            "zip_postal_code": "zip_postal_code",
            "country_region": "country_region",
            "web_page": "web_page",
            "notes": "notes",
        },
        "date_columns": [],
    },
    "employees": {
        "csv_file": "employees.csv",
        "staging_table": "stg_employees",
        "columns": {
            "id": "employee_id",
            "company": "company",
            "last_name": "last_name",
            "first_name": "first_name",
            "email_address": "email_address",
            "job_title": "job_title",
            "business_phone": "business_phone",
            "home_phone": "home_phone",
            "mobile_phone": "mobile_phone",
            "fax_number": "fax_number",
            "address": "address",
            "city": "city",
            "state_province": "state_province",
            "zip_postal_code": "zip_postal_code",
            "country_region": "country_region",
            "web_page": "web_page",
            "notes": "notes",
        },
        "date_columns": [],
    },
    "orders": {
        "csv_file": "orders.csv",
        "staging_table": "stg_orders",
        "columns": {
            "id": "order_id",
            "employee_id": "employee_id",
            "customer_id": "customer_id",
            "order_date": "order_date",
            "shipped_date": "shipped_date",
            "shipper_id": "shipper_id",
            "ship_name": "ship_name",
            "ship_address": "ship_address",
            "ship_city": "ship_city",
            "ship_state_province": "ship_state_province",
            "ship_zip_postal_code": "ship_zip_postal_code",
            "ship_country_region": "ship_country_region",
            "shipping_fee": "shipping_fee",
            "taxes": "taxes",
            "payment_type": "payment_type",
            "paid_date": "paid_date",
            "notes": "notes",
            "tax_rate": "tax_rate",
            "tax_status_id": "tax_status_id",
            "status_id": "status_id",
        },
        "date_columns": ["order_date", "shipped_date", "paid_date"],
    },
    "order_details": {
        "csv_file": "order_details.csv",
        "staging_table": "stg_order_details",
        "columns": {
            "id": "order_detail_id",
            "order_id": "order_id",
            "product_id": "product_id",
            "quantity": "quantity",
            "unit_price": "unit_price",
            "discount": "discount",
            "status_id": "status_id",
            "date_allocated": "date_allocated",
            "purchase_order_id": "purchase_order_id",
            "inventory_id": "inventory_id",
        },
        "date_columns": ["date_allocated"],
    },
    "products": {
        "csv_file": "products.csv",
        "staging_table": "stg_products",
        "columns": {
            "id": "product_id",
            "supplier_ids": "supplier_ids",
            "product_code": "product_code",
            "product_name": "product_name",
            "description": "description",
            "standard_cost": "standard_cost",
            "list_price": "list_price",
            "reorder_level": "reorder_level",
            "target_level": "target_level",
            "quantity_per_unit": "quantity_per_unit",
            "discontinued": "discontinued",
            "minimum_reorder_quantity": "minimum_reorder_quantity",
            "category": "category",
        },
        "date_columns": [],
    },
    "shippers": {
        "csv_file": "shippers.csv",
        "staging_table": "stg_shippers",
        "columns": {
            "id": "shipper_id",
            "company": "company",
            "last_name": "last_name",
            "first_name": "first_name",
            "email_address": "email_address",
            "job_title": "job_title",
            "business_phone": "business_phone",
            "home_phone": "home_phone",
            "mobile_phone": "mobile_phone",
            "fax_number": "fax_number",
            "address": "address",
            "city": "city",
            "state_province": "state_province",
            "zip_postal_code": "zip_postal_code",
            "country_region": "country_region",
            "web_page": "web_page",
            "notes": "notes",
        },
        "date_columns": [],
    },
    "suppliers": {
        "csv_file": "suppliers.csv",
        "staging_table": "stg_suppliers",
        "columns": {
            "id": "supplier_id",
            "company": "company",
            "last_name": "last_name",
            "first_name": "first_name",
            "email_address": "email_address",
            "job_title": "job_title",
            "business_phone": "business_phone",
            "home_phone": "home_phone",
            "mobile_phone": "mobile_phone",
            "fax_number": "fax_number",
            "address": "address",
            "city": "city",
            "state_province": "state_province",
            "zip_postal_code": "zip_postal_code",
            "country_region": "country_region",
            "web_page": "web_page",
            "notes": "notes",
        },
        "date_columns": [],
    },
    "invoices": {
        "csv_file": "invoices.csv",
        "staging_table": "stg_invoices",
        "columns": {
            "id": "invoice_id",
            "order_id": "order_id",
            "invoice_date": "invoice_date",
            "due_date": "due_date",
            "tax": "tax",
            "shipping": "shipping",
            "amount_due": "amount_due",
        },
        "date_columns": ["invoice_date", "due_date"],
    },
    "orders_status": {
        "csv_file": "orders_status.csv",
        "staging_table": "stg_orders_status",
        "columns": {
            "id": "status_id",
            "status_name": "status_name",
        },
        "date_columns": [],
    },
    "order_details_status": {
        "csv_file": "order_details_status.csv",
        "staging_table": "stg_order_details_status",
        "columns": {
            "id": "status_id",
            "status_name": "status_name",
        },
        "date_columns": [],
    },
    "inventory_transactions": {
        "csv_file": "inventory_transactions.csv",
        "staging_table": "stg_inventory_transactions",
        "columns": {
            "id": "transaction_id",
            "transaction_type": "transaction_type",
            "transaction_created_date": "transaction_created_date",
            "transaction_modified_date": "transaction_modified_date",
            "product_id": "product_id",
            "quantity": "quantity",
            "purchase_order_id": "purchase_order_id",
            "customer_order_id": "customer_order_id",
            "comments": "comments",
        },
        "date_columns": ["transaction_created_date", "transaction_modified_date"],
    },
    "inventory_transaction_types": {
        "csv_file": "inventory_transaction_types.csv",
        "staging_table": "stg_inventory_transaction_types",
        "columns": {
            "id": "type_id",
            "type_name": "type_name",
        },
        "date_columns": [],
    },
    "purchase_orders": {
        "csv_file": "purchase_orders.csv",
        "staging_table": "stg_purchase_orders",
        "columns": {
            "id": "purchase_order_id",
            "supplier_id": "supplier_id",
            "created_by": "created_by",
            "submitted_date": "submitted_date",
            "creation_date": "creation_date",
            "status_id": "status_id",
            "expected_date": "expected_date",
            "shipping_fee": "shipping_fee",
            "taxes": "taxes",
            "payment_date": "payment_date",
            "payment_amount": "payment_amount",
            "payment_method": "payment_method",
            "notes": "notes",
            "approved_by": "approved_by",
            "approved_date": "approved_date",
            "submitted_by": "submitted_by",
        },
        "date_columns": ["submitted_date", "creation_date", "expected_date", "payment_date", "approved_date"],
    },
    "purchase_order_details": {
        "csv_file": "purchase_order_details.csv",
        "staging_table": "stg_purchase_order_details",
        "columns": {
            "id": "purchase_order_detail_id",
            "purchase_order_id": "purchase_order_id",
            "product_id": "product_id",
            "quantity": "quantity",
            "unit_cost": "unit_cost",
            "date_received": "date_received",
            "posted_to_inventory": "posted_to_inventory",
            "inventory_id": "inventory_id",
        },
        "date_columns": ["date_received"],
    },
    "purchase_order_status": {
        "csv_file": "purchase_order_status.csv",
        "staging_table": "stg_purchase_order_status",
        "columns": {
            "id": "status_id",
            "status": "status",
        },
        "date_columns": [],
    },
}

# Main staging tables to truncate (core tables only)
CORE_STAGING_TABLES = [
    "stg_customers",
    "stg_employees", 
    "stg_orders",
    "stg_order_details",
    "stg_products",
    "stg_shippers",
    "stg_suppliers",
    "stg_invoices",
    "stg_orders_status",
    "stg_order_details_status",
    "stg_inventory_transactions",
    "stg_inventory_transaction_types",
    "stg_purchase_orders",
    "stg_purchase_order_details",
    "stg_purchase_order_status",
]


def _get_postgres_engine():
    """Get SQLAlchemy engine from Airflow connection"""
    hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
    engine: Engine = hook.get_sqlalchemy_engine()
    return engine


def _load_csv_to_staging(config_name: str, config: dict):
    """Generic function to load a CSV file to staging table"""
    csv_path = f"{DATA_PATH}/{config['csv_file']}"
    table_name = config["staging_table"]
    col_mapping = config["columns"]
    date_cols = config["date_columns"]
    
    print(f"[{config_name}] Reading file: {csv_path}")
    
    try:
        # Read CSV with proper encoding
        df = pd.read_csv(
            csv_path,
            encoding='utf-8',
            parse_dates=date_cols if date_cols else False,
        )
        
        print(f"[{config_name}] Read {len(df)} rows from CSV")
        
        # Rename columns according to mapping
        # Only rename columns that exist in the dataframe
        existing_cols = {k: v for k, v in col_mapping.items() if k in df.columns}
        df = df[list(existing_cols.keys())]  # Select only mapped columns
        df = df.rename(columns=existing_cols)
        
        # Add metadata columns
        df["load_timestamp"] = pd.Timestamp.now()
        df["source_file"] = config["csv_file"]
        
        # Load to staging
        df.to_sql(
            table_name,
            _get_postgres_engine(),
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )
        
        print(f"[{config_name}] Loaded {len(df)} rows to staging.{table_name}")
        return len(df)
        
    except FileNotFoundError:
        print(f"[{config_name}] WARNING: File not found: {csv_path}")
        return 0
    except Exception as e:
        print(f"[{config_name}] ERROR: {str(e)}")
        raise


@dag(
    dag_id="ingest_to_staging",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["elt", "ingest", "northwind"],
    doc_md="""
    # Northwind Data Ingestion DAG
    
    Ingests all CSV files from the Northwind dataset into staging tables.
    
    ## Source Files:
    - customer.csv → staging.stg_customers
    - employees.csv → staging.stg_employees
    - orders.csv → staging.stg_orders
    - order_details.csv → staging.stg_order_details
    - products.csv → staging.stg_products
    - shippers.csv → staging.stg_shippers
    - suppliers.csv → staging.stg_suppliers
    - invoices.csv → staging.stg_invoices
    - And more lookup tables...
    """,
)
def ingest_to_staging():
    
    @task
    def create_staging_tables():
        """Create staging tables if they don't exist"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Create staging schema if not exists
        hook.run("CREATE SCHEMA IF NOT EXISTS staging;")
        
        print("Staging schema ready.")
        return {"status": "ready"}
    
    @task
    def truncate_staging_tables():
        """Truncate all staging tables before loading"""
        print("Truncating staging tables...")
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        for table in CORE_STAGING_TABLES:
            try:
                hook.run(f"TRUNCATE TABLE staging.{table};")
                print(f"  Truncated staging.{table}")
            except Exception as e:
                # Table might not exist yet on first run
                print(f"  Note: Could not truncate staging.{table}: {e}")
        
        print("Staging tables truncated.")
        return {"status": "truncated"}
    
    # Create individual load tasks for each CSV
    @task
    def load_customers():
        return _load_csv_to_staging("customers", CSV_CONFIGS["customers"])
    
    @task
    def load_employees():
        return _load_csv_to_staging("employees", CSV_CONFIGS["employees"])
    
    @task
    def load_orders():
        return _load_csv_to_staging("orders", CSV_CONFIGS["orders"])
    
    @task
    def load_order_details():
        return _load_csv_to_staging("order_details", CSV_CONFIGS["order_details"])
    
    @task
    def load_products():
        return _load_csv_to_staging("products", CSV_CONFIGS["products"])
    
    @task
    def load_shippers():
        return _load_csv_to_staging("shippers", CSV_CONFIGS["shippers"])
    
    @task
    def load_suppliers():
        return _load_csv_to_staging("suppliers", CSV_CONFIGS["suppliers"])
    
    @task
    def load_invoices():
        return _load_csv_to_staging("invoices", CSV_CONFIGS["invoices"])
    
    @task
    def load_orders_status():
        return _load_csv_to_staging("orders_status", CSV_CONFIGS["orders_status"])
    
    @task
    def load_order_details_status():
        return _load_csv_to_staging("order_details_status", CSV_CONFIGS["order_details_status"])
    
    @task
    def load_inventory_transactions():
        return _load_csv_to_staging("inventory_transactions", CSV_CONFIGS["inventory_transactions"])
    
    @task
    def load_inventory_transaction_types():
        return _load_csv_to_staging("inventory_transaction_types", CSV_CONFIGS["inventory_transaction_types"])
    
    @task
    def load_purchase_orders():
        return _load_csv_to_staging("purchase_orders", CSV_CONFIGS["purchase_orders"])
    
    @task
    def load_purchase_order_details():
        return _load_csv_to_staging("purchase_order_details", CSV_CONFIGS["purchase_order_details"])
    
    @task
    def load_purchase_order_status():
        return _load_csv_to_staging("purchase_order_status", CSV_CONFIGS["purchase_order_status"])
    
    @task
    def generate_load_summary(
        customers, employees, orders, order_details, products,
        shippers, suppliers, invoices, orders_status, order_details_status,
        inventory_transactions, inventory_transaction_types,
        purchase_orders, purchase_order_details, purchase_order_status
    ):
        """Generate summary of loaded data"""
        summary = {
            "customers": customers,
            "employees": employees,
            "orders": orders,
            "order_details": order_details,
            "products": products,
            "shippers": shippers,
            "suppliers": suppliers,
            "invoices": invoices,
            "orders_status": orders_status,
            "order_details_status": order_details_status,
            "inventory_transactions": inventory_transactions,
            "inventory_transaction_types": inventory_transaction_types,
            "purchase_orders": purchase_orders,
            "purchase_order_details": purchase_order_details,
            "purchase_order_status": purchase_order_status,
        }
        
        print("\n" + "=" * 50)
        print("INGESTION SUMMARY")
        print("=" * 50)
        total = 0
        for name, count in summary.items():
            print(f"  {name}: {count:,} rows")
            total += count
        print("-" * 50)
        print(f"  TOTAL: {total:,} rows")
        print("=" * 50)
        
        return summary
    
    # Define task dependencies
    create_schema = create_staging_tables()
    truncate = truncate_staging_tables()
    
    create_schema >> truncate
    
    # Load all CSVs in parallel after truncation
    customers = load_customers()
    employees = load_employees()
    orders = load_orders()
    order_details = load_order_details()
    products = load_products()
    shippers = load_shippers()
    suppliers = load_suppliers()
    invoices = load_invoices()
    orders_status = load_orders_status()
    order_details_status = load_order_details_status()
    inventory_transactions = load_inventory_transactions()
    inventory_transaction_types = load_inventory_transaction_types()
    purchase_orders = load_purchase_orders()
    purchase_order_details = load_purchase_order_details()
    purchase_order_status = load_purchase_order_status()
    
    truncate >> [
        customers, employees, orders, order_details, products,
        shippers, suppliers, invoices, orders_status, order_details_status,
        inventory_transactions, inventory_transaction_types,
        purchase_orders, purchase_order_details, purchase_order_status
    ]
    
    summary = generate_load_summary(
        customers, employees, orders, order_details, products,
        shippers, suppliers, invoices, orders_status, order_details_status,
        inventory_transactions, inventory_transaction_types,
        purchase_orders, purchase_order_details, purchase_order_status
    )


ingest_to_staging()
