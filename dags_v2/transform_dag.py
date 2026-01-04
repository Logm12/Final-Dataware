"""
transform_dag.py

Transform DAG for Northwind data warehouse.
Builds dimensional model from staging tables.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"
SQL_TRANSFORM_PATH = "/opt/airflow/dags/sql/transform"


def _run_sql_script(script_name: str):
    """Execute a SQL script file"""
    hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
    file_path = f"{SQL_TRANSFORM_PATH}/{script_name}"
    
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
        
        print(f"--- Executing script: {script_name} ---")
        hook.run(sql_script)
        print(f"--- Executed {script_name} successfully. ---")
        
    except FileNotFoundError:
        print(f"Error: Cannot find {file_path}")
        raise
    except Exception as e:
        print(f"Error when running script {script_name}: {e}")
        raise


@dag(
    dag_id="transform_to_warehouse",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["elt", "transform", "northwind", "dwh"],
    doc_md="""
    # Northwind DWH Transform DAG
    
    Transforms staging data into dimensional model:
    
    ## Dimensions:
    - dim_customer - Customer master data
    - dim_employee - Employee/sales rep data
    - dim_shipper - Shipping company data
    - dim_product - Product catalog
    - dim_category - Product categories
    - dim_date - Date dimension (calendar)
    - dim_geography - Geographic locations
    
    ## Facts:
    - order_items - Order line item facts
    """,
)
def transform_to_warehouse():
    
    @task
    def truncate_dwh_tables():
        """Truncate all DWH tables before rebuild"""
        _run_sql_script("truncate_dwh_tables.sql")
    
    @task
    def create_indexes():
        """Create indexes for better performance"""
        _run_sql_script("create_indexes.sql")
    
    # Dimension build tasks
    @task
    def build_dim_customer():
        _run_sql_script("build_dim_customer.sql")
    
    @task
    def build_dim_employee():
        _run_sql_script("build_dim_employee.sql")
    
    @task
    def build_dim_shipper():
        _run_sql_script("build_dim_shipper.sql")
    
    @task
    def build_dim_category():
        _run_sql_script("build_dim_category.sql")
    
    @task
    def build_dim_product():
        """Build product dimension - depends on category"""
        _run_sql_script("build_dim_product.sql")
    
    @task
    def build_dim_date():
        _run_sql_script("build_dim_date.sql")
    
    @task
    def build_dim_geography():
        _run_sql_script("build_dim_geography.sql")
    
    # Fact table build task
    @task
    def build_fact_order_items():
        """Build fact table - depends on all dimensions"""
        _run_sql_script("build_fact_order_items.sql")
    
    @task
    def generate_transform_summary():
        """Generate summary of transformed data"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        tables = [
            "dwh.dim_customer",
            "dwh.dim_employee",
            "dwh.dim_shipper",
            "dwh.dim_category",
            "dwh.dim_product",
            "dwh.dim_date",
            "dwh.dim_geography",
            "dwh.order_items",
        ]
        
        print("\n" + "=" * 50)
        print("TRANSFORM SUMMARY")
        print("=" * 50)
        
        for table in tables:
            try:
                count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
                print(f"  {table}: {count:,} rows")
            except Exception as e:
                print(f"  {table}: ERROR - {e}")
        
        print("=" * 50)
        return {"status": "completed"}
    
    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================
    
    # Step 1: Truncate all DWH tables
    truncate_task = truncate_dwh_tables()
    
    # Step 2: Create indexes on staging tables
    index_task = create_indexes()
    
    # Step 3: Build independent dimensions in parallel
    customer_task = build_dim_customer()
    employee_task = build_dim_employee()
    shipper_task = build_dim_shipper()
    date_task = build_dim_date()
    geography_task = build_dim_geography()
    
    # Step 4: Build category first (product depends on it)
    category_task = build_dim_category()
    
    # Step 5: Build product (depends on category)
    product_task = build_dim_product()
    
    # Step 6: Build fact table (depends on all dimensions)
    fact_task = build_fact_order_items()
    
    # Step 7: Generate summary
    summary = generate_transform_summary()
    
    # Define dependency chain
    truncate_task >> index_task
    
    # All independent dimensions start after indexing
    index_task >> [customer_task, employee_task, shipper_task, date_task, geography_task]
    
    # Category also starts after indexing
    index_task >> category_task
    
    # Product depends on category
    category_task >> product_task
    
    # Fact table depends on all dimensions
    [customer_task, employee_task, shipper_task, date_task, geography_task, product_task] >> fact_task
    
    # Summary after fact table
    fact_task >> summary


transform_to_warehouse()