import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# BỎ import ExternalTaskSensor
DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"

SQL_TRANSFORM_PATH = "/opt/airflow/dags/sql/transform"


def _run_sql_script(script_name: str):
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
    dag_id="transform_dwh_dag_v3_manual", # Đã đổi tên 
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  
    catchup=False,
    tags=["elt", "transform", "dwh_final", "v3_manual"],
)
def transform_dwh_dag_v3_manual():
    
    @task
    def truncate_dwh_tables():
        _run_sql_script("truncate_dwh_tables.sql")

    @task
    def build_dim_customer():
        _run_sql_script("build_dim_customer.sql")

    @task
    def build_dim_geography():
        _run_sql_script("build_dim_geography.sql")

    @task
    def build_dim_date():
        _run_sql_script("build_dim_date.sql")

    @task
    def build_dim_shipping():
        _run_sql_script("build_dim_shipping.sql")

    @task
    def build_dim_department():
        _run_sql_script("build_dim_department.sql")

    @task
    def build_dim_category():
        _run_sql_script("build_dim_category.sql")
        
    @task
    def build_dim_product():
        _run_sql_script("build_dim_product.sql")

    @task
    def build_fact_order_items():
        _run_sql_script("build_fact_order_items.sql")

    
    @task
    def create_staging_indexes():
        _run_sql_script("create_indexes.sql")

    truncate_task = truncate_dwh_tables()
    index_task = create_staging_indexes()
    
    dept_task = build_dim_department()
    
    parallel_dims = [
        build_dim_customer(),
        build_dim_geography(),
        build_dim_date(),
        build_dim_shipping(),
    ]
    
    cat_task = build_dim_category()
    prod_task = build_dim_product()

    fact_task = build_fact_order_items()

    
    truncate_task >> index_task
    index_task >> dept_task
    index_task >> parallel_dims
    
    dept_task >> cat_task >> prod_task
    
    [prod_task] + parallel_dims >> fact_task

transform_dwh_dag_v3_manual()