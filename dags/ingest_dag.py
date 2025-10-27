import pendulum
from airflow.decorators import dag
# Import PostgresOperator with fallbacks for different Airflow versions / linters
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
except ImportError:
    try:
        from airflow.operators.postgres_operator import PostgresOperator
    except ImportError:
        from airflow.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # Thêm import

OMS_TABLES = ["customers", "categories", "departments", "products", "orders", "order_items"]
SLMS_TABLES = ["shipping"]

@dag(
    dag_id="ingest_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["project", "ingest", "elt"]
)
def ingest_dag():
    """
    ### DAG để Ingest dữ liệu từ OLTP vào Staging
    Và kích hoạt transform_dag sau khi hoàn tất.
    """

    ingest_tasks = [] # Tạo một list rỗng

    # 1. Tạo tasks cho OMS
    for table in OMS_TABLES:
        task = PostgresOperator(
            task_id=f"ingest_oms_{table}",
            postgres_conn_id="dwh_postgres_conn",
            sql=f"""
                TRUNCATE TABLE staging.stg_{table};
                INSERT INTO staging.stg_{table}
                SELECT * FROM oms_oltp.{table};
            """
        )
        ingest_tasks.append(task) # Thêm task vào list

    # 2. Tạo tasks cho SLMS
    for table in SLMS_TABLES:
        task = PostgresOperator(
            task_id=f"ingest_slms_{table}",
            postgres_conn_id="dwh_postgres_conn",
            sql=f"""
                TRUNCATE TABLE staging.stg_{table};
                INSERT INTO staging.stg_{table}
                SELECT * FROM slms_oltp.{table};
            """
        )
        ingest_tasks.append(task) # Thêm task vào list

    # 3. Task cuối cùng: Kích hoạt transform_dag
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_dag",
        wait_for_completion=False,
    )

    # 4. Định nghĩa thứ tự:
    # Tất cả các task trong list ingest_tasks phải chạy xong
    # Sau đó mới chạy task trigger_transform_dag
    ingest_tasks >> trigger_transform_dag

# Gọi hàm dag
ingest_dag()