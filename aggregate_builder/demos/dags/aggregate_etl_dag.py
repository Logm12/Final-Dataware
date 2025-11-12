"""
Airflow DAG: Aggregate Builder ETL
Chạy ETL pipeline hàng ngày để refresh aggregate tables
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add ETL path
sys.path.append('/opt/airflow/demos/etl')

default_args = {
    'owner': 'miai',
    'depends_on_past': False,
    'email': ['admin@miai.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'aggregate_builder_etl',
    default_args=default_args,
    description='ETL pipeline to build and refresh aggregate tables',
    schedule_interval='0 2 * * *',  # Chạy lúc 2AM mỗi ngày
    start_date=days_ago(1),
    catchup=False,
    tags=['dwh', 'aggregate', 'etl'],
)


def check_source_data(**context):
    """Task 1: Check if source CSV exists"""
    import os
    csv_path = "/opt/airflow/data/DataCoSupplyChainDataset - DataCoSupplyChainDataset_6k.csv"
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    
    file_size = os.path.getsize(csv_path)
    print(f"✓ Source file found: {csv_path}")
    print(f"✓ File size: {file_size / 1024:.2f} KB")
    
    context['ti'].xcom_push(key='csv_path', value=csv_path)
    context['ti'].xcom_push(key='file_size', value=file_size)


def initialize_schemas(**context):
    """Task 2: Initialize database schemas"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    engine = create_engine(DB_CONN)
    
    sql_path = "/opt/airflow/demos/sql/01_init_hybrid_schema.sql"
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    with engine.connect() as conn:
        conn.execute(text(sql_script))
        conn.commit()
    
    print("✓ Schemas initialized (oms_oltp, slms_oltp, staging, dwh)")


def load_csv_to_staging(**context):
    """Task 3: Load CSV into staging tables"""
    import pandas as pd
    from sqlalchemy import create_engine
    
    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='check_source', key='csv_path')
    
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    engine = create_engine(DB_CONN)
    
    print(f"Loading CSV from {csv_path}")
    df = pd.read_csv(csv_path, encoding='latin-1')
    
    print(f"✓ Loaded {len(df)} rows")
    ti.xcom_push(key='row_count', value=len(df))


def populate_dimensions(**context):
    """Task 4: Populate dimension tables"""
    from hybrid_aggregate_builder import HybridAggregateBuilder
    
    CSV_PATH = context['ti'].xcom_pull(task_ids='check_source', key='csv_path')
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    
    builder = HybridAggregateBuilder(CSV_PATH, DB_CONN)
    builder.load_csv()
    builder.populate_dim_date()
    builder.populate_dimensions()
    
    print("✓ All dimensions populated")


def populate_fact_table(**context):
    """Task 5: Populate fact table"""
    from hybrid_aggregate_builder import HybridAggregateBuilder
    
    CSV_PATH = context['ti'].xcom_pull(task_ids='check_source', key='csv_path')
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    
    builder = HybridAggregateBuilder(CSV_PATH, DB_CONN)
    builder.load_csv()
    builder.populate_fact_table()
    
    print("✓ Fact table populated")


def build_aggregates(**context):
    """Task 6: Build aggregate tables"""
    from sqlalchemy import create_engine, text
    
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    engine = create_engine(DB_CONN)
    
    sql_path = "/opt/airflow/demos/sql/02_create_aggregates.sql"
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    with engine.connect() as conn:
        for statement in sql_script.split(';'):
            if statement.strip():
                conn.execute(text(statement))
                conn.commit()
    
    print("✓ Aggregates created")


def validate_aggregates(**context):
    """Task 7: Validate aggregate tables"""
    from sqlalchemy import create_engine
    import pandas as pd
    
    DB_CONN = os.getenv('DWH_DB_CONN', 'postgresql://postgres:postgres@postgres:5432/dwh_demo')
    engine = create_engine(DB_CONN)
    
    # Check aggregate summary
    summary_df = pd.read_sql("SELECT * FROM dwh.v_aggregate_summary ORDER BY row_count DESC", engine)
    
    print("\n" + "="*60)
    print("AGGREGATE SUMMARY")
    print("="*60)
    print(summary_df.to_string(index=False))
    print("="*60)
    
    # Validate row counts
    for _, row in summary_df.iterrows():
        if row['row_count'] == 0:
            raise ValueError(f"Aggregate table {row['table_name']} is empty!")
    
    print("✓ All aggregates validated successfully")
    
    context['ti'].xcom_push(key='aggregate_summary', value=summary_df.to_dict())


def send_success_notification(**context):
    """Task 8: Send success notification"""
    ti = context['ti']
    row_count = ti.xcom_pull(task_ids='load_csv', key='row_count')
    
    print("\n" + "🎉"*20)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"Processed {row_count} source records")
    print("All aggregate tables refreshed")
    print("🎉"*20 + "\n")


# Define tasks
task_check_source = PythonOperator(
    task_id='check_source',
    python_callable=check_source_data,
    provide_context=True,
    dag=dag,
)

task_init_schemas = PythonOperator(
    task_id='initialize_schemas',
    python_callable=initialize_schemas,
    provide_context=True,
    dag=dag,
)

task_load_csv = PythonOperator(
    task_id='load_csv',
    python_callable=load_csv_to_staging,
    provide_context=True,
    dag=dag,
)

task_populate_dims = PythonOperator(
    task_id='populate_dimensions',
    python_callable=populate_dimensions,
    provide_context=True,
    dag=dag,
)

task_populate_fact = PythonOperator(
    task_id='populate_fact',
    python_callable=populate_fact_table,
    provide_context=True,
    dag=dag,
)

task_build_aggs = PythonOperator(
    task_id='build_aggregates',
    python_callable=build_aggregates,
    provide_context=True,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_aggregates',
    python_callable=validate_aggregates,
    provide_context=True,
    dag=dag,
)

task_notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# Define dependencies (linear pipeline)
task_check_source >> task_init_schemas >> task_load_csv >> task_populate_dims >> task_populate_fact >> task_build_aggs >> task_validate >> task_notify
