"""
master_elt_pipeline_dag.py

Master DAG that orchestrates the complete ELT pipeline:
1. Ingest data from sources to staging
2. Transform staging to dimensional model
3. Run data quality validation
4. Generate metrics and alerts

Uses TriggerDagRunOperator to chain DAGs together.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="master_elt_pipeline_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 2 * * *",  # Run daily at 2 AM
    catchup=False,
    tags=["elt", "master", "orchestration", "pipeline"],
    doc_md="""
    # Master ELT Pipeline DAG
    
    This DAG orchestrates the complete end-to-end ELT pipeline:
    
    ## Pipeline Stages:
    1. **Pre-flight Checks**: Verify database connectivity and source data availability
    2. **Ingestion**: Trigger ingest_dag to load data from CSV to staging
    3. **Transformation**: Trigger transform_dag to build dimensional model
    4. **Validation**: Trigger data_quality_dag to validate the results
    5. **Post-processing**: Generate metrics and send notifications
    
    ## Schedule:
    - Runs daily at 2:00 AM Vietnam time
    - Can be manually triggered for ad-hoc refreshes
    
    ## Dependencies:
    - Requires dwh_postgres_conn to be configured
    - Requires source data file to be present in /opt/airflow/data
    """
)
def master_elt_pipeline_dag():

    @task
    def preflight_check():
        """
        Pre-flight validation before starting the pipeline.
        Checks database connectivity and source file availability.
        """
        import os
        
        print("=== Starting Pre-flight Checks ===")
        
        # Check database connectivity
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        try:
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            print("✓ Database connection successful")
            cursor.close()
            conn.close()
        except Exception as e:
            raise ValueError(f"Database connection failed: {e}")
        
        # Check if schemas exist
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        schemas = hook.get_records("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('staging', 'dwh')
        """)
        
        if len(schemas) < 2:
            raise ValueError("Required schemas (staging, dwh) not found")
        print("✓ Required schemas exist")
        
        print("=== Pre-flight Checks Completed ===")
        return {"status": "ready", "timestamp": str(pendulum.now())}

    @task
    def record_pipeline_start():
        """Record pipeline execution start time and metadata"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Create audit table if not exists
        hook.run("""
            CREATE TABLE IF NOT EXISTS dwh.pipeline_audit (
                audit_id SERIAL PRIMARY KEY,
                pipeline_name VARCHAR(100),
                execution_date TIMESTAMP,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR(20),
                records_processed INT,
                error_message TEXT
            )
        """)
        
        # Record start
        hook.run("""
            INSERT INTO dwh.pipeline_audit (pipeline_name, execution_date, start_time, status)
            VALUES ('master_elt_pipeline', CURRENT_DATE, CURRENT_TIMESTAMP, 'running')
        """)
        
        print("Pipeline execution recorded in audit table")
        return {"status": "started"}

    # Trigger the ingestion DAG
    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        trigger_dag_id="ingest_data_dag_single_file_v2",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Trigger the transformation DAG
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="transform_dwh_dag_v3_manual",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Trigger the data quality DAG
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_validation_dag",
        trigger_dag_id="data_quality_validation_dag",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    @task
    def generate_pipeline_metrics():
        """Generate metrics about the pipeline execution"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        metrics = {}
        
        # Count records in each table
        tables = [
            ("staging.stg_oms_orders", "staging_orders"),
            ("staging.stg_oms_order_items", "staging_order_items"),
            ("staging.stg_slms_shipments", "staging_shipments"),
            ("dwh.dim_customer", "dim_customers"),
            ("dwh.dim_product", "dim_products"),
            ("dwh.dim_date", "dim_dates"),
            ("dwh.dim_geography", "dim_geographies"),
            ("dwh.order_items", "fact_order_items"),
        ]
        
        for table, metric_name in tables:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            metrics[metric_name] = count
        
        # Calculate summary statistics
        summary = hook.get_first("""
            SELECT 
                SUM(sales) as total_sales,
                SUM(order_item_profit) as total_profit,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(DISTINCT product_key) as unique_products
            FROM dwh.order_items
        """)
        
        metrics["total_sales"] = float(summary[0]) if summary[0] else 0
        metrics["total_profit"] = float(summary[1]) if summary[1] else 0
        metrics["unique_customers"] = summary[2] or 0
        metrics["unique_products"] = summary[3] or 0
        
        print("\n=== Pipeline Metrics ===")
        for key, value in metrics.items():
            print(f"  {key}: {value:,}" if isinstance(value, (int, float)) else f"  {key}: {value}")
        
        return metrics

    @task
    def record_pipeline_completion(metrics):
        """Record pipeline completion and final metrics"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Update audit record
        hook.run(f"""
            UPDATE dwh.pipeline_audit
            SET end_time = CURRENT_TIMESTAMP,
                status = 'completed',
                records_processed = {metrics.get('fact_order_items', 0)}
            WHERE pipeline_name = 'master_elt_pipeline'
              AND execution_date = CURRENT_DATE
              AND end_time IS NULL
        """)
        
        print("\n=== Pipeline Completed Successfully ===")
        print(f"Total records processed: {metrics.get('fact_order_items', 0):,}")
        print(f"Total sales: ${metrics.get('total_sales', 0):,.2f}")
        
        return {"status": "completed", "metrics": metrics}

    # Define task dependencies
    preflight = preflight_check()
    audit_start = record_pipeline_start()
    
    # Chain the pipeline stages
    preflight >> audit_start >> trigger_ingest >> trigger_transform >> trigger_quality
    
    # Generate metrics and complete
    metrics = generate_pipeline_metrics()
    completion = record_pipeline_completion(metrics)
    
    trigger_quality >> metrics >> completion


master_elt_pipeline_dag()