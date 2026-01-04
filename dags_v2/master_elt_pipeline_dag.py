"""
master_elt_pipeline_dag.py

Master DAG that orchestrates the complete Northwind ELT pipeline:
1. Ingest data from CSV to staging
2. Transform staging to dimensional model
3. Run data quality validation
4. Generate metrics and alerts
"""

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="master_elt_pipeline_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 2 * * *",  # Run daily at 2 AM
    catchup=False,
    tags=["elt", "master", "orchestration", "northwind", "pipeline"],
    doc_md="""
    # Master Northwind ELT Pipeline DAG
    
    Orchestrates the complete end-to-end ELT pipeline:
    
    ## Pipeline Stages:
    1. **Pre-flight Checks**: Verify database connectivity and schemas
    2. **Ingestion**: Load all CSV files to staging tables
    3. **Transformation**: Build dimensional model from staging
    4. **Validation**: Run data quality checks
    5. **Post-processing**: Generate metrics and audit records
    
    ## Schedule:
    - Runs daily at 2:00 AM Vietnam time
    - Can be manually triggered for ad-hoc refreshes
    """,
)
def master_elt_pipeline_dag():

    @task
    def preflight_check():
        """Pre-flight validation before starting the pipeline"""
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
            print("! Required schemas not found, will be created during ingestion")
        else:
            print("✓ Required schemas exist")
        
        print("=== Pre-flight Checks Completed ===")
        return {"status": "ready", "timestamp": str(pendulum.now())}

    @task
    def record_pipeline_start():
        """Record pipeline execution start time"""
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
            VALUES ('master_northwind_pipeline', CURRENT_DATE, CURRENT_TIMESTAMP, 'running')
        """)
        
        print("Pipeline execution recorded in audit table")
        return {"status": "started"}

    # Trigger the ingestion DAG
    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        trigger_dag_id="ingest_to_staging",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Trigger the transformation DAG
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="transform_to_warehouse",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Trigger the data quality DAG
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_validation_dag",
        trigger_dag_id="data_quality_checks",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Trigger the reporting aggregates DAG
    trigger_reporting = TriggerDagRunOperator(
        task_id="trigger_reporting_aggregates_dag",
        trigger_dag_id="build_reporting_aggregates",
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
        
        # Count records in staging tables
        staging_tables = [
            ("staging.stg_customers", "staging_customers"),
            ("staging.stg_employees", "staging_employees"),
            ("staging.stg_orders", "staging_orders"),
            ("staging.stg_order_details", "staging_order_details"),
            ("staging.stg_products", "staging_products"),
        ]
        
        # Count records in DWH tables
        dwh_tables = [
            ("dwh.dim_customer", "dim_customers"),
            ("dwh.dim_employee", "dim_employees"),
            ("dwh.dim_shipper", "dim_shippers"),
            ("dwh.dim_product", "dim_products"),
            ("dwh.dim_date", "dim_dates"),
            ("dwh.dim_geography", "dim_geographies"),
            ("dwh.order_items", "fact_order_items"),
        ]
        
        all_tables = staging_tables + dwh_tables
        
        for table, metric_name in all_tables:
            try:
                count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
                metrics[metric_name] = count
            except Exception as e:
                metrics[metric_name] = 0
                print(f"Warning: Could not count {table}: {e}")
        
        # Calculate summary statistics from fact table
        try:
            summary = hook.get_first("""
                SELECT 
                    SUM(line_total) as total_sales,
                    COUNT(DISTINCT customer_key) as unique_customers,
                    COUNT(DISTINCT product_key) as unique_products,
                    COUNT(*) as total_line_items
                FROM dwh.order_items
            """)
            
            metrics["total_sales"] = float(summary[0]) if summary[0] else 0
            metrics["unique_customers"] = summary[1] or 0
            metrics["unique_products"] = summary[2] or 0
            metrics["total_line_items"] = summary[3] or 0
        except Exception as e:
            print(f"Warning: Could not calculate summary: {e}")
        
        print("\n=== Pipeline Metrics ===")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"  {key}: ${value:,.2f}")
            elif isinstance(value, int):
                print(f"  {key}: {value:,}")
            else:
                print(f"  {key}: {value}")
        
        return metrics

    @task
    def record_pipeline_completion(metrics):
        """Record pipeline completion and final metrics"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        records_processed = metrics.get('fact_order_items', 0)
        
        # Update audit record
        hook.run(f"""
            UPDATE dwh.pipeline_audit
            SET end_time = CURRENT_TIMESTAMP,
            status = 'completed',
            records_processed = {records_processed}
            WHERE pipeline_name = 'master_northwind_pipeline'
              AND execution_date = CURRENT_DATE
              AND end_time IS NULL
        """)
        
        print("\n=== Pipeline Completed Successfully ===")
        print(f"Total records processed: {records_processed:,}")
        print(f"Total sales: ${metrics.get('total_sales', 0):,.2f}")
        
        return {"status": "completed", "metrics": metrics}

    # Define task dependencies
    preflight = preflight_check()
    audit_start = record_pipeline_start()
    
    # Chain the pipeline stages: ingest -> transform -> quality -> reporting
    preflight >> audit_start >> trigger_ingest >> trigger_transform >> trigger_quality >> trigger_reporting
    
    # Generate metrics and complete
    metrics = generate_pipeline_metrics()
    completion = record_pipeline_completion(metrics)
    
    trigger_reporting >> metrics >> completion


master_elt_pipeline_dag()