"""
data_quality_dag.py

This DAG performs data quality checks on the DWH after transformation.
Validates referential integrity, data completeness, and business rules.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="data_quality_validation_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Triggered after transform DAG
    catchup=False,
    tags=["elt", "data_quality", "validation", "dwh"],
)
def data_quality_validation_dag():
    """
    Data Quality Validation DAG
    
    Performs comprehensive data quality checks on the dimensional model:
    1. Referential integrity checks
    2. Null value validation
    3. Uniqueness constraints
    4. Business rule validation
    5. Data completeness checks
    """

    @task
    def check_referential_integrity():
        """Check that all foreign keys in fact table reference valid dimension keys"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        checks = [
            ("customer_key", "dim_customer"),
            ("product_key", "dim_product"),
            ("order_date_key", "dim_date", "date_key"),
            ("shipping_date_key", "dim_date", "date_key"),
            ("customer_geography_key", "dim_geography", "geography_key"),
            ("order_geography_key", "dim_geography", "geography_key"),
            ("shipping_key", "dim_shipping"),
        ]
        
        results = []
        for check in checks:
            fk_col = check[0]
            dim_table = check[1]
            pk_col = check[2] if len(check) > 2 else fk_col.replace("_key", "_key")
            if pk_col == fk_col:
                pk_col = fk_col  # dim_customer -> customer_key
            
            # Special handling for role-playing dimensions
            if dim_table == "dim_date":
                pk_col = "date_key"
            elif dim_table == "dim_geography":
                pk_col = "geography_key"
            elif dim_table == "dim_shipping":
                pk_col = "shipping_key"
            elif dim_table == "dim_customer":
                pk_col = "customer_key"
            elif dim_table == "dim_product":
                pk_col = "product_key"
            
            sql = f"""
            SELECT COUNT(*) as orphan_count
            FROM dwh.order_items f
            LEFT JOIN dwh.{dim_table} d ON f.{fk_col} = d.{pk_col}
            WHERE f.{fk_col} IS NOT NULL AND d.{pk_col} IS NULL
            """
            
            result = hook.get_first(sql)
            orphan_count = result[0] if result else 0
            
            if orphan_count > 0:
                results.append(f"FAILED: {fk_col} has {orphan_count} orphan records")
            else:
                results.append(f"PASSED: {fk_col} → {dim_table}")
        
        print("\n=== Referential Integrity Check Results ===")
        for r in results:
            print(r)
        
        # Fail if any orphans found
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"Referential integrity violations: {len(failures)} checks failed")
        
        return {"status": "passed", "checks": len(results)}

    @task
    def check_null_values():
        """Check for unexpected NULL values in critical columns"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Define columns that should never be NULL
        critical_columns = {
            "dwh.order_items": ["order_item_key", "sales", "order_item_quantity"],
            "dwh.dim_customer": ["customer_key", "customer_id"],
            "dwh.dim_product": ["product_key", "product_id", "product_name"],
            "dwh.dim_date": ["date_key", "full_date"],
            "dwh.dim_geography": ["geography_key", "city", "country"],
        }
        
        results = []
        for table, columns in critical_columns.items():
            for col in columns:
                sql = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                result = hook.get_first(sql)
                null_count = result[0] if result else 0
                
                if null_count > 0:
                    results.append(f"FAILED: {table}.{col} has {null_count} NULL values")
                else:
                    results.append(f"PASSED: {table}.{col} has no NULLs")
        
        print("\n=== NULL Value Check Results ===")
        for r in results:
            print(r)
        
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"NULL value violations: {len(failures)} checks failed")
        
        return {"status": "passed", "checks": len(results)}

    @task
    def check_uniqueness():
        """Check uniqueness constraints on dimension natural keys"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        unique_checks = [
            ("dwh.dim_customer", "customer_id"),
            ("dwh.dim_product", "product_id"),
            ("dwh.dim_date", "full_date"),
            ("dwh.dim_department", "department_id"),
            ("dwh.dim_category", "category_id"),
        ]
        
        results = []
        for table, column in unique_checks:
            sql = f"""
            SELECT {column}, COUNT(*) as cnt
            FROM {table}
            WHERE {column} IS NOT NULL
            GROUP BY {column}
            HAVING COUNT(*) > 1
            LIMIT 5
            """
            
            duplicates = hook.get_records(sql)
            
            if duplicates:
                results.append(f"FAILED: {table}.{column} has {len(duplicates)} duplicate values")
            else:
                results.append(f"PASSED: {table}.{column} is unique")
        
        print("\n=== Uniqueness Check Results ===")
        for r in results:
            print(r)
        
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"Uniqueness violations: {len(failures)} checks failed")
        
        return {"status": "passed", "checks": len(results)}

    @task
    def check_business_rules():
        """Validate business rules and data consistency"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        results = []
        
        # Rule 1: Sales should be positive
        sql = "SELECT COUNT(*) FROM dwh.order_items WHERE sales < 0"
        negative_sales = hook.get_first(sql)[0]
        if negative_sales > 0:
            results.append(f"WARNING: {negative_sales} records with negative sales")
        else:
            results.append("PASSED: All sales values are non-negative")
        
        # Rule 2: Quantity should be positive
        sql = "SELECT COUNT(*) FROM dwh.order_items WHERE order_item_quantity <= 0"
        invalid_qty = hook.get_first(sql)[0]
        if invalid_qty > 0:
            results.append(f"WARNING: {invalid_qty} records with invalid quantity")
        else:
            results.append("PASSED: All quantities are positive")
        
        # Rule 3: Order date should not be in the future
        sql = "SELECT COUNT(*) FROM dwh.dim_date WHERE full_date > CURRENT_DATE"
        future_dates = hook.get_first(sql)[0]
        if future_dates > 0:
            results.append(f"INFO: {future_dates} future dates in dim_date (may be valid)")
        else:
            results.append("PASSED: No future dates in orders")
        
        # Rule 4: Discount rate should be between 0 and 1
        sql = """
        SELECT COUNT(*) FROM dwh.order_items 
        WHERE order_item_discount_rate < 0 OR order_item_discount_rate > 1
        """
        invalid_discount = hook.get_first(sql)[0]
        if invalid_discount > 0:
            results.append(f"WARNING: {invalid_discount} records with invalid discount rate")
        else:
            results.append("PASSED: All discount rates are valid (0-1)")
        
        print("\n=== Business Rules Check Results ===")
        for r in results:
            print(r)
        
        return {"status": "completed", "results": results}

    @task
    def check_data_completeness():
        """Verify data completeness across all tables"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        tables = [
            "dwh.dim_customer",
            "dwh.dim_product",
            "dwh.dim_category",
            "dwh.dim_department",
            "dwh.dim_date",
            "dwh.dim_geography",
            "dwh.dim_shipping",
            "dwh.order_items",
        ]
        
        results = []
        for table in tables:
            sql = f"SELECT COUNT(*) FROM {table}"
            count = hook.get_first(sql)[0]
            
            if count == 0:
                results.append(f"FAILED: {table} is empty!")
            else:
                results.append(f"PASSED: {table} has {count} records")
        
        print("\n=== Data Completeness Check Results ===")
        for r in results:
            print(r)
        
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"Completeness violations: {len(failures)} tables are empty")
        
        return {"status": "passed", "tables_checked": len(tables)}

    @task
    def generate_quality_report(ri_result, null_result, unique_result, biz_result, complete_result):
        """Generate final data quality report"""
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION REPORT")
        print("=" * 60)
        print(f"\n1. Referential Integrity: {ri_result['status'].upper()}")
        print(f"   - Checks performed: {ri_result['checks']}")
        print(f"\n2. NULL Value Validation: {null_result['status'].upper()}")
        print(f"   - Checks performed: {null_result['checks']}")
        print(f"\n3. Uniqueness Constraints: {unique_result['status'].upper()}")
        print(f"   - Checks performed: {unique_result['checks']}")
        print(f"\n4. Business Rules: {biz_result['status'].upper()}")
        print(f"\n5. Data Completeness: {complete_result['status'].upper()}")
        print(f"   - Tables checked: {complete_result['tables_checked']}")
        print("\n" + "=" * 60)
        print("ALL DATA QUALITY CHECKS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
        return {"overall_status": "passed"}

    # Define task dependencies
    ri_check = check_referential_integrity()
    null_check = check_null_values()
    unique_check = check_uniqueness()
    biz_check = check_business_rules()
    complete_check = check_data_completeness()
    
    # All checks can run in parallel, then generate report
    report = generate_quality_report(
        ri_check, null_check, unique_check, biz_check, complete_check
    )


data_quality_validation_dag()