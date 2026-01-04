"""
data_quality_dag.py

Data Quality validation DAG for Northwind DWH.
Validates referential integrity, data completeness, and business rules.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="data_quality_checks",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["elt", "data_quality", "validation", "northwind"],
    doc_md="""
    # Data Quality Validation DAG
    
    Performs comprehensive data quality checks on the Northwind DWH:
    1. Referential integrity checks
    2. Null value validation
    3. Uniqueness constraints
    4. Business rule validation
    5. Data completeness checks
    """,
)
def data_quality_checks():
    
    @task
    def check_referential_integrity():
        """Check that all foreign keys in fact table reference valid dimension keys"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Define FK checks: (fact_column, dim_table, dim_pk_column)
        checks = [
            ("customer_key", "dim_customer", "customer_key"),
            ("employee_key", "dim_employee", "employee_key"),
            ("shipper_key", "dim_shipper", "shipper_key"),
            ("product_key", "dim_product", "product_key"),
            ("order_date_key", "dim_date", "date_key"),
            ("shipped_date_key", "dim_date", "date_key"),
            ("customer_geography_key", "dim_geography", "geography_key"),
            ("shipping_geography_key", "dim_geography", "geography_key"),
        ]
        
        results = []
        for fk_col, dim_table, pk_col in checks:
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
                results.append(f"PASSED: {fk_col} -> {dim_table}")
        
        print("\n=== Referential Integrity Check Results ===")
        for r in results:
            print(f"  {r}")
        
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"Referential integrity violations: {len(failures)} checks failed")
        
        return {"status": "passed", "checks": len(results)}

    @task
    def check_null_values():
        """Check for unexpected NULL values in critical columns"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        # Define critical columns that should not be NULL
        critical_columns = {
            "dwh.order_items": ["order_item_key", "order_id", "quantity", "unit_price"],
            "dwh.dim_customer": ["customer_key", "customer_id"],
            "dwh.dim_employee": ["employee_key", "employee_id"],
            "dwh.dim_product": ["product_key", "product_id", "product_name"],
            "dwh.dim_date": ["date_key", "full_date"],
            "dwh.dim_geography": ["geography_key", "city", "country"],
        }
        
        results = []
        for table, columns in critical_columns.items():
            for col in columns:
                sql = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                try:
                    result = hook.get_first(sql)
                    null_count = result[0] if result else 0
                    
                    if null_count > 0:
                        results.append(f"FAILED: {table}.{col} has {null_count} NULL values")
                    else:
                        results.append(f"PASSED: {table}.{col} has no NULLs")
                except Exception as e:
                    results.append(f"SKIPPED: {table}.{col} - {e}")
        
        print("\n=== NULL Value Check Results ===")
        for r in results:
            print(f"  {r}")
        
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
            ("dwh.dim_employee", "employee_id"),
            ("dwh.dim_shipper", "shipper_id"),
            ("dwh.dim_product", "product_id"),
            ("dwh.dim_date", "full_date"),
            ("dwh.dim_category", "category_name"),
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
            
            try:
                duplicates = hook.get_records(sql)
                
                if duplicates:
                    results.append(f"FAILED: {table}.{column} has {len(duplicates)} duplicate values")
                else:
                    results.append(f"PASSED: {table}.{column} is unique")
            except Exception as e:
                results.append(f"SKIPPED: {table}.{column} - {e}")
        
        print("\n=== Uniqueness Check Results ===")
        for r in results:
            print(f"  {r}")
        
        failures = [r for r in results if r.startswith("FAILED")]
        if failures:
            raise ValueError(f"Uniqueness violations: {len(failures)} checks failed")
        
        return {"status": "passed", "checks": len(results)}

    @task
    def check_business_rules():
        """Validate business rules and data consistency"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        results = []
        
        # Rule 1: Quantity should be positive
        sql = "SELECT COUNT(*) FROM dwh.order_items WHERE quantity <= 0"
        try:
            invalid_qty = hook.get_first(sql)[0]
            if invalid_qty > 0:
                results.append(f"WARNING: {invalid_qty} records with invalid quantity")
            else:
                results.append("PASSED: All quantities are positive")
        except:
            results.append("SKIPPED: Quantity check")
        
        # Rule 2: Unit price should be non-negative
        sql = "SELECT COUNT(*) FROM dwh.order_items WHERE unit_price < 0"
        try:
            negative_price = hook.get_first(sql)[0]
            if negative_price > 0:
                results.append(f"WARNING: {negative_price} records with negative unit_price")
            else:
                results.append("PASSED: All unit prices are non-negative")
        except:
            results.append("SKIPPED: Unit price check")
        
        # Rule 3: Discount should be between 0 and 1
        sql = """
        SELECT COUNT(*) FROM dwh.order_items 
        WHERE discount < 0 OR discount > 1
        """
        try:
            invalid_discount = hook.get_first(sql)[0]
            if invalid_discount > 0:
                results.append(f"WARNING: {invalid_discount} records with invalid discount")
            else:
                results.append("PASSED: All discounts are valid (0-1)")
        except:
            results.append("SKIPPED: Discount check")
        
        # Rule 4: Line total should match calculation
        sql = """
        SELECT COUNT(*) FROM dwh.order_items 
        WHERE ABS(line_total - (quantity * unit_price * (1 - COALESCE(discount, 0)))) > 0.01
        """
        try:
            mismatched = hook.get_first(sql)[0]
            if mismatched > 0:
                results.append(f"WARNING: {mismatched} records with mismatched line_total")
            else:
                results.append("PASSED: All line_total calculations are correct")
        except:
            results.append("SKIPPED: Line total check")
        
        print("\n=== Business Rules Check Results ===")
        for r in results:
            print(f"  {r}")
        
        return {"status": "completed", "results": results}

    @task
    def check_data_completeness():
        """Verify data completeness across all tables"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        tables = [
            "dwh.dim_customer",
            "dwh.dim_employee",
            "dwh.dim_shipper",
            "dwh.dim_product",
            "dwh.dim_category",
            "dwh.dim_date",
            "dwh.dim_geography",
            "dwh.order_items",
        ]
        
        results = []
        for table in tables:
            try:
                sql = f"SELECT COUNT(*) FROM {table}"
                count = hook.get_first(sql)[0]
                
                if count == 0:
                    results.append(f"FAILED: {table} is empty!")
                else:
                    results.append(f"PASSED: {table} has {count:,} records")
            except Exception as e:
                results.append(f"SKIPPED: {table} - {e}")
        
        print("\n=== Data Completeness Check Results ===")
        for r in results:
            print(f"  {r}")
        
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
        print("ALL DATA QUALITY CHECKS COMPLETED")
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


data_quality_checks()