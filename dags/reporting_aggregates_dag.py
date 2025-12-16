"""
reporting_aggregates_dag.py

DAG to build pre-aggregated tables for BI reporting performance.
Creates materialized views and summary tables for common analytics queries.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="reporting_aggregates_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Run after transform DAG
    catchup=False,
    tags=["elt", "reporting", "aggregates", "bi"],
    doc_md="""
    # Reporting Aggregates DAG
    
    Builds pre-aggregated tables for faster BI dashboard performance.
    
    ## Aggregates Created:
    1. **Daily Sales Summary**: Sales metrics by date
    2. **Customer Segment Analysis**: Metrics by customer segment
    3. **Product Performance**: Sales and profit by product/category
    4. **Geographic Analysis**: Regional sales distribution
    5. **Shipping Performance**: Delivery metrics by shipping mode
    """
)
def reporting_aggregates_dag():

    @task
    def create_reporting_schema():
        """Create reporting schema if not exists"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        hook.run("CREATE SCHEMA IF NOT EXISTS reporting")
        print("Reporting schema created/verified")
        return {"status": "ready"}

    @task
    def build_daily_sales_summary():
        """Build daily sales aggregation table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.daily_sales_summary;
        
        CREATE TABLE reporting.daily_sales_summary AS
        SELECT 
            d.full_date,
            d.year,
            d.quarter_number,
            d.month_number,
            d.day_of_week,
            d.is_weekend,
            COUNT(DISTINCT f.order_item_key) AS total_orders,
            SUM(f.order_item_quantity) AS total_quantity,
            SUM(f.sales) AS total_sales,
            SUM(f.order_item_profit) AS total_profit,
            SUM(f.order_item_discount) AS total_discount,
            AVG(f.sales) AS avg_order_value,
            AVG(f.order_item_profit_ratio) AS avg_profit_ratio,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            COUNT(DISTINCT f.product_key) AS unique_products
        FROM dwh.order_items f
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            d.full_date, d.year, d.quarter_number, 
            d.month_number, d.day_of_week, d.is_weekend
        ORDER BY d.full_date;
        
        -- Create index for date-based queries
        CREATE INDEX idx_daily_sales_date 
            ON reporting.daily_sales_summary(full_date);
        CREATE INDEX idx_daily_sales_year_month 
            ON reporting.daily_sales_summary(year, month_number);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.daily_sales_summary")[0]
        print(f"Daily sales summary created with {count} rows")
        return {"table": "daily_sales_summary", "rows": count}

    @task
    def build_customer_segment_analysis():
        """Build customer segment aggregation table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.customer_segment_analysis;
        
        CREATE TABLE reporting.customer_segment_analysis AS
        SELECT 
            c.customer_segment,
            d.year,
            d.quarter_number,
            COUNT(DISTINCT c.customer_key) AS total_customers,
            COUNT(f.order_item_key) AS total_orders,
            SUM(f.sales) AS total_sales,
            SUM(f.order_item_profit) AS total_profit,
            AVG(f.sales) AS avg_order_value,
            SUM(f.sales) / COUNT(DISTINCT c.customer_key) AS revenue_per_customer,
            AVG(f.order_item_discount_rate) AS avg_discount_rate
        FROM dwh.order_items f
        JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY c.customer_segment, d.year, d.quarter_number
        ORDER BY d.year, d.quarter_number, total_sales DESC;
        
        CREATE INDEX idx_segment_analysis 
            ON reporting.customer_segment_analysis(customer_segment, year);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.customer_segment_analysis")[0]
        print(f"Customer segment analysis created with {count} rows")
        return {"table": "customer_segment_analysis", "rows": count}

    @task
    def build_product_performance():
        """Build product performance aggregation table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.product_performance;
        
        CREATE TABLE reporting.product_performance AS
        SELECT 
            p.product_key,
            p.product_id,
            p.product_name,
            cat.category_name,
            dept.department_name,
            COUNT(f.order_item_key) AS times_ordered,
            SUM(f.order_item_quantity) AS total_quantity_sold,
            SUM(f.sales) AS total_sales,
            SUM(f.order_item_profit) AS total_profit,
            AVG(f.order_item_profit_ratio) AS avg_profit_ratio,
            AVG(f.order_item_discount_rate) AS avg_discount_rate,
            p.product_price AS unit_price,
            CASE 
                WHEN SUM(f.sales) > 0 THEN SUM(f.order_item_profit) / SUM(f.sales) * 100
                ELSE 0 
            END AS profit_margin_pct
        FROM dwh.order_items f
        JOIN dwh.dim_product p ON f.product_key = p.product_key
        LEFT JOIN dwh.dim_category cat ON p.category_key = cat.category_key
        LEFT JOIN dwh.dim_department dept ON cat.department_key = dept.department_key
        GROUP BY 
            p.product_key, p.product_id, p.product_name, 
            cat.category_name, dept.department_name, p.product_price
        ORDER BY total_sales DESC;
        
        CREATE INDEX idx_product_perf_sales 
            ON reporting.product_performance(total_sales DESC);
        CREATE INDEX idx_product_perf_dept 
            ON reporting.product_performance(department_name, category_name);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.product_performance")[0]
        print(f"Product performance table created with {count} rows")
        return {"table": "product_performance", "rows": count}

    @task
    def build_geographic_analysis():
        """Build geographic sales analysis table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.geographic_analysis;
        
        CREATE TABLE reporting.geographic_analysis AS
        SELECT 
            g.country,
            g.state,
            g.city,
            g.latitude,
            g.longitude,
            d.year,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            COUNT(f.order_item_key) AS total_orders,
            SUM(f.sales) AS total_sales,
            SUM(f.order_item_profit) AS total_profit,
            AVG(f.sales) AS avg_order_value,
            SUM(f.order_item_quantity) AS total_quantity
        FROM dwh.order_items f
        JOIN dwh.dim_geography g ON f.order_geography_key = g.geography_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            g.country, g.state, g.city, 
            g.latitude, g.longitude, d.year
        ORDER BY total_sales DESC;
        
        CREATE INDEX idx_geo_analysis_country 
            ON reporting.geographic_analysis(country, year);
        CREATE INDEX idx_geo_analysis_state 
            ON reporting.geographic_analysis(state, year);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.geographic_analysis")[0]
        print(f"Geographic analysis table created with {count} rows")
        return {"table": "geographic_analysis", "rows": count}

    @task
    def build_shipping_performance():
        """Build shipping performance analysis table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.shipping_performance;
        
        CREATE TABLE reporting.shipping_performance AS
        SELECT 
            s.shipping_mode,
            s.delivery_status,
            s.delivery_risk,
            d.year,
            d.quarter_number,
            COUNT(f.order_item_key) AS total_shipments,
            SUM(f.sales) AS total_sales,
            AVG(f.sales) AS avg_order_value,
            COUNT(CASE WHEN s.delivery_status = 'Late delivery' THEN 1 END) AS late_deliveries,
            COUNT(CASE WHEN s.delivery_status = 'Advance shipping' THEN 1 END) AS early_deliveries,
            COUNT(CASE WHEN s.delivery_status = 'Shipping on time' THEN 1 END) AS on_time_deliveries,
            CASE 
                WHEN COUNT(f.order_item_key) > 0 
                THEN COUNT(CASE WHEN s.delivery_status = 'Late delivery' THEN 1 END) * 100.0 / COUNT(f.order_item_key)
                ELSE 0 
            END AS late_delivery_pct
        FROM dwh.order_items f
        JOIN dwh.dim_shipping s ON f.shipping_key = s.shipping_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            s.shipping_mode, s.delivery_status, s.delivery_risk,
            d.year, d.quarter_number
        ORDER BY d.year, d.quarter_number, total_shipments DESC;
        
        CREATE INDEX idx_shipping_perf_mode 
            ON reporting.shipping_performance(shipping_mode, year);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.shipping_performance")[0]
        print(f"Shipping performance table created with {count} rows")
        return {"table": "shipping_performance", "rows": count}

    @task
    def build_kpi_summary():
        """Build executive KPI summary table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.kpi_summary;
        
        CREATE TABLE reporting.kpi_summary AS
        SELECT 
            d.year,
            d.quarter_number,
            d.month_number,
            -- Revenue KPIs
            SUM(f.sales) AS total_revenue,
            SUM(f.order_item_profit) AS total_profit,
            SUM(f.order_item_discount) AS total_discounts,
            
            -- Volume KPIs
            COUNT(f.order_item_key) AS total_orders,
            SUM(f.order_item_quantity) AS total_units_sold,
            
            -- Customer KPIs
            COUNT(DISTINCT f.customer_key) AS active_customers,
            SUM(f.sales) / NULLIF(COUNT(DISTINCT f.customer_key), 0) AS revenue_per_customer,
            
            -- Product KPIs
            COUNT(DISTINCT f.product_key) AS active_products,
            
            -- Efficiency KPIs
            CASE 
                WHEN SUM(f.sales) > 0 
                THEN SUM(f.order_item_profit) / SUM(f.sales) * 100 
                ELSE 0 
            END AS profit_margin_pct,
            AVG(f.order_item_discount_rate) * 100 AS avg_discount_pct
            
        FROM dwh.order_items f
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY d.year, d.quarter_number, d.month_number
        ORDER BY d.year, d.quarter_number, d.month_number;
        
        CREATE INDEX idx_kpi_summary_period 
            ON reporting.kpi_summary(year, quarter_number, month_number);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.kpi_summary")[0]
        print(f"KPI summary table created with {count} rows")
        return {"table": "kpi_summary", "rows": count}

    @task
    def generate_aggregate_report(results):
        """Generate summary report of all aggregates built"""
        print("\n" + "=" * 60)
        print("REPORTING AGGREGATES BUILD SUMMARY")
        print("=" * 60)
        
        total_rows = 0
        for result in results:
            if result and "rows" in result:
                print(f"  {result['table']}: {result['rows']:,} rows")
                total_rows += result['rows']
        
        print("-" * 60)
        print(f"  TOTAL: {total_rows:,} aggregate rows created")
        print("=" * 60)
        
        return {"status": "completed", "total_rows": total_rows}

    # Define task dependencies
    schema = create_reporting_schema()
    
    # Build all aggregates in parallel
    daily = build_daily_sales_summary()
    customer = build_customer_segment_analysis()
    product = build_product_performance()
    geo = build_geographic_analysis()
    shipping = build_shipping_performance()
    kpi = build_kpi_summary()
    
    # Schema must exist before building aggregates
    schema >> [daily, customer, product, geo, shipping, kpi]
    
    # Generate final report
    report = generate_aggregate_report([daily, customer, product, geo, shipping, kpi])
    
    [daily, customer, product, geo, shipping, kpi] >> report


reporting_aggregates_dag()