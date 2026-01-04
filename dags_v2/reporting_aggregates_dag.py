"""
reporting_aggregates_dag.py

DAG to build pre-aggregated tables for BI reporting performance.
Creates summary tables for common analytics queries.
Updated for Northwind schema.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"


@dag(
    dag_id="build_reporting_aggregates",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Run after transform DAG
    catchup=False,
    tags=["elt", "reporting", "aggregates", "bi", "northwind"],
    doc_md="""
    # Reporting Aggregates DAG (Northwind)
    
    Builds pre-aggregated tables for faster BI dashboard performance.
    
    ## Aggregates Created:
    1. **Daily Sales Summary**: Sales metrics by date
    2. **Customer Analysis**: Metrics by customer
    3. **Product Performance**: Sales by product/category
    4. **Geographic Analysis**: Regional sales distribution
    5. **Employee Performance**: Sales by employee/sales rep
    6. **KPI Summary**: Executive KPI dashboard
    """,
)
def build_reporting_aggregates():

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
            d.month_name,
            d.day_of_week,
            d.is_weekend,
            COUNT(f.order_item_key) AS total_line_items,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.quantity) AS total_quantity,
            SUM(f.line_total) AS total_sales,
            SUM(f.shipping_fee) AS total_shipping,
            SUM(f.taxes) AS total_taxes,
            AVG(f.line_total) AS avg_line_value,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            COUNT(DISTINCT f.product_key) AS unique_products
        FROM dwh.order_items f
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            d.full_date, d.year, d.quarter_number, 
            d.month_number, d.month_name, d.day_of_week, d.is_weekend
        ORDER BY d.full_date;
        
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
    def build_customer_analysis():
        """Build customer aggregation table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.customer_analysis;
        
        CREATE TABLE reporting.customer_analysis AS
        SELECT 
            c.customer_key,
            c.customer_id,
            c.customer_fname || ' ' || c.customer_lname AS customer_name,
            c.company,
            c.customer_city,
            c.customer_state,
            c.customer_country,
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(f.order_item_key) AS total_line_items,
            SUM(f.quantity) AS total_quantity,
            SUM(f.line_total) AS total_sales,
            AVG(f.line_total) AS avg_order_value,
            MIN(d.full_date) AS first_order_date,
            MAX(d.full_date) AS last_order_date,
            COUNT(DISTINCT d.year || '-' || d.month_number) AS active_months
        FROM dwh.order_items f
        JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            c.customer_key, c.customer_id, c.customer_fname, c.customer_lname,
            c.company, c.customer_city, c.customer_state, c.customer_country
        ORDER BY total_sales DESC;
        
        CREATE INDEX idx_customer_analysis_id 
            ON reporting.customer_analysis(customer_id);
        CREATE INDEX idx_customer_analysis_sales 
            ON reporting.customer_analysis(total_sales DESC);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.customer_analysis")[0]
        print(f"Customer analysis created with {count} rows")
        return {"table": "customer_analysis", "rows": count}

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
            p.product_code,
            p.product_name,
            cat.category_name,
            p.standard_cost,
            p.list_price,
            COUNT(f.order_item_key) AS times_ordered,
            SUM(f.quantity) AS total_quantity_sold,
            SUM(f.line_total) AS total_sales,
            AVG(f.unit_price) AS avg_selling_price,
            AVG(f.discount) AS avg_discount,
            -- Profit margin calculation
            CASE 
                WHEN p.list_price > 0 
                THEN (p.list_price - COALESCE(p.standard_cost, 0)) / p.list_price * 100
                ELSE 0 
            END AS list_margin_pct,
            COUNT(DISTINCT f.customer_key) AS unique_customers
        FROM dwh.order_items f
        JOIN dwh.dim_product p ON f.product_key = p.product_key
        LEFT JOIN dwh.dim_category cat ON p.category_key = cat.category_key
        GROUP BY 
            p.product_key, p.product_id, p.product_code, p.product_name,
            cat.category_name, p.standard_cost, p.list_price
        ORDER BY total_sales DESC;
        
        CREATE INDEX idx_product_perf_sales 
            ON reporting.product_performance(total_sales DESC);
        CREATE INDEX idx_product_perf_category 
            ON reporting.product_performance(category_name);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.product_performance")[0]
        print(f"Product performance table created with {count} rows")
        return {"table": "product_performance", "rows": count}

    @task
    def build_category_analysis():
        """Build category-level analysis"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.category_analysis;
        
        CREATE TABLE reporting.category_analysis AS
        SELECT 
            cat.category_key,
            cat.category_name,
            d.year,
            d.quarter_number,
            COUNT(DISTINCT p.product_id) AS product_count,
            COUNT(f.order_item_key) AS total_line_items,
            SUM(f.quantity) AS total_quantity,
            SUM(f.line_total) AS total_sales,
            AVG(f.line_total) AS avg_line_value,
            COUNT(DISTINCT f.customer_key) AS unique_customers
        FROM dwh.order_items f
        JOIN dwh.dim_product p ON f.product_key = p.product_key
        JOIN dwh.dim_category cat ON p.category_key = cat.category_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY cat.category_key, cat.category_name, d.year, d.quarter_number
        ORDER BY d.year, d.quarter_number, total_sales DESC;
        
        CREATE INDEX idx_category_analysis 
            ON reporting.category_analysis(category_name, year);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.category_analysis")[0]
        print(f"Category analysis table created with {count} rows")
        return {"table": "category_analysis", "rows": count}

    @task
    def build_geographic_analysis():
        """Build geographic sales analysis table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.geographic_analysis;
        
        CREATE TABLE reporting.geographic_analysis AS
        SELECT 
            g.geography_key,
            g.country,
            g.state,
            g.city,
            d.year,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(f.order_item_key) AS total_line_items,
            SUM(f.line_total) AS total_sales,
            SUM(f.shipping_fee) AS total_shipping_fees,
            AVG(f.line_total) AS avg_line_value,
            SUM(f.quantity) AS total_quantity
        FROM dwh.order_items f
        JOIN dwh.dim_geography g ON f.shipping_geography_key = g.geography_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            g.geography_key, g.country, g.state, g.city, d.year
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
    def build_employee_performance():
        """Build employee/sales rep performance table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.employee_performance;
        
        CREATE TABLE reporting.employee_performance AS
        SELECT 
            e.employee_key,
            e.employee_id,
            e.employee_fname || ' ' || e.employee_lname AS employee_name,
            e.job_title,
            d.year,
            d.quarter_number,
            COUNT(DISTINCT f.order_id) AS total_orders_handled,
            COUNT(f.order_item_key) AS total_line_items,
            SUM(f.line_total) AS total_sales,
            SUM(f.quantity) AS total_quantity,
            AVG(f.line_total) AS avg_order_value,
            COUNT(DISTINCT f.customer_key) AS unique_customers_served
        FROM dwh.order_items f
        JOIN dwh.dim_employee e ON f.employee_key = e.employee_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            e.employee_key, e.employee_id, e.employee_fname, e.employee_lname,
            e.job_title, d.year, d.quarter_number
        ORDER BY total_sales DESC;
        
        CREATE INDEX idx_employee_perf_year 
            ON reporting.employee_performance(year, quarter_number);
        CREATE INDEX idx_employee_perf_sales 
            ON reporting.employee_performance(total_sales DESC);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.employee_performance")[0]
        print(f"Employee performance table created with {count} rows")
        return {"table": "employee_performance", "rows": count}

    @task
    def build_shipper_performance():
        """Build shipper performance table"""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql = """
        DROP TABLE IF EXISTS reporting.shipper_performance;
        
        CREATE TABLE reporting.shipper_performance AS
        SELECT 
            sh.shipper_key,
            sh.shipper_id,
            sh.shipper_name,
            d.year,
            d.quarter_number,
            COUNT(DISTINCT f.order_id) AS total_shipments,
            SUM(f.shipping_fee) AS total_shipping_revenue,
            AVG(f.shipping_fee) AS avg_shipping_fee,
            SUM(f.line_total) AS total_order_value,
            COUNT(DISTINCT f.customer_key) AS unique_customers
        FROM dwh.order_items f
        JOIN dwh.dim_shipper sh ON f.shipper_key = sh.shipper_key
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY 
            sh.shipper_key, sh.shipper_id, sh.shipper_name,
            d.year, d.quarter_number
        ORDER BY total_shipments DESC;
        
        CREATE INDEX idx_shipper_perf_year 
            ON reporting.shipper_performance(year, quarter_number);
        """
        
        hook.run(sql)
        
        count = hook.get_first("SELECT COUNT(*) FROM reporting.shipper_performance")[0]
        print(f"Shipper performance table created with {count} rows")
        return {"table": "shipper_performance", "rows": count}

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
            d.month_name,
            
            -- Revenue KPIs
            SUM(f.line_total) AS total_revenue,
            SUM(f.shipping_fee) AS total_shipping_fees,
            SUM(f.taxes) AS total_taxes,
            
            -- Volume KPIs
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(f.order_item_key) AS total_line_items,
            SUM(f.quantity) AS total_units_sold,
            
            -- Customer KPIs
            COUNT(DISTINCT f.customer_key) AS active_customers,
            SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.customer_key), 0) AS revenue_per_customer,
            
            -- Product KPIs
            COUNT(DISTINCT f.product_key) AS active_products,
            
            -- Efficiency KPIs
            AVG(f.discount) * 100 AS avg_discount_pct,
            AVG(f.unit_price) AS avg_unit_price
            
        FROM dwh.order_items f
        JOIN dwh.dim_date d ON f.order_date_key = d.date_key
        GROUP BY d.year, d.quarter_number, d.month_number, d.month_name
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
    customer = build_customer_analysis()
    product = build_product_performance()
    category = build_category_analysis()
    geo = build_geographic_analysis()
    employee = build_employee_performance()
    shipper = build_shipper_performance()
    kpi = build_kpi_summary()
    
    # Schema must exist before building aggregates
    schema >> [daily, customer, product, category, geo, employee, shipper, kpi]
    
    # Generate final report
    report = generate_aggregate_report([daily, customer, product, category, geo, employee, shipper, kpi])
    
    [daily, customer, product, category, geo, employee, shipper, kpi] >> report


build_reporting_aggregates()