import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

# SQL cho Dim (Dùng UPSERT để đảm bảo Idempotence)
# Dùng ON CONFLICT DO UPDATE để cập nhật nếu đã tồn tại, INSERT nếu chưa có.

# Lưu ý: Các file SQL này nên được đặt trong thư mục /include/sql
# nhưng để đơn giản cho project, chúng ta viết trực tiếp ở đây.

SQL_TRANSFORM_DIM_DEPARTMENT = """
    INSERT INTO dwh.dim_department (
        department_key, department_id, department_name
    )
    SELECT
        department_id, -- Dùng natural key làm surrogate key luôn cho đơn giản
        department_id,
        department_name
    FROM staging.stg_departments
    ON CONFLICT (department_key) DO UPDATE SET
        department_name = EXCLUDED.department_name;
"""

SQL_TRANSFORM_DIM_CATEGORY = """
    INSERT INTO dwh.dim_category (
        category_key, category_id, category_name, department_key
    )
    SELECT
        c.category_id, -- Dùng natural key
        c.category_id,
        c.category_name,
        c.department_id -- Dùng natural key của parent
    FROM staging.stg_categories c
    ON CONFLICT (category_key) DO UPDATE SET
        category_name = EXCLUDED.category_name,
        department_key = EXCLUDED.department_key;
"""

SQL_TRANSFORM_DIM_PRODUCT = """
    INSERT INTO dwh.dim_product (
        product_key, product_card_id, product_name, product_price, category_key
        -- Bỏ qua SCD Type 2 (is_current, effective_date) cho project này
    )
    SELECT
        p.product_card_id, -- Dùng natural key
        p.product_card_id,
        p.product_name,
        p.product_price,
        p.category_id -- Dùng natural key của parent
    FROM staging.stg_products p
    ON CONFLICT (product_key) DO UPDATE SET
        product_name = EXCLUDED.product_name,
        product_price = EXCLUDED.product_price,
        category_key = EXCLUDED.category_key;
"""

SQL_TRANSFORM_DIM_CUSTOMER = """
    INSERT INTO dwh.dim_customer (
        customer_key, customer_id, customer_fname, customer_lname, customer_segment
        -- Bỏ qua SCD Type 2 cho project này
    )
    SELECT
        customer_id, -- Dùng natural key
        customer_id,
        customer_fname,
        customer_lname,
        customer_segment
    FROM staging.stg_customers
    ON CONFLICT (customer_key) DO UPDATE SET
        customer_fname = EXCLUDED.customer_fname,
        customer_lname = EXCLUDED.customer_lname,
        customer_segment = EXCLUDED.customer_segment;
"""

# Bảng Date và Geography cần script riêng (hoặc load từ file CSV)
# Ở đây ta giả định đã có data (hoặc tạo 1 task riêng để chạy 1 lần)

# Bảng Fact (Dùng TRUNCATE + INSERT)
SQL_TRANSFORM_FACT_ORDER_ITEMS = """
    TRUNCATE TABLE dwh.fact_order_items;
    INSERT INTO dwh.fact_order_items (
        order_item_key,
        -- Dimension Foreign Keys
        customer_key,
        product_key,
        order_date_key,
        shipping_date_key,
        shipping_key,
        -- Degenerate Dimensions
        order_id,
        order_item_id,
        -- Facts
        order_item_quantity,
        sales,
        order_item_discount,
        order_item_profit_ratio,
        order_profit_per_order,
        days_for_shipping_real,
        late_delivery_risk
    )
    SELECT
        oi.order_item_id AS order_item_key, -- Dùng natural key
        
        -- Foreign Keys (dùng natural key từ bảng staging)
        o.customer_id AS customer_key,
        oi.product_card_id AS product_key,
        TO_CHAR(o.order_date, 'YYYYMMDD')::INT AS order_date_key,
        TO_CHAR(s.shipping_date, 'YYYYMMDD')::INT AS shipping_date_key,
        
        -- Logic tạo FK cho Junk Dim (Dim_Shipping)
        -- Ta tạo 1 key_id_bằng cách nối các giá trị lại
        (
            COALESCE(s.shipping_mode, 'NA') || '-' ||
            COALESCE(s.delivery_status, 'NA') || '-' ||
            COALESCE(s.late_delivery_risk::TEXT, 'NA')
        ) AS shipping_key,
        
        -- Degenerate Dimensions
        o.order_id,
        oi.order_item_id,

        -- Facts (đo lường)
        oi.order_item_quantity,
        oi.sales,
        oi.order_item_discount,
        oi.order_item_profit_ratio,
        oi.order_profit_per_order,
        s.days_for_shipping_real,
        s.late_delivery_risk
        
    FROM staging.stg_order_items oi
    LEFT JOIN staging.stg_orders o ON oi.order_id = o.order_id
    LEFT JOIN staging.stg_shipping s ON oi.order_id = s.order_id;
"""

# Logic cho Dim_Shipping (Junk Dim)
SQL_TRANSFORM_DIM_SHIPPING = """
    INSERT INTO dwh.dim_shipping (
        shipping_key, shipping_mode, delivery_status, late_delivery_risk
    )
    SELECT
        DISTINCT
        (
            COALESCE(shipping_mode, 'NA') || '-' ||
            COALESCE(delivery_status, 'NA') || '-' ||
            COALESCE(late_delivery_risk::TEXT, 'NA')
        ) AS shipping_key,
        shipping_mode,
        delivery_status,
        late_delivery_risk
    FROM staging.stg_shipping
    ON CONFLICT (shipping_key) DO NOTHING; -- Nếu key đã tồn tại, bỏ qua
"""

@dag(
    dag_id="transform_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@daily", # Chạy cùng lịch với ingest_dag
    catchup=False,
    tags=["project", "transform", "elt"]
)
def transform_dag():
    """
    ### DAG để Transform dữ liệu từ Staging vào DWH
    DAG này thực hiện bước 'Transform' trong ELT.
    Nó chạy sau khi ingest_dag thành công.
    - Dims được UPSERT.
    - Facts được TRUNCATE và load lại.
    """

    # 1. Transform Dims (chạy song song)
    # Nhóm Dims độc lập
    transform_dim_department = PostgresOperator(
        task_id="transform_dim_department",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_DEPARTMENT
    )
    
    transform_dim_customer = PostgresOperator(
        task_id="transform_dim_customer",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_CUSTOMER
    )
    
    transform_dim_shipping = PostgresOperator(
        task_id="transform_dim_shipping",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_SHIPPING
    )
    
    # Nhóm Dims phụ thuộc (Snowflake)
    transform_dim_category = PostgresOperator(
        task_id="transform_dim_category",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_CATEGORY
    )
    
    transform_dim_product = PostgresOperator(
        task_id="transform_dim_product",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_PRODUCT
    )

    # 2. Transform Fact (chạy cuối cùng)
    transform_fact_order_items = PostgresOperator(
        task_id="transform_fact_order_items",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_FACT_ORDER_ITEMS
    )

    # 3. Định nghĩa thứ tự chạy (Dependencies)
    
    # Các Dims thuộc hệ thống Snowflake phải chạy theo thứ tự
    transform_dim_department >> transform_dim_category >> transform_dim_product
    
    # Bảng Fact phải chờ TẤT CẢ các bảng Dim transform xong
    [
        transform_dim_product, # Task cuối cùng của chuỗi snowflake
        transform_dim_customer,
        transform_dim_shipping
        # Thêm dim_date, dim_geography nếu có
    ] >> transform_fact_order_items

# Gọi hàm dag
transform_dag()