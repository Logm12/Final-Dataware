import pendulum
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ===============================
# 0) POOL & UTILS
# ===============================
POOL_NAME = "postgres_transform_pool"  # tạo pool trong Airflow UI (ví dụ size=4)

def build_upsert_sql(
    target_table: str,
    target_cols: list[str],
    conflict_cols: list[str],
    src_select_sql: str,
    update_cols: list[str] | None = None,
) -> str:
    """
    Tạo khung SQL UPSERT chuẩn (INSERT ... ON CONFLICT DO UPDATE ...).
    - target_table: 'schema.table'
    - target_cols:  danh sách cột đích theo thứ tự
    - conflict_cols: cột/nhóm cột UNIQUE/PK để ON CONFLICT
    - src_select_sql: câu SELECT trả về đúng số cột theo target_cols
    - update_cols:  cột sẽ được cập nhật khi xung đột; mặc định = (target_cols - conflict_cols)
    """
    if update_cols is None:
        update_cols = [c for c in target_cols if c not in conflict_cols]

    target_cols_csv = ", ".join(target_cols)
    conflict_cols_csv = ", ".join(conflict_cols)

    # SET col = EXCLUDED.col cho các cột cần update
    set_clause = ",\n        ".join(f'{c} = EXCLUDED.{c}' for c in update_cols) or "NOTHING"

    return f"""
    INSERT INTO {target_table} (
        {target_cols_csv}
    )
    {src_select_sql}
    ON CONFLICT ({conflict_cols_csv}) DO UPDATE SET
        {set_clause};
    """.strip()


def build_merge_sql_pg15(
    target_table: str,
    alias_t: str,
    src_subquery_sql: str,
    alias_s: str,
    match_cols: list[str],
    update_map: dict[str, str] | None,
    insert_cols: list[str],
) -> str:
    """
    Tùy chọn: Khung MERGE cho PostgreSQL 15+ (nếu môi trường hỗ trợ).
    - match_cols: ['key1','key2',...] điều kiện ON (s.key1=t.key1 AND ...)
    - update_map: { 'col_in_target': 's.expr_or_col', ... } cặp cập nhật
    - insert_cols: cột sẽ insert; giá trị insert mặc định 's.col' theo tên cột
    """
    on_cond = " AND ".join([f"{alias_s}.{c} = {alias_t}.{c}" for c in match_cols])

    update_sql = ""
    if update_map:
        set_pairs = ",\n        ".join([f"{k} = {v}" for k, v in update_map.items()])
        update_sql = f"WHEN MATCHED THEN\n    UPDATE SET\n        {set_pairs}\n"

    insert_cols_csv = ", ".join(insert_cols)
    insert_vals_csv = ", ".join([f"{alias_s}.{c}" for c in insert_cols])

    return f"""
    MERGE INTO {target_table} AS {alias_t}
    USING (
        {src_subquery_sql}
    ) AS {alias_s}
    ON ({on_cond})
    {update_sql}WHEN NOT MATCHED THEN
        INSERT ({insert_cols_csv})
        VALUES ({insert_vals_csv});
    """.strip()


# ===============================
# 1) PREPARE: INDEXES & ANALYZE
# ===============================
SQL_PREPARE_INDEXES = """
-- STAGING
CREATE INDEX IF NOT EXISTS idx_stg_orders_order_id          ON staging.stg_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id       ON staging.stg_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_id     ON staging.stg_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_product_card ON staging.stg_order_items(product_card_id);
CREATE INDEX IF NOT EXISTS idx_stg_shipping_order_id        ON staging.stg_shipping(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_shipping_key_parts
  ON staging.stg_shipping (COALESCE(shipping_mode,'NA'), COALESCE(delivery_status,'NA'), COALESCE(late_delivery_risk::text,'NA'));
CREATE INDEX IF NOT EXISTS idx_stg_products_category_id     ON staging.stg_products(category_id);
CREATE INDEX IF NOT EXISTS idx_stg_categories_department_id ON staging.stg_categories(department_id);

-- DWH (lookup & group-by thường gặp)
CREATE INDEX IF NOT EXISTS idx_dim_category_department_key ON dwh.dim_category(department_key);
CREATE INDEX IF NOT EXISTS idx_dim_product_category_key    ON dwh.dim_product(category_key);

CREATE INDEX IF NOT EXISTS idx_fact_customer_key           ON dwh.fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_key            ON dwh.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_date_key         ON dwh.fact_order_items(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipping_date_key      ON dwh.fact_order_items(shipping_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipping_key           ON dwh.fact_order_items(shipping_key);
"""

SQL_ANALYZE_STAGING = """
ANALYZE staging.stg_orders;
ANALYZE staging.stg_order_items;
ANALYZE staging.stg_shipping;
ANALYZE staging.stg_products;
ANALYZE staging.stg_categories;
ANALYZE staging.stg_customers;
ANALYZE staging.stg_departments;
"""


# ===============================
# 2) UPSERT SQL TỪ KHUNG CHUNG
# ===============================
# 2.1 Department (UPSERT)
SQL_TRANSFORM_DIM_DEPARTMENT = build_upsert_sql(
    target_table="dwh.dim_department",
    target_cols=["department_key", "department_id", "department_name"],
    conflict_cols=["department_key"],
    src_select_sql="""
        SELECT
            department_id AS department_key,
            department_id AS department_id,
            department_name
        FROM staging.stg_departments
    """,
    update_cols=["department_name"],  # chỉ cần update tên
)

# 2.2 Category (UPSERT, phụ thuộc Department)
SQL_TRANSFORM_DIM_CATEGORY = build_upsert_sql(
    target_table="dwh.dim_category",
    target_cols=["category_key", "category_id", "category_name", "department_key"],
    conflict_cols=["category_key"],
    src_select_sql="""
        SELECT
            c.category_id   AS category_key,
            c.category_id   AS category_id,
            c.category_name,
            c.department_id AS department_key
        FROM staging.stg_categories c
    """,
    update_cols=["category_name", "department_key"],
)

# 2.3 Product (UPSERT, phụ thuộc Category)
SQL_TRANSFORM_DIM_PRODUCT = build_upsert_sql(
    target_table="dwh.dim_product",
    target_cols=["product_key", "product_card_id", "product_name", "product_price", "category_key"],
    conflict_cols=["product_key"],
    src_select_sql="""
        SELECT
            p.product_card_id AS product_key,
            p.product_card_id AS product_card_id,
            p.product_name,
            p.product_price,
            p.category_id     AS category_key
        FROM staging.stg_products p
    """,
    update_cols=["product_name", "product_price", "category_key"],
)

# 2.4 Customer (UPSERT)
SQL_TRANSFORM_DIM_CUSTOMER = build_upsert_sql(
    target_table="dwh.dim_customer",
    target_cols=["customer_key", "customer_id", "customer_fname", "customer_lname", "customer_segment"],
    conflict_cols=["customer_key"],
    src_select_sql="""
        SELECT
            customer_id      AS customer_key,
            customer_id      AS customer_id,
            customer_fname,
            customer_lname,
            customer_segment
        FROM staging.stg_customers
    """,
    update_cols=["customer_fname", "customer_lname", "customer_segment"],
)

# 2.5 Shipping (Junk Dim) — key tổng hợp -> nếu trùng thì bỏ qua
SQL_TRANSFORM_DIM_SHIPPING = """
    INSERT INTO dwh.dim_shipping (
        shipping_key, shipping_mode, delivery_status, late_delivery_risk
    )
    SELECT DISTINCT
        (
            COALESCE(shipping_mode, 'NA') || '-' ||
            COALESCE(delivery_status, 'NA') || '-' ||
            COALESCE(late_delivery_risk::TEXT, 'NA')
        ) AS shipping_key,
        shipping_mode,
        delivery_status,
        late_delivery_risk
    FROM staging.stg_shipping
    ON CONFLICT (shipping_key) DO NOTHING;
"""

# 2.6 FACT (giữ nguyên truncate+insert; tối ưu bằng temp)
SQL_TRANSFORM_FACT_ORDER_ITEMS = """
    TRUNCATE TABLE dwh.fact_order_items;

    CREATE TEMP TABLE tmp_fact_src AS
    SELECT
        oi.order_item_id AS order_item_key,
        o.customer_id    AS customer_key,
        oi.product_card_id AS product_key,
        TO_CHAR(o.order_date, 'YYYYMMDD')::INT     AS order_date_key,
        TO_CHAR(s.shipping_date, 'YYYYMMDD')::INT  AS shipping_date_key,
        (
            COALESCE(s.shipping_mode, 'NA') || '-' ||
            COALESCE(s.delivery_status, 'NA') || '-' ||
            COALESCE(s.late_delivery_risk::TEXT, 'NA')
        ) AS shipping_key,
        o.order_id,
        oi.order_item_id,
        oi.order_item_quantity,
        oi.sales,
        oi.order_item_discount,
        oi.order_item_profit_ratio,
        oi.order_profit_per_order,
        s.days_for_shipping_real,
        s.late_delivery_risk
    FROM staging.stg_order_items oi
    LEFT JOIN staging.stg_orders   o ON oi.order_id = o.order_id
    LEFT JOIN staging.stg_shipping s ON oi.order_id = s.order_id;

    ANALYZE tmp_fact_src;

    INSERT INTO dwh.fact_order_items (
        order_item_key,
        customer_key,
        product_key,
        order_date_key,
        shipping_date_key,
        shipping_key,
        order_id,
        order_item_id,
        order_item_quantity,
        sales,
        order_item_discount,
        order_item_profit_ratio,
        order_profit_per_order,
        days_for_shipping_real,
        late_delivery_risk
    )
    SELECT
        order_item_key,
        customer_key,
        product_key,
        order_date_key,
        shipping_date_key,
        shipping_key,
        order_id,
        order_item_id,
        order_item_quantity,
        sales,
        order_item_discount,
        order_item_profit_ratio,
        order_profit_per_order,
        days_for_shipping_real,
        late_delivery_risk
    FROM tmp_fact_src;

    DROP TABLE IF EXISTS tmp_fact_src;
"""


# ===============================
# 3) DAG
# ===============================
@dag(
    dag_id="transform_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["project", "transform", "elt"]
)
def transform_dag():
    """
    DAG Transform từ Staging vào DWH
    - Có KHUNG UPSERT dựng sẵn (build_upsert_sql / build_merge_sql_pg15)
    - Dims: UPSERT
    - Fact: TRUNCATE + INSERT (tối ưu temp)
    - Chuẩn bị index + analyze trước khi chạy
    """

    prepare_indexes = PostgresOperator(
        task_id="prepare_indexes",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_PREPARE_INDEXES,
        autocommit=True,
        pool=POOL_NAME,
        priority_weight=100
    )

    analyze_staging = PostgresOperator(
        task_id="analyze_staging",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_ANALYZE_STAGING,
        autocommit=True,
        pool=POOL_NAME,
        priority_weight=90
    )

    # DIMs
    transform_dim_department = PostgresOperator(
        task_id="transform_dim_department",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_DEPARTMENT,
        pool=POOL_NAME
    )
    transform_dim_customer = PostgresOperator(
        task_id="transform_dim_customer",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_CUSTOMER,
        pool=POOL_NAME
    )
    transform_dim_shipping = PostgresOperator(
        task_id="transform_dim_shipping",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_SHIPPING,
        pool=POOL_NAME
    )
    transform_dim_category = PostgresOperator(
        task_id="transform_dim_category",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_CATEGORY,
        pool=POOL_NAME
    )
    transform_dim_product = PostgresOperator(
        task_id="transform_dim_product",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_DIM_PRODUCT,
        pool=POOL_NAME
    )

    # FACT
    transform_fact_order_items = PostgresOperator(
        task_id="transform_fact_order_items",
        postgres_conn_id="dwh_postgres_conn",
        sql=SQL_TRANSFORM_FACT_ORDER_ITEMS,
        pool=POOL_NAME,
        priority_weight=120
    )

    # Dependencies
    prepare_indexes >> analyze_staging

    analyze_staging >> transform_dim_department >> transform_dim_category >> transform_dim_product
    analyze_staging >> [transform_dim_customer, transform_dim_shipping]

    [transform_dim_product, transform_dim_customer, transform_dim_shipping] >> transform_fact_order_items

transform_dag()
