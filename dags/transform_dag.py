import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# BỎ import ExternalTaskSensor

# ============================================================================
# ================== KHAI BÁO BIẾN CỐ ĐỊNH (CONSTANTS) ======================
# ============================================================================

# Tên connection ID bạn đã tạo trong Airflow UI
DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"

# Đường dẫn đến thư mục chứa các file SQL (bên trong container)
SQL_TRANSFORM_PATH = "/opt/airflow/dags/sql/transform"

# ============================================================================
# ================== HÀM HỖ TRỢ (HELPER FUNCTION) ===========================
# ============================================================================

def _run_sql_script(script_name: str):
    """
    Hàm helper để đọc file .sql và thực thi nó bằng PostgresHook.
    Đây là cách làm "tối ưu tốc độ" nhất.
    """
    hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
    file_path = f"{SQL_TRANSFORM_PATH}/{script_name}"
    
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
        
        print(f"--- Đang thực thi script: {script_name} ---")
        hook.run(sql_script)
        print(f"--- Thực thi {script_name} thành công. ---")
        
    except FileNotFoundError:
        print(f"LỖI: Không tìm thấy file {file_path}")
        raise
    except Exception as e:
        print(f"LỖI khi chạy {script_name}: {e}")
        raise

# ============================================================================
# ================== ĐỊNH NGHĨA DAG (DAG DEFINITION) ==========================
# ============================================================================

@dag(
    dag_id="transform_dwh_dag_v3_manual", # Đã đổi tên 
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Vẫn giữ lịch, bạn có thể TẮT (pause) DAG này
    catchup=False,
    tags=["elt", "transform", "dwh_final", "v3_manual"],
)
def transform_dwh_dag_v3_manual():
    """
    DAG này chịu trách nhiệm Transform dữ liệu từ Staging -> DWH.
    
    *** CẢNH BÁO: DAG NÀY KHÔNG CÓ SENSOR. ***
    Nó GIẢ ĐỊNH rằng ingest_dag đã chạy xong.
    Chỉ chạy thủ công khi bạn chắc chắn ingest đã hoàn thành.
    
    Luồng chạy:
    1. Truncate (dọn dẹp) toàn bộ bảng DWH.
    2. Build song song các Dimension độc lập (Customer, Geo, Date, Ship, Dept).
    3. Build các Dimension phụ thuộc (Category -> Product).
    4. Build bảng Fact (SAU KHI tất cả Dim đã xong).
    """

    # === TASK 1: ĐÃ XÓA SENSOR ===
    
    # Task 2 (nay là task đầu tiên): Dọn dẹp DWH
    @task
    def truncate_dwh_tables():
        _run_sql_script("truncate_dwh_tables.sql")

    # Task 3: Build các Dims song song (Nhóm A)
    @task
    def build_dim_customer():
        _run_sql_script("build_dim_customer.sql")

    @task
    def build_dim_geography():
        _run_sql_script("build_dim_geography.sql")

    @task
    def build_dim_date():
        _run_sql_script("build_dim_date.sql")

    @task
    def build_dim_shipping():
        _run_sql_script("build_dim_shipping.sql")

    # Task 4: Build chuỗi Snowflake (Dept -> Cat -> Prod)
    @task
    def build_dim_department():
        _run_sql_script("build_dim_department.sql")

    @task
    def build_dim_category():
        _run_sql_script("build_dim_category.sql")
        
    @task
    def build_dim_product():
        _run_sql_script("build_dim_product.sql")

    # Task 5: Build Fact (Chạy cuối cùng)
    @task
    def build_fact_order_items():
        _run_sql_script("build_fact_order_items.sql")

    # ============================================================================
    # ================== ĐIỀU PHỐI (ORCHESTRATION) =============================
    # ============================================================================
    
    # Bước 1: Dọn dẹp
    truncate_task = truncate_dwh_tables()
    
    # Bước 2: Build các Dims song song
    dept_task = build_dim_department()
    
    parallel_dims = [
        build_dim_customer(),
        build_dim_geography(),
        build_dim_date(),
        build_dim_shipping(),
    ]
    
    # Bước 3: Build chuỗi Snowflake
    cat_task = build_dim_category()
    prod_task = build_dim_product()

    # Bước 4: Build Fact
    fact_task = build_fact_order_items()

    # == THIẾT LẬP LUỒNG CHẠY (KHÔNG CÒN wait_for_ingest) ==
    
    # Truncate xong -> Mới bắt đầu build các Dims
    truncate_task >> dept_task
    truncate_task >> parallel_dims
    
    # Luồng Snowflake: Dept -> Cat -> Prod
    dept_task >> cat_task >> prod_task
    
    # "Barrier" (Rào cản): Bảng Fact phải chờ TẤT CẢ các Dims hoàn thành
    [prod_task] + parallel_dims >> fact_task

# Khởi tạo DAG
transform_dwh_dag_v3_manual()