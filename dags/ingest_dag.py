import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine import Engine

# ============================================================================
# ================== KHAI BÁO BIẾN CỐ ĐỊNH (CONSTANTS) ======================
# ============================================================================

# Tên connection ID bạn đã tạo trong Airflow UI
DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"

# Đường dẫn data bên trong Airflow container (đã được map từ ./data)
DATA_PATH = "/opt/airflow/data"
# TÊN FILE CSV DUY NHẤT CỦA BẠN (ĐÃ XÁC NHẬN)
SOURCE_CSV_FILE = "DataCoSupplyChainDataset_4k_with_13cols.csv" 

# =LIÊN KẾT GIỮA KAGGLE CSV VÀ STAGING TABLE==================================
# Đây là phần quan trọng nhất: Map tên cột trong file CSV gốc (Kaggle) 
# với tên cột trong bảng Staging (init.sql) của bạn.

# 1. Bảng stg_oms_orders (Mapping này có vẻ đã đúng)
OMS_ORDERS_TABLE = "stg_oms_orders"
OMS_ORDERS_COLS_MAPPING = {
    # Tên cột trong CSV : Tên cột trong Staging
    "Order Id": "order_id",
    "Customer Id": "customer_id",
    "Customer Fname": "customer_fname",
    "Customer Lname": "customer_lname",
    "Customer Segment": "customer_segment",
    "Order City": "order_city",
    "Order State": "order_state",
    "Order Country": "order_country",
    "order date (DateOrders)": "order_date",
    "Order Status": "order_status",
}

# 2. Bảng stg_oms_order_items (*** PHẦN ĐÃ ĐƯỢC SỬA LỖI ***)
OMS_ORDER_ITEMS_TABLE = "stg_oms_order_items"
OMS_ORDER_ITEMS_COLS_MAPPING = {
    # Tên cột trong CSV : Tên cột trong Staging
    "Order Item Id": "order_item_id",
    "Order Id": "order_id",
    
    # === SỬA LỖI 1: "Product Id" không tìm thấy ===
    # Tên cột CSV "Product Card Id" sẽ được map vào cột "product_id" của staging
    "Product Card Id": "product_id", 
    # Tên cột CSV "Order Item Cardprod Id" sẽ được map vào "product_card_id"
    "Order Item Cardprod Id": "product_card_id", 
    # === KẾT THÚC SỬA LỖI 1 ===

    "Product Name": "product_name",
    "Product Price": "product_price",
    "Order Item Quantity": "quantity",
    "Sales": "sales",
    "Order Item Discount": "discount",
    
    # === SỬA LỖI 2: "Order Item Profit" không tìm thấy ===
    # Tên cột CSV "Order Profit Per Order" sẽ được map vào cột "profit"
    "Order Profit Per Order": "profit",
    # === KẾT THÚC SỬA LỖI 2 ===
    
    "Order Item Discount Rate": "discount_rate",
    "Order Item Profit Ratio": "profit_ratio",
    "Category Id": "category_id",
    "Category Name": "category_name",
    "Department Id": "department_id",
    "Department Name": "department_name",
}

# 3. Bảng stg_slms_shipments (Mapping này có vẻ đã đúng)
SLMS_SHIPMENTS_TABLE = "stg_slms_shipments"
SLMS_SHIPMENTS_COLS_MAPPING = {
    # Tên cột trong CSV : Tên cột trong Staging
    "Order Item Id": "shipment_id", # Dùng Order Item Id làm key cho shipment
    "Order Id": "order_id",
    "Shipping Mode": "shipping_mode",
    "shipping date (DateOrders)": "ship_date",
    "Delivery Status": "delivery_status",
    "Late_delivery_risk": "delivery_risk",
    "Order City": "ship_city",      # Dùng 'Order City' cho 'ship_city'
    "Order State": "ship_state",    # Dùng 'Order State' cho 'ship_state'
    "Order Country": "ship_country",  # Dùng 'Order Country' cho 'ship_country'
    "Latitude": "latitude",
    "Longitude": "longitude",
}

# ============================================================================
# ================== ĐỊNH NGHĨA DAG (DAG DEFINITION) ==========================
# ============================================================================

@dag(
    dag_id="ingest_data_dag_single_file_v2", # Đổi tên dag_id để Airflow nhận diện file mới
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Chạy hàng ngày
    catchup=False,
    tags=["elt", "ingest", "dwh_final", "single_file", "v2_fixed"],
)
def ingest_data_dag_single_file_v2():
    """
    DAG này chịu trách nhiệm thu thập (Ingest) dữ liệu từ MỘT file CSV
    duy nhất và "tách" nó ra 3 bảng Staging.
    
    Luồng chạy:
    1. Truncate (dọn dẹp) 3 bảng staging.
    2. Extract: Đọc toàn bộ file CSV vào 1 DataFrame (dùng XCom).
    3. Load (song song): 3 task nhận DataFrame, tự xử lý và load
       vào 3 bảng staging riêng biệt.
    """

    @task
    def truncate_staging_tables():
        """
        Task 1: Dọn dẹp các bảng staging để đảm bảo idempotent.
        """
        print("Đang dọn dẹp (TRUNCATE) các bảng staging...")
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        
        sql_truncate = """
        TRUNCATE TABLE staging.stg_oms_orders;
        TRUNCATE TABLE staging.stg_oms_order_items;
        TRUNCATE TABLE staging.stg_slms_shipments;
        """
        hook.run(sql_truncate)
        print("Đã dọn dẹp xong 3 bảng staging.")

    @task
    def extract_full_csv() -> pd.DataFrame:
        """
        Task 2: Đọc toàn bộ file CSV vào một DataFrame và trả về nó.
        Airflow sẽ tự động chia sẻ DataFrame này cho các task sau qua XCom.
        """
        csv_path = f"{DATA_PATH}/{SOURCE_CSV_FILE}"
        
        # Lấy tất cả các cột cần thiết từ cả 3 mapping
        all_cols_needed = set()
        all_cols_needed.update(OMS_ORDERS_COLS_MAPPING.keys())
        all_cols_needed.update(OMS_ORDER_ITEMS_COLS_MAPPING.keys())
        all_cols_needed.update(SLMS_SHIPMENTS_COLS_MAPPING.keys())
        
        # Lấy các cột ngày
        date_cols = [
            "order date (DateOrders)",
            "shipping date (DateOrders)"
        ]
        
        print(f"Đang đọc file: {csv_path}")
        # Chú ý: Thêm encoding='latin1' vì file này có ký tự đặc biệt
        df = pd.read_csv(
            csv_path,
            usecols=list(all_cols_needed), # Chỉ đọc các cột cần
            parse_dates=date_cols,
            dayfirst=True, # Quan trọng!
            encoding='latin1' 
        )
        print(f"Đã đọc xong {len(df)} dòng từ CSV.")
        return df

    # --- CÁC TASK LOAD SONG SONG ---

    def _get_postgres_engine():
        """Hàm helper nhỏ để lấy engine cho to_sql."""
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        engine: Engine = hook.get_sqlalchemy_engine()
        return engine

    @task
    def load_stg_oms_orders(df: pd.DataFrame):
        """
        Task 3a: Nhận DataFrame, xử lý và load cho stg_oms_orders.
        """
        print("Đang xử lý và load stg_oms_orders...")
        
        # 1. Chọn các cột cần thiết
        df_orders = df[OMS_ORDERS_COLS_MAPPING.keys()].copy()
        
        # 2. Đổi tên cột
        df_orders.rename(columns=OMS_ORDERS_COLS_MAPPING, inplace=True)
        
        # 3. Xử lý logic nghiệp vụ (Rất quan trọng!)
        # Bảng Order là grain 'order_id', ta phải xóa trùng
        df_final = df_orders.drop_duplicates(subset=["order_id"])
        
        # 4. Thêm metadata
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "OMS"
        
        # 5. Load
        df_final.to_sql(
            OMS_ORDERS_TABLE,
            _get_postgres_engine(),
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000 
        )
        print(f"Hoàn thành load {len(df_final)} dòng vào {OMS_ORDERS_TABLE}.")

    @task
    def load_stg_oms_order_items(df: pd.DataFrame):
        """
        Task 3b: Nhận DataFrame, xử lý và load cho stg_oms_order_items.
        """
        print("Đang xử lý và load stg_oms_order_items...")
        
        # 1. Chọn các cột cần thiết
        df_items = df[OMS_ORDER_ITEMS_COLS_MAPPING.keys()].copy()
        
        # 2. Đổi tên cột
        df_items.rename(columns=OMS_ORDER_ITEMS_COLS_MAPPING, inplace=True)
        
        # 3. Xử lý logic nghiệp vụ
        # Grain là 'order_item_id', thường không cần drop_duplicates
        df_final = df_items
        
        # 4. Thêm metadata
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "OMS"
        
        # 5. Load
        df_final.to_sql(
            OMS_ORDER_ITEMS_TABLE,
            _get_postgres_engine(),
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000 
        )
        print(f"Hoàn thành load {len(df_final)} dòng vào {OMS_ORDER_ITEMS_TABLE}.")

    @task
    def load_stg_slms_shipments(df: pd.DataFrame):
        """
        Task 3c: Nhận DataFrame, xử lý và load cho stg_slms_shipments.
        """
        print("Đang xử lý và load stg_slms_shipments...")
        
        # 1. Chọn các cột cần thiết
        df_ship = df[SLMS_SHIPMENTS_COLS_MAPPING.keys()].copy()
        
        # 2. Đổi tên cột
        df_ship.rename(columns=SLMS_SHIPMENTS_COLS_MAPPING, inplace=True)
        
        # 3. Xử lý logic nghiệp vụ
        # Grain là 'shipment_id' (đang dùng 'order_item_id'), không cần drop
        df_final = df_ship
        
        # 4. Thêm metadata
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "SLMS"
        
        # 5. Load
        df_final.to_sql(
            SLMS_SHIPMENTS_TABLE,
            _get_postgres_engine(),
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000 
        )
        print(f"Hoàn thành load {len(df_final)} dòng vào {SLMS_SHIPMENTS_TABLE}.")


    # ============================================================================
    # ================== ĐIỀU PHỐI (ORCHESTRATION) =============================
    # ============================================================================
    
    # 

    # Bước 1: Truncate trước
    truncate_task = truncate_staging_tables()

    # Bước 2: Đọc file CSV duy nhất
    full_df = extract_full_csv()

    # Bước 3: Chỉ khi Truncate xong thì mới Extract
    truncate_task >> full_df

    # Bước 4: Khi Extract xong, chạy 3 task Load song song
    # Chúng ta truyền 'full_df' (XCom) làm tham số cho các task
    full_df >> [
        load_stg_oms_orders(full_df),
        load_stg_oms_order_items(full_df),
        load_stg_slms_shipments(full_df)
    ]

# Khởi tạo DAG
ingest_data_dag_single_file_v2()