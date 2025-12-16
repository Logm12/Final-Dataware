import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine import Engine



DWH_POSTGRES_CONN_ID = "dwh_postgres_conn"

DATA_PATH = "/opt/airflow/data"
SOURCE_CSV_FILE = "DataCoSupplyChainDataset_4k_with_13cols.csv" 

OMS_ORDERS_TABLE = "stg_oms_orders"
OMS_ORDERS_COLS_MAPPING = {
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

OMS_ORDER_ITEMS_TABLE = "stg_oms_order_items"
OMS_ORDER_ITEMS_COLS_MAPPING = {
    # Tên cột trong CSV : Tên cột trong Staging
    "Order Item Id": "order_item_id",
    "Order Id": "order_id",
    

    "Product Card Id": "product_id", 
    "Order Item Cardprod Id": "product_card_id", 

    "Product Name": "product_name",
    "Product Price": "product_price",
    "Order Item Quantity": "quantity",
    "Sales": "sales",
    "Order Item Discount": "discount",
 
    "Order Profit Per Order": "profit",
    
    "Order Item Discount Rate": "discount_rate",
    "Order Item Profit Ratio": "profit_ratio",
    "Category Id": "category_id",
    "Category Name": "category_name",
    "Department Id": "department_id",
    "Department Name": "department_name",
}

SLMS_SHIPMENTS_TABLE = "stg_slms_shipments"
SLMS_SHIPMENTS_COLS_MAPPING = {

    "Order Item Id": "shipment_id", 
    "Order Id": "order_id",
    "Shipping Mode": "shipping_mode",
    "shipping date (DateOrders)": "ship_date",
    "Delivery Status": "delivery_status",
    "Late_delivery_risk": "delivery_risk",
    "Order City": "ship_city",      
    "Order State": "ship_state",    
    "Order Country": "ship_country", 
    "Latitude": "latitude",
    "Longitude": "longitude",
}


@dag(
    dag_id="ingest_data_dag_single_file_v2", 
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Chạy hàng ngày
    catchup=False,
    tags=["elt", "ingest", "dwh_final", "single_file", "v2_fixed"],
)
def ingest_data_dag_single_file_v2():

    @task
    def truncate_staging_tables():

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
        csv_path = f"{DATA_PATH}/{SOURCE_CSV_FILE}"
        
        all_cols_needed = set()
        all_cols_needed.update(OMS_ORDERS_COLS_MAPPING.keys())
        all_cols_needed.update(OMS_ORDER_ITEMS_COLS_MAPPING.keys())
        all_cols_needed.update(SLMS_SHIPMENTS_COLS_MAPPING.keys())
        
        date_cols = [
            "order date (DateOrders)",
            "shipping date (DateOrders)"
        ]
        
        print(f"Đang đọc file: {csv_path}")
        df = pd.read_csv(
            csv_path,
            usecols=list(all_cols_needed), # Chỉ đọc các cột cần
            parse_dates=date_cols,
            dayfirst=True, # Quan trọng!
            encoding='latin1' 
        )
        print(f"Đã đọc xong {len(df)} dòng từ CSV.")
        return df


    def _get_postgres_engine():
        hook = PostgresHook(postgres_conn_id=DWH_POSTGRES_CONN_ID)
        engine: Engine = hook.get_sqlalchemy_engine()
        return engine

    @task
    def load_stg_oms_orders(df: pd.DataFrame):

        print("Đang xử lý và load stg_oms_orders...")
        
        df_orders = df[OMS_ORDERS_COLS_MAPPING.keys()].copy()
        
        df_orders.rename(columns=OMS_ORDERS_COLS_MAPPING, inplace=True)
        
        # Bảng Order là grain 'order_id', ta phải xóa trùng
        df_final = df_orders.drop_duplicates(subset=["order_id"])
        
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "OMS"
        
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
        print("Đang xử lý và load stg_oms_order_items...")
        
        df_items = df[OMS_ORDER_ITEMS_COLS_MAPPING.keys()].copy()
        
        df_items.rename(columns=OMS_ORDER_ITEMS_COLS_MAPPING, inplace=True)
        
        df_final = df_items
        
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "OMS"
        
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

        print("Đang xử lý và load stg_slms_shipments...")
        
        df_ship = df[SLMS_SHIPMENTS_COLS_MAPPING.keys()].copy()
        
        df_ship.rename(columns=SLMS_SHIPMENTS_COLS_MAPPING, inplace=True)
        
        df_final = df_ship
        
        df_final["load_timestamp"] = pd.Timestamp.now()
        df_final["source_system"] = "SLMS"
        
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



    truncate_task = truncate_staging_tables()

    full_df = extract_full_csv()

    truncate_task >> full_df


    full_df >> [
        load_stg_oms_orders(full_df),
        load_stg_oms_order_items(full_df),
        load_stg_slms_shipments(full_df)
    ]


ingest_data_dag_single_file_v2()
