# load_source_data.py

import pandas as pd
from sqlalchemy import create_engine
import re

print("Script started...")

# --- 1. KẾT NỐI TỚI CÁC DATABASE POSTGRESQL ---
# Sử dụng SQLAlchemy để tạo kết nối.
# Mật khẩu 'mysecretpassword' là mật khẩu bạn đã đặt trong file .env
# Tên database và port phải khớp với file docker-compose.yml

try:
    # Connection string cho OMS DB (Order Management System)
    engine_oms = create_engine('postgresql://user:root@localhost:5433/oms_db')

    # Connection string cho SLMS DB (Shipping & Logistics Management System)
    engine_slms = create_engine('postgresql://user:root@localhost:5434/slms_db')

    print("Successfully created database engines.")

except Exception as e:
    print(f"Error creating database engines: {e}")
    exit()

# --- 2. ĐỌC VÀ LÀM SẠCH DỮ LIỆU TỪ FILE CSV ---

try:
    # Đọc dữ liệu từ file CSV
    df = pd.read_csv('DataCoSupplyChainDataset.csv', encoding='latin1')
    print(f"Successfully loaded CSV file. Shape: {df.shape}")

    # LÀM SẠCH TÊN CỘT: Đây là bước cực kỳ quan trọng.
    # Tên cột gốc có dấu cách, ký tự đặc biệt, sẽ gây lỗi khi làm việc với SQL.
    # Chúng ta sẽ chuyển hết về chữ thường và thay thế ký tự không hợp lệ bằng dấu gạch dưới.
    def clean_col_names(df):
        cols = df.columns
        new_cols = []
        for col in cols:
            new_col = re.sub(r'[^A-Za-z0-9_]+', '', col.lower().strip().replace(' ', '_'))
            new_cols.append(new_col)
        df.columns = new_cols
        return df

    df = clean_col_names(df)
    print("Column names cleaned.")
    # In ra vài tên cột sau khi làm sạch để kiểm tra:
    print("Sample cleaned column names:", df.columns.tolist()[:5])

except FileNotFoundError:
    print("Error: DataCoSupplyChainDataset.csv not found. Make sure it's in the same directory.")
    exit()
except Exception as e:
    print(f"An error occurred during file processing: {e}")
    exit()


# --- 3. PHÂN CHIA DỮ LIỆU CHO 2 SERVICE ---

# Định nghĩa các cột thuộc về mỗi service dựa trên logic kinh doanh
# Lưu ý: 'order_id' và 'order_customer_id' sẽ có ở cả hai để làm khóa liên kết.

oms_columns = [
    'customer_id', 'customer_fname', 'customer_lname', 'customer_segment',
    'customer_city', 'customer_state', 'customer_country', 'customer_street',
    'customer_zipcode', 'customer_email', 'order_id', 'order_date_dateorders',
    'order_status', 'order_city', 'order_state', 'order_country', 'order_zipcode',
    'order_item_id', 'order_item_product_price', 'order_item_discount',
    'order_item_discount_rate', 'order_item_quantity', 'order_item_total',
    'order_item_profit_ratio', 'product_card_id', 'product_name',
    'product_description', 'product_category_id', 'product_price', 'product_status',
    'product_image', 'sales_per_customer', 'order_profit_per_order', 'sales',
    'category_id', 'category_name', 'department_id', 'department_name',
    'market', 'order_region', 'days_for_shipment_scheduled', 'type' # Type là payment method
]

slms_columns = [
    'shipping_mode', 'shipping_date_dateorders', 'days_for_shipping_real',
    'late_delivery_risk', 'delivery_status', 'latitude', 'longitude',
    'customer_city', 'customer_state', 'customer_country', # Dùng cho điểm đến
    'order_id', # Foreign key từ OMS
    'order_customer_id' # Foreign key từ OMS
]

# Đổi tên cột 'order_customer_id' trong df cho nhất quán
if 'order_customer_id' not in df.columns and 'ordercustomerid' in df.columns:
    df.rename(columns={'ordercustomerid': 'order_customer_id'}, inplace=True)


# Tạo 2 DataFrame riêng biệt
df_oms = df[oms_columns]
df_slms = df[slms_columns]

print(f"Created OMS dataframe with shape: {df_oms.shape}")
print(f"Created SLMS dataframe with shape: {df_slms.shape}")


# --- 4. TẢI DỮ LIỆU VÀO DATABASE (OLTP) ---

try:
    # Tải dữ liệu OMS vào bảng 'source_orders' trong database 'oms_db'
    print("Loading data into OMS database...")
    df_oms.to_sql(
        'source_orders',
        engine_oms,
        if_exists='replace', # 'replace' sẽ xóa bảng cũ và tạo lại. Tốt cho lần chạy đầu.
        index=False,
        chunksize=1000 # Ghi dữ liệu theo từng chunk để tối ưu bộ nhớ
    )
    print("✅ OMS data loaded successfully.")

    # Tải dữ liệu SLMS vào bảng 'source_shipping' trong database 'slms_db'
    print("Loading data into SLMS database...")
    df_slms.to_sql(
        'source_shipping',
        engine_slms,
        if_exists='replace',
        index=False,
        chunksize=1000
    )
    print("✅ SLMS data loaded successfully.")

except Exception as e:
    print(f"An error occurred while loading data to the databases: {e}")

print("Script finished.")