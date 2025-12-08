# 📊 Data Cleansing Guide - Supply Chain Data Warehouse

## 📋 Tổng Quan

Script `data_cleansing.py` thực hiện cleansing toàn diện cho dữ liệu Supply Chain, chuẩn bị dữ liệu sạch để load vào Data Warehouse.

---

## 🔧 Các Bước Cleansing

### **STEP 1: Loading Data**
- Đọc file CSV gốc
- Kiểm tra encoding và format
- Đếm số dòng và cột ban đầu

### **STEP 2: Data Exploration**
- Phân tích cấu trúc dữ liệu
- Xác định missing values
- Kiểm tra data types
- Phát hiện duplicate rows

### **STEP 3: Removing Empty/Unnamed Columns**
- Xóa các cột không có tên
- Xóa các cột hoàn toàn null
- Xóa các cột "Unnamed"

### **STEP 4: Removing Duplicates**
- Loại bỏ các dòng trùng lặp hoàn toàn
- Giữ lại bản ghi đầu tiên

### **STEP 5: Cleaning Customer Data**
- Thay thế "XXXXXXXXX" bằng NULL trong:
  - Customer Email
  - Customer Password
- Chuẩn hóa tên khách hàng (trim spaces)

### **STEP 6: Cleaning Numeric Data**
- Convert các cột số về đúng data type
- Validate giá trị âm trong các cột không được âm:
  - Days for shipping
  - Order Item Quantity
  - Sales
  - Product Price
- Xử lý giá trị invalid

### **STEP 7: Cleaning Date Data**
- Convert sang datetime format
- Validate date ranges
- Xử lý invalid dates
- Cột được xử lý:
  - order date (DateOrders)
  - shipping date (DateOrders)

### **STEP 8: Cleaning Categorical Data**
- Trim whitespace từ tất cả string columns
- Standardize categorical values
- Phân tích distribution của các giá trị

### **STEP 9: Handling Missing Values**
- Báo cáo missing values theo từng cột
- Tính phần trăm missing
- Sẵn sàng cho imputation strategies

### **STEP 10: Adding Data Quality Flags**
Thêm các cột metadata cho Data Warehouse:

- **`data_quality_score`**: Điểm chất lượng (0-100)
  - 100: Hoàn hảo
  - 75: Thiếu 1 trường quan trọng
  - 50: Thiếu 2 trường quan trọng
  - 25: Thiếu 3 trường quan trọng
  - 0: Thiếu 4+ trường quan trọng

- **`cleansing_timestamp`**: Thời điểm cleansing

- **`record_source`**: Nguồn dữ liệu (DataCoSupplyChain)

### **STEP 11: Saving Cleaned Data**
- Lưu file với timestamp
- Format: `*_cleaned_YYYYMMDD_HHMMSS.csv`
- Encoding: UTF-8

---

## 📁 Output Files

### File được tạo ra:
```
DataCoSupplyChainDataset_4k_with_13cols_cleaned_YYYYMMDD_HHMMSS.csv
```

### Cấu trúc file output:
- Tất cả cột gốc (đã được cleansing)
- 3 cột metadata mới:
  - `data_quality_score`
  - `cleansing_timestamp`
  - `record_source`

---

## 🚀 Cách Sử Dụng

### **Cách 1: Chạy trực tiếp**
```bash
python data_cleansing.py
```

### **Cách 2: Import vào script khác**
```python
from data_cleansing import SupplyChainDataCleaner

# Tạo cleaner instance
cleaner = SupplyChainDataCleaner('path/to/your/file.csv')

# Chạy full pipeline
output_file = cleaner.run_full_cleansing()

# Hoặc chạy từng bước
cleaner.load_data()
cleaner.explore_data()
cleaner.remove_duplicates()
# ... các bước khác
```

### **Cách 3: Tùy chỉnh output path**
```python
cleaner = SupplyChainDataCleaner('input.csv')
cleaner.run_full_cleansing(output_file='custom_output.csv')
```

---

## 📊 Data Quality Metrics

### Các trường quan trọng được kiểm tra:
1. **Order Id** - Mã đơn hàng
2. **Customer Id** - Mã khách hàng
3. **Product Name** - Tên sản phẩm
4. **Sales** - Doanh số

### Quality Score Calculation:
- Bắt đầu với 100 điểm
- Trừ 25 điểm cho mỗi trường quan trọng bị thiếu
- Kết quả: Score từ 0-100

---

## ⚙️ Tùy Chỉnh Script

### Thay đổi input file:
Sửa trong hàm `main()`:
```python
input_file = r'd:\VSC_FINAL DWH\your_file.csv'
```

### Thêm custom cleansing rules:
Thêm method mới vào class `SupplyChainDataCleaner`:
```python
def custom_cleaning_step(self):
    """Your custom cleaning logic"""
    print("Running custom step...")
    # Your code here
```

Sau đó thêm vào pipeline trong `run_full_cleansing()`:
```python
self.custom_cleaning_step()
```

### Thay đổi missing value strategy:
Trong method `handle_missing_values()`, thêm:
```python
# Fill numeric với median
numeric_cols = self.df.select_dtypes(include=[np.number]).columns
self.df[numeric_cols] = self.df[numeric_cols].fillna(self.df[numeric_cols].median())

# Fill categorical với mode
cat_cols = self.df.select_dtypes(include=['object']).columns
for col in cat_cols:
    self.df[col].fillna(self.df[col].mode()[0], inplace=True)
```

---

## 🎯 Best Practices cho Data Warehouse

### 1. **Slowly Changing Dimensions (SCD)**
Thêm các cột tracking:
```python
self.df['effective_date'] = datetime.now()
self.df['expiry_date'] = '9999-12-31'
self.df['is_current'] = True
self.df['version'] = 1
```

### 2. **Surrogate Keys**
Tạo surrogate key cho dimension tables:
```python
self.df['customer_sk'] = range(1, len(self.df) + 1)
self.df['product_sk'] = range(1, len(self.df) + 1)
```

### 3. **Data Lineage**
Track nguồn gốc dữ liệu:
```python
self.df['source_system'] = 'DataCoSupplyChain'
self.df['load_date'] = datetime.now()
self.df['loaded_by'] = 'ETL_Process'
```

### 4. **Business Keys**
Giữ nguyên business keys:
- Order Id
- Customer Id
- Product Card Id

---

## 📈 Monitoring & Logging

Script tự động tạo report với:
- Số dòng trước/sau cleansing
- Số duplicates removed
- Số missing values
- Columns dropped
- Quality score distribution

### Lưu log ra file:
```python
import logging

logging.basicConfig(
    filename='cleansing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

---

## 🔍 Validation Checks

### Trước khi load vào DWH, kiểm tra:

1. **Primary Keys không null**
   ```python
   assert self.df['Order Id'].notnull().all()
   ```

2. **Foreign Keys hợp lệ**
   ```python
   assert self.df['Customer Id'].isin(customer_dim['Customer Id']).all()
   ```

3. **Date ranges hợp lệ**
   ```python
   assert (self.df['shipping date'] >= self.df['order date']).all()
   ```

4. **Numeric ranges hợp lệ**
   ```python
   assert (self.df['Sales'] >= 0).all()
   ```

---

## 🛠️ Troubleshooting

### Lỗi thường gặp:

**1. FileNotFoundError**
- Kiểm tra đường dẫn file
- Đảm bảo file tồn tại

**2. UnicodeDecodeError**
- Thử encoding khác: `encoding='latin-1'` hoặc `encoding='cp1252'`

**3. Memory Error**
- Xử lý file lớn theo chunks:
  ```python
  chunk_size = 10000
  for chunk in pd.read_csv(file, chunksize=chunk_size):
      # Process chunk
  ```

**4. Date parsing errors**
- Chỉ định format cụ thể:
  ```python
  pd.to_datetime(df[col], format='%m/%d/%Y %H:%M')
  ```

---

## 📚 Next Steps

Sau khi cleansing, bạn có thể:

1. **Load vào Staging Area**
2. **Transform cho Dimension Tables**
3. **Load vào Fact Tables**
4. **Create Indexes**
5. **Setup Incremental Load**

---


Nếu có vấn đề, kiểm tra:
- Python version >= 3.7
- pandas installed: `pip install pandas`
- numpy installed: `pip install numpy`

