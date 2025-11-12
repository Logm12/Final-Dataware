---

# 🚀 DWH Demo — ETL & Aggregate Performance Showcase

**Mục tiêu:**
Chạy ETL từ file CSV mới `DataCoSupplyChainDataset_6k_with_13cols.csv`, tạo dimensions + fact + aggregates, validate kết quả và demo hiệu năng giữa **fact table** và **aggregate tables**.

---

## ✅ Thứ tự chạy (Fast Run — 10~15 phút)

1. Mở **PowerShell**, chuyển vào thư mục `demos`
2. Kiểm tra Docker & PostgreSQL container
3. Kiểm tra database & schemas nhanh
4. Xác nhận file CSV mới tồn tại
5. Chạy ETL (full pipeline)
6. (Nếu cần) Tạo aggregates bằng SQL trực tiếp
7. Validate kết quả (row counts)
8. Demo performance (EXPLAIN ANALYZE fact vs aggregate)
9. (Tùy chọn) Kết nối Power BI hoặc show sample data

---

## ⚙️ PowerShell Commands (Copy & Paste)

### 0️⃣ Bắt đầu ở workspace demos

```powershell
cd "demos"
```

---

### 1️⃣ Kiểm tra Docker container đang chạy

```powershell
docker ps --filter "name=jovial_mayer"
```

# Có thể thay tên container nếu muốn

---

### 2️⃣ Kiểm tra database `dwh_demo` và schemas/tables nhanh

```powershell
docker exec jovial_mayer psql -U root -d dwh_demo -c "\dt dwh.*"
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('dwh','staging') ORDER BY table_schema, table_name LIMIT 50;"
```

---

### 3️⃣ Xác nhận file CSV (mới) tồn tại

```powershell
Test-Path "D:\1st (24-25)\DW\DataCoSupplyChainDataset_6k_with_13cols.csv"

# Xem header (nếu cần)
python -c "import pandas as pd; print(pd.read_csv(r'D:/1st (24-25)/DW/DataCoSupplyChainDataset_6k_with_13cols.csv', nrows=1).columns.tolist())"
```

---

### 4️⃣ Chạy ETL (full pipeline)

```powershell
cd "D:\1st (24-25)\DW\MiAI_Airflow\demos\etl"
python hybrid_aggregate_builder.py
```

> 💡 Output sẽ log progress:
> `populate dim_date` → `populate dimensions` → `populate_fact_table` → `create_aggregates`.

Nếu script báo lỗi → dừng lại và xem log (để debug nhanh).

---

### 5️⃣ (Nếu ETL không tạo aggregates) Chạy SQL file trực tiếp bằng psql trong container

```powershell
Get-Content "D:\1st (24-25)\DW\MiAI_Airflow\demos\sql\02_create_aggregates.sql" | docker exec -i jovial_mayer psql -U root -d dwh_demo
```

---

### 6️⃣ Validate nhanh (row counts)

```powershell
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT 'dim_date' as tbl, COUNT(*) FROM dwh.dim_date UNION ALL SELECT 'dim_product', COUNT(*) FROM dwh.dim_product UNION ALL SELECT 'dim_customer', COUNT(*) FROM dwh.dim_customer UNION ALL SELECT 'dim_category', COUNT(*) FROM dwh.dim_category UNION ALL SELECT 'order_items', COUNT(*) FROM dwh.order_items;"

# Hoặc chạy helper script
cd "D:\1st (24-25)\DW\MiAI_Airflow\demos"
.\quick_validate.ps1
```

📊 **Kết quả mong đợi:**

| Table                  | Expected Rows |
| :--------------------- | ------------: |
| `order_items`          |          5999 |
| `dim_date`             |          4018 |
| `agg_sales_daily`      |           501 |
| `agg_sales_monthly`    |            95 |
| `agg_top_customers`    |           194 |
| `agg_top_products`     |           323 |
| `agg_category_summary` |            89 |

---

### 7️⃣ Demo Performance — EXPLAIN ANALYZE

#### Từ Fact Table (với JOINs)

```powershell
docker exec jovial_mayer psql -U root -d dwh_demo -c "EXPLAIN ANALYZE SELECT cat.category_name, SUM(oi.sales) AS total FROM dwh.order_items oi JOIN dwh.dim_product p ON oi.product_key = p.product_key JOIN dwh.dim_category cat ON p.category_key = cat.category_key JOIN dwh.dim_date d ON oi.order_date_key = d.date_key WHERE d.year BETWEEN 2015 AND 2018 GROUP BY cat.category_name ORDER BY total DESC LIMIT 5;" 2>&1 | Select-String "Execution Time"
```

#### Từ Aggregate Table (không JOINs)

```powershell
docker exec jovial_mayer psql -U root -d dwh_demo -c "EXPLAIN ANALYZE SELECT category_name, SUM(total_sales) AS total FROM dwh.agg_sales_monthly_category WHERE year BETWEEN 2015 AND 2018 GROUP BY category_name ORDER BY total DESC LIMIT 5;" 2>&1 | Select-String "Execution Time"
```

> 📈 **Tính speedup:** > `speedup = time_fact / time_agg`

---

### 8️⃣ (Tùy chọn) Show sample rows từ aggregate

```powershell
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT year, month_number, category_name, total_sales::NUMERIC(12,2), total_quantity, unique_customers FROM dwh.agg_sales_monthly_category ORDER BY total_sales DESC LIMIT 10;"
```
