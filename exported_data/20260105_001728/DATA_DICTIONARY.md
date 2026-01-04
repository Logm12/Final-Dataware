# Data Dictionary - Data Warehouse Export
Export Date: 2026-01-05 00:17:28

## Exported Tables

- **dim_customer.csv**: Dimension: Customer information
- **dim_date.csv**: Dimension: Date information
- **dim_employee.csv**: Dimension: Employee information
- **dim_product.csv**: Dimension: Product information
- **fact_sales.csv**: Fact: Sales transactions
- **rpt_sales_overview.csv**: Report: Sales overview dashboard
- **rpt_customer_analytics.csv**: Report: Customer analytics dashboard
- **rpt_product_performance.csv**: Report: Product performance dashboard


## Usage Instructions

1. **Import vào Tableau**:
   - File → Connect to Data → Text file
   - Chọn file CSV cần import
   - Thiết lập relationships giữa các bảng

2. **Import vào Power BI**:
   - Get Data → Text/CSV
   - Chọn file CSV
   - Transform data nếu cần
   - Tạo relationships trong Model view

3. **Import vào Google Data Studio**:
   - Create → Data source → File Upload
   - Upload file CSV
   - Tạo report từ data source

## Table Relationships

### Fact Sales (fact_sales)
- customer_id → dim_customer.customer_id
- product_id → dim_product.product_id
- employee_id → dim_employee.employee_id
- date_key → dim_date.date_key

### Fact Purchase Order (fact_purchase_order)
- product_id → dim_product.product_id
- employee_id → dim_employee.employee_id
- date_key → dim_date.date_key

### Fact Inventory (fact_inventory)
- product_id → dim_product.product_id
- date_key → dim_date.date_key

## Key Metrics

### Sales Metrics
- Total Revenue: SUM(fact_sales.total_amount)
- Total Orders: COUNT(DISTINCT fact_sales.order_id)
- Average Order Value: AVG(fact_sales.total_amount)
- Total Quantity: SUM(fact_sales.quantity)

### Customer Metrics
- Total Customers: COUNT(DISTINCT dim_customer.customer_id)
- Active Customers: COUNT(DISTINCT fact_sales.customer_id)
- Customer Lifetime Value: SUM(fact_sales.total_amount) per customer

### Product Metrics
- Total Products: COUNT(DISTINCT dim_product.product_id)
- Top Products: ORDER BY SUM(fact_sales.total_amount) DESC
- Product Category Performance: GROUP BY dim_product.category

## Notes
- All dates are in YYYY-MM-DD format
- Currency values are in USD
- NULL values may exist in optional fields
