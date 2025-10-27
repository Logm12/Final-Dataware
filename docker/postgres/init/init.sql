/*
================================================================
-- 1. TẠO CÁC SCHEMAS CHÍNH
-- Tạo 4 schemas (khu vực) chúng ta cần
================================================================
*/
CREATE SCHEMA IF NOT EXISTS oms_oltp;    -- Giả lập service 1 (sẽ để trống)
CREATE SCHEMA IF NOT EXISTS slms_oltp;   -- Giả lập service 2 (sẽ để trống)
CREATE SCHEMA IF NOT EXISTS staging;   -- Nơi chứa dữ liệu thô (raw)
CREATE SCHEMA IF NOT EXISTS dwh;       -- Nơi chứa Data Warehouse (mô hình Star/Snowflake)

/*
================================================================
-- 2. TẠO BẢNG STAGING
-- Các bảng này dùng để "hứng" dữ liệu thô từ file CSV.
-- Chúng ta dùng kiểu TEXT cho mọi cột để đảm bảo ingest không bị lỗi.
-- DAG_INGEST sẽ chịu trách nhiệm load data vào đây.
================================================================
*/

-- Bảng này mô phỏng dữ liệu từ Order Management System (OMS)
CREATE TABLE IF NOT EXISTS staging.oms_service (
    "Type" TEXT,
    "Days for shipment (scheduled)" TEXT,
    "Benefit per order" TEXT,
    "Sales per customer" TEXT,
    "Category Id" TEXT,
    "Category Name" TEXT,
    "Customer City" TEXT,
    "Customer Country" TEXT,
    "Customer Email" TEXT,
    "Customer Fname" TEXT,
    "Customer Id" TEXT,
    "Customer Lname" TEXT,
    "Customer Password" TEXT,
    "Customer Segment" TEXT,
    "Customer State" TEXT,
    "Customer Street" TEXT,
    "Customer Zipcode" TEXT,
    "Department Id" TEXT,
    "Department Name" TEXT,
    "Market" TEXT,
    "Order City" TEXT,
    "Order Country" TEXT,
    "Order Customer Id" TEXT,
    "order date (DateOrders)" TEXT,
    "Order Id" TEXT,
    "Order Item Cardprod Id" TEXT,
    "Order Item Discount" TEXT,
    "Order Item Discount Rate" TEXT,
    "Order Item Id" TEXT,
    "Order Item Product Price" TEXT,
    "Order Item Profit Ratio" TEXT,
    "Order Item Quantity" TEXT,
    "Sales" TEXT,
    "Order Item Total" TEXT,
    "Order Profit Per Order" TEXT,
    "Order Region" TEXT,
    "Order State" TEXT,
    "Order Status" TEXT,
    "Order Zipcode" TEXT,
    "Product Card Id" TEXT,
    "Product Category Id" TEXT,
    "Product Description" TEXT,
    "Product Image" TEXT,
    "Product Name" TEXT,
    "Product Price" TEXT,
    "Product Status" TEXT
);

-- Bảng này mô phỏng dữ liệu từ Shipping & Logistics Management System (SLMS)
CREATE TABLE IF NOT EXISTS staging.slms_service (
    "Order Id" TEXT, -- Khóa liên kết
    "Days for shipping (real)" TEXT,
    "Delivery Status" TEXT,
    "Late_delivery_risk" TEXT,
    "Latitude" TEXT,
    "Longitude" TEXT,
    "shipping date (DateOrders)" TEXT,
    "Shipping Mode" TEXT
);

/*
================================================================
-- 3. TẠO CÁC BẢNG DATA WAREHOUSE (DWH)
-- Dựa trên thiết kế Hybrid Schema (Star + Snowflake) từ file PDF.
-- Thứ tự tạo rất quan trọng (do có Foreign Keys).
================================================================
*/

-- Kích hoạt extension nếu cần (cho các hàm xử lý)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- STAR DIM 2: Dim_Date (Role-Playing)
-- (Tạo trước vì nó không phụ thuộc)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    iso_week INT,
    month_number INT,
    month_name VARCHAR(10),
    month_abbr VARCHAR(3),
    quarter_number INT,
    quarter_name VARCHAR(2),
    year INT,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(50),
    is_working_day BOOLEAN,
    retail_week INT,
    retail_month INT,
    retail_year INT
);

-- STAR DIM 3: Dim_Geography (Role-Playing)
CREATE TABLE IF NOT EXISTS dwh.dim_geography (
    geography_key INT PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    zipcode VARCHAR(20),
    region VARCHAR(50),
    market VARCHAR(50),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    time_zone VARCHAR(50),
    country_code VARCHAR(3),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STAR DIM 4: Dim_Shipping (Junk Dimension)
CREATE TABLE IF NOT EXISTS dwh.dim_shipping (
    shipping_key INT PRIMARY KEY,
    shipping_mode VARCHAR(50),
    delivery_status VARCHAR(50),
    late_delivery_risk BOOLEAN,
    days_for_shipment_scheduled INT,
    is_express_shipping BOOLEAN,
    is_international BOOLEAN,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STAR DIM 1: Dim_Customer
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT NOT NULL,
    customer_fname VARCHAR(50),
    customer_lname VARCHAR(50),
    customer_email VARCHAR(100),
    customer_segment VARCHAR(20),
    customer_street VARCHAR(200),
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    customer_zipcode VARCHAR(20),
    customer_country VARCHAR(50),
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP
);

-- SNOWFLAKE: Dim_Department (Root Table)
-- (Tạo trước Category và Product)
CREATE TABLE IF NOT EXISTS dwh.dim_department (
    department_key INT PRIMARY KEY,
    department_id INT NOT NULL,
    department_name VARCHAR(100),
    updated_date TIMESTAMP
);

-- SNOWFLAKE: Dim_Category (Parent Table)
-- (Tạo sau Department, trước Product)
CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_key INT PRIMARY KEY,
    category_id INT NOT NULL,
    category_name VARCHAR(100),
    department_key INT NOT NULL,
    updated_date TIMESTAMP,
    FOREIGN KEY (department_key) REFERENCES dwh.dim_department(department_key)
);

-- SNOWFLAKE: Dim_Product (Child Table)
-- (Tạo sau Category)
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key INT PRIMARY KEY,
    product_card_id INT NOT NULL,
    product_name VARCHAR(200),
    product_description TEXT,
    product_image VARCHAR(500),
    product_price DECIMAL(10,2),
    product_status INT,
    category_key INT NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP,
    FOREIGN KEY (category_key) REFERENCES dwh.dim_category(category_key)
);

-- FACT TABLE: Fact_Order_Items
-- (Tạo cuối cùng, sau khi tất cả các Dim đã được tạo)
CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_item_key BIGINT PRIMARY KEY,
    customer_key INT NOT NULL,
    order_date_key INT NOT NULL,
    shipping_date_key INT NULL,
    customer_geography_key INT NOT NULL,
    order_geography_key INT NOT NULL,
    shipping_key INT NOT NULL,
    product_key INT NOT NULL,
    order_id VARCHAR(20) NOT NULL,
    order_item_id VARCHAR(20) NOT NULL,
    order_status VARCHAR(20),
    payment_type VARCHAR(20),
    order_item_quantity INT NOT NULL,
    sales DECIMAL(10,2) NOT NULL,
    order_item_product_price DECIMAL(10,2) NOT NULL,
    order_item_discount DECIMAL(10,2) NOT NULL,
    order_profit_per_order DECIMAL(10,2) NOT NULL,
    order_item_total DECIMAL(10,2) NOT NULL,
    order_item_discount_rate DECIMAL(5,4),
    order_item_profit_ratio DECIMAL(5,4),
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    late_delivery_risk BOOLEAN,
    etl_insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_update_date TIMESTAMP,
    source_system VARCHAR(50),
    
    -- Định nghĩa Foreign Keys
    FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    FOREIGN KEY (order_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (shipping_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (customer_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (order_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (shipping_key) REFERENCES dwh.dim_shipping(shipping_key),
    FOREIGN KEY (product_key) REFERENCES dwh.dim_product(product_key)
);