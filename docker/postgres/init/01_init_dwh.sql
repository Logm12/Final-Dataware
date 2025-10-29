-- === TẠO SCHEMA ===
-- OLTP Schemas
CREATE SCHEMA IF NOT EXISTS oms_oltp;  -- Order Management System
CREATE SCHEMA IF NOT EXISTS slms_oltp; -- Shipping & Logistics Management System

-- Data Warehouse Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- === TẠO BẢNG DWH ===

-- Dim_Department (Root của Snowflake)
CREATE TABLE dwh.dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id INT NOT NULL,
    department_name VARCHAR(50),
    
    -- Cần thiết cho UPSERT
    CONSTRAINT uq_dept_id UNIQUE(department_id) 
);

-- Dim_Category (Parent của Product)
CREATE TABLE dwh.dim_category (
    category_key SERIAL PRIMARY KEY,
    category_id INT NOT NULL,
    category_name VARCHAR(50),
    department_key INT,
    
    -- Cần thiết cho UPSERT
    CONSTRAINT uq_cat_id UNIQUE(category_id),
    
    -- SỬA LỖI 1: Thêm FK cho Snowflake
    FOREIGN KEY (department_key) REFERENCES dwh.dim_department(department_key)
);

-- Dim_Product (Child table trong Snowflake)
CREATE TABLE dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    category_key INT,
    product_id INT, -- Từ file của bạn, có thể là mã nội bộ
    product_card_id VARCHAR(20) NOT NULL, -- Natural Key từ source
    product_name VARCHAR(100),
    product_price DECIMAL(10, 2),
    product_image VARCHAR(255),
    
    -- Cần thiết cho UPSERT
    CONSTRAINT uq_prod_card_id UNIQUE(product_card_id),
    
    -- SỬA LỖI 1: Thêm FK cho Snowflake
    FOREIGN KEY (category_key) REFERENCES dwh.dim_category(category_key)
);

-- Dim_Customer (Star Dimension)
CREATE TABLE dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    customer_fname VARCHAR(50),
    customer_lname VARCHAR(50),
    customer_segment VARCHAR(50),
    customer_city VARCHAR(50),
    customer_state VARCHAR(50),
    customer_country VARCHAR(50),

    -- Cần thiết cho UPSERT
    CONSTRAINT uq_cust_id UNIQUE(customer_id)
);

-- Dim_Date (Star, Role-Playing)
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY, -- Dùng YYYYMMDD làm key
    full_date DATE NOT NULL,
    day_of_week INT,
    month_number INT,
    quarter_number INT,
    year INT,
    is_weekend BOOLEAN,

    -- Cần thiết cho UPSERT
    CONSTRAINT uq_full_date UNIQUE(full_date)
);

-- Dim_Geography (Star, Role-Playing)
CREATE TABLE dwh.dim_geography (
    geography_key SERIAL PRIMARY KEY,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    region VARCHAR(50),
    market VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- Cần thiết cho UPSERT (giả định 1 địa điểm là duy nhất)
    CONSTRAINT uq_geo_location UNIQUE(city, state, country, region, market)
);

-- Dim_Shipping (Star, Junk Dimension)
CREATE TABLE dwh.dim_shipping (
    shipping_key SERIAL PRIMARY KEY,
    shipping_mode VARCHAR(50),
    delivery_status VARCHAR(50),
    -- Đổi sang BOOLEAN hoặc INT để khớp với dữ liệu (0 hoặc 1)
    late_delivery_risk BOOLEAN, 
    
    -- Cần thiết cho UPSERT (kết hợp các giá trị là duy nhất)
    CONSTRAINT uq_shipping_combo UNIQUE(shipping_mode, delivery_status, late_delivery_risk)
);

-- Fact_Order_Items (Fact Table)
CREATE TABLE dwh.fact_order_items ( -- Đổi tên thành fact_order_items cho chuẩn Kimball
    order_item_key SERIAL PRIMARY KEY,
    customer_key INT,
    order_date_key INT,
    shipping_date_key INT,
    customer_geography_key INT,
    order_geography_key INT,
    shipping_key INT,
    product_key INT,
    
    -- Degenerate Dimensions (Thêm từ PDF, rất quan trọng)
    order_id VARCHAR(20) NOT NULL,
    order_item_id VARCHAR(20) NOT NULL,

    -- Measures
    order_item_quantity INT,
    sales DECIMAL(10, 2),
    order_item_discount DECIMAL(10, 2),
    order_profit_per_order DECIMAL(10, 2), -- Đổi tên từ file SQL của bạn
    order_item_discount_rate DECIMAL(5, 2),
    order_item_profit_ratio DECIMAL(5, 2),
    
    -- Foreign Keys to Dims
    FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    FOREIGN KEY (order_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (shipping_date_key) REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (customer_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (order_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (shipping_key) REFERENCES dwh.dim_shipping(shipping_key),
    FOREIGN KEY (product_key) REFERENCES dwh.dim_product(product_key)
);
