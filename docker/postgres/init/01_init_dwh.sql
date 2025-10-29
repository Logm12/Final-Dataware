-- === TẠO SCHEMA ===
-- OLTP Schemas
CREATE SCHEMA IF NOT EXISTS oms_oltp;  -- Order Management System
CREATE SCHEMA IF NOT EXISTS slms_oltp; -- Shipping & Logistics Management System

-- Data Warehouse Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- === TẠO BẢNG STAGING (ĐỂ TĂNG TỐC TRANSFORM) ===
-- Các bảng staging này là bản sao 1:1 của OLTP
-- Chúng ta thêm INDEX vào các cột JOIN để tăng tốc query fact

CREATE TABLE staging.departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100)
);

CREATE TABLE staging.categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100),
    department_id INT
);
-- Index cho join
CREATE INDEX idx_stg_cat_dept_id ON staging.categories(department_id);


CREATE TABLE staging.products (
    product_card_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2),
    product_status INT,
    product_image VARCHAR(255),
    product_description TEXT,
    category_id INT
);
-- Index cho join
CREATE INDEX idx_stg_prod_cat_id ON staging.products(category_id);


CREATE TABLE staging.customers (
    customer_id INT PRIMARY KEY,
    customer_fname VARCHAR(100),
    customer_lname VARCHAR(100),
    customer_email VARCHAR(100),
    customer_segment VARCHAR(100),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_country VARCHAR(100),
    customer_street VARCHAR(255),
    customer_zipcode VARCHAR(20),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6)
);
-- Index cho join địa lý (dùng 3 cột)
CREATE INDEX idx_stg_cust_geo ON staging.customers(customer_city, customer_state, customer_country);


CREATE TABLE staging.orders (
    order_id INT PRIMARY KEY,
    order_customer_id INT,
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    order_city VARCHAR(100),
    order_state VARCHAR(100),
    order_country VARCHAR(100),
    order_zipcode VARCHAR(20),
    order_region VARCHAR(50),
    order_market VARCHAR(50),
    payment_type VARCHAR(50)
);
-- Index cho join
CREATE INDEX idx_stg_ord_cust_id ON staging.orders(order_customer_id);
CREATE INDEX idx_stg_ord_date ON staging.orders(order_date);
-- Index cho join địa lý (dùng 5 cột)
CREATE INDEX idx_stg_ord_geo ON staging.orders(order_city, order_state, order_country, order_region, order_market);


CREATE TABLE staging.order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    order_item_cardprod_id INT,
    order_item_quantity INT,
    order_item_product_price DECIMAL(10, 2),
    order_item_discount DECIMAL(10, 2),
    order_item_discount_rate DECIMAL(5, 2),
    order_item_total DECIMAL(10, 2),
    sales DECIMAL(10, 2),
    order_profit_per_order DECIMAL(10, 2),
    order_item_profit_ratio DECIMAL(5, 2)
);
-- Index cho join
CREATE INDEX idx_stg_oi_order_id ON staging.order_items(order_id);
CREATE INDEX idx_stg_oi_prod_id ON staging.order_items(order_item_cardprod_id);


CREATE TABLE staging.shipping (
    shipping_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    shipping_date TIMESTAMP,
    shipping_mode VARCHAR(50),
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    delivery_status VARCHAR(50),
    late_delivery_risk INT
);
-- Index cho join
CREATE INDEX idx_stg_ship_order_id ON staging.shipping(order_id);
CREATE INDEX idx_stg_ship_date ON staging.shipping(shipping_date);
-- Index cho join dim_shipping (dùng 3 cột)
CREATE INDEX idx_stg_ship_combo ON staging.shipping(shipping_mode, delivery_status, late_delivery_risk);
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
