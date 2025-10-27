-- =====================================================================
-- File: 03_create_dwh_tables.sql
-- Description: Creates all dimension and fact tables for the DWH schema.
--              Assembled from detailed scripts provided by the team.
-- Author: Your AI Assistant (based on team's design)
-- Date: 2025-10-26
-- =====================================================================

-- Switch to the correct schema
SET search_path TO dwh;

-- ---------------------------------------------------------------------
-- SNOWFLAKE HIERARCHY - Must be created in order from Parent to Child
-- ---------------------------------------------------------------------

-- Dim_Department (Root Table)
CREATE TABLE IF NOT EXISTS dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id INT NOT NULL,
    department_name VARCHAR(100),
    updated_date TIMESTAMP
);

-- Dim_Category (Parent Table)
CREATE TABLE IF NOT EXISTS dim_category (
    category_key SERIAL PRIMARY KEY,
    category_id INT NOT NULL,
    category_name VARCHAR(100),
    department_key INT NOT NULL,
    updated_date TIMESTAMP,
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

-- ---------------------------------------------------------------------
-- STAR DIMENSIONS
-- ---------------------------------------------------------------------

-- STAR DIMENSION 1: Dim_Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
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

-- STAR DIMENSION 2: Dim_Date (Role-Playing)
CREATE TABLE IF NOT EXISTS dim_date (
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

-- STAR DIMENSION 3: Dim_Geography (Role-Playing)
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key SERIAL PRIMARY KEY,
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

-- STAR DIMENSION 4: Dim_Shipping (Junk Dimension)
CREATE TABLE IF NOT EXISTS dim_shipping (
    shipping_key SERIAL PRIMARY KEY,
    shipping_mode VARCHAR(50),
    delivery_status VARCHAR(50),
    late_delivery_risk BOOLEAN,
    days_for_shipment_scheduled INT,
    is_express_shipping BOOLEAN,
    is_international BOOLEAN,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SNOWFLAKE CHILD TABLE: Dim_Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
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
    FOREIGN KEY (category_key) REFERENCES dim_category(category_key)
);

-- ---------------------------------------------------------------------
-- FACT TABLE - Must be created LAST
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key BIGSERIAL PRIMARY KEY,
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
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (shipping_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (order_geography_key) REFERENCES dim_geography(geography_key),
    FOREIGN KEY (shipping_key) REFERENCES dim_shipping(shipping_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
);

-- ---------------------------------------------------------------------
-- INDEXES FOR QUERY PERFORMANCE
-- ---------------------------------------------------------------------

-- Indexes for dim_department
CREATE INDEX IF NOT EXISTS idx_dept_id ON dim_department(department_id);
CREATE INDEX IF NOT EXISTS idx_dept_name ON dim_department(department_name);

-- Indexes for dim_category
CREATE INDEX IF NOT EXISTS idx_cat_id ON dim_category(category_id);
CREATE INDEX IF NOT EXISTS idx_cat_dept ON dim_category(department_key);
CREATE INDEX IF NOT EXISTS idx_cat_name ON dim_category(category_name);

-- Indexes for dim_customer
CREATE INDEX IF NOT EXISTS idx_customer_id ON dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_segment ON dim_customer(customer_segment);
CREATE INDEX IF NOT EXISTS idx_customer_current ON dim_customer(is_current);
CREATE INDEX IF NOT EXISTS idx_customer_city ON dim_customer(customer_city);
CREATE INDEX IF NOT EXISTS idx_customer_country ON dim_customer(customer_country);

-- Indexes for dim_date
CREATE INDEX IF NOT EXISTS idx_date_full ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_date_year_month ON dim_date(year, month_number);
CREATE INDEX IF NOT EXISTS idx_date_quarter ON dim_date(year, quarter_number);

-- Indexes for dim_geography
CREATE INDEX IF NOT EXISTS idx_geo_city ON dim_geography(city);
CREATE INDEX IF NOT EXISTS idx_geo_country ON dim_geography(country);
CREATE INDEX IF NOT EXISTS idx_geo_region ON dim_geography(region);
CREATE INDEX IF NOT EXISTS idx_geo_market ON dim_geography(market);
CREATE INDEX IF NOT EXISTS idx_geo_coords ON dim_geography(latitude, longitude);

-- Indexes for dim_shipping
CREATE INDEX IF NOT EXISTS idx_ship_mode ON dim_shipping(shipping_mode);
CREATE INDEX IF NOT EXISTS idx_ship_status ON dim_shipping(delivery_status);
CREATE INDEX IF NOT EXISTS idx_ship_risk ON dim_shipping(late_delivery_risk);

-- Indexes for dim_product
CREATE INDEX IF NOT EXISTS idx_prod_card_id ON dim_product(product_card_id);
CREATE INDEX IF NOT EXISTS idx_prod_category ON dim_product(category_key);
CREATE INDEX IF NOT EXISTS idx_prod_name ON dim_product(product_name);
CREATE INDEX IF NOT EXISTS idx_prod_current ON dim_product(is_current);

-- Indexes for fact_order_items
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_date ON fact_order_items(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipping_date ON fact_order_items(shipping_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_id ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_cust_date ON fact_order_items(customer_key, order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_date ON fact_order_items(product_key, order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_geo_date ON fact_order_items(customer_geography_key, order_date_key);