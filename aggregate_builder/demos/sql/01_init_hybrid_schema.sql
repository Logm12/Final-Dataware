-- ========================================
-- AGGREGATE BUILDER DEMO
-- Hybrid Schema: oms_oltp, slms_oltp, staging, dwh
-- ========================================

-- STEP 1: Create Schemas
CREATE SCHEMA IF NOT EXISTS oms_oltp;
CREATE SCHEMA IF NOT EXISTS slms_oltp;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ========================================
-- STEP 2: Create DWH Dimension Tables
-- ========================================

-- Dim_Customer
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       INT,
    customer_fname    VARCHAR(50),
    customer_lname    VARCHAR(50),
    customer_segment  VARCHAR(50),
    customer_city     VARCHAR(50),
    customer_state    VARCHAR(50),
    customer_country  VARCHAR(50)
);

-- Dim_Product
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key       SERIAL PRIMARY KEY,
    category_key      INT,
    product_id        INT,
    product_name      VARCHAR(100),
    product_card_id   VARCHAR(20),
    product_price     DECIMAL(10,2),
    product_image     VARCHAR(255)
);

-- Dim_Category
CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_key      SERIAL PRIMARY KEY,
    category_id       INT,
    category_name     VARCHAR(50),
    department_key    INT
);

-- Dim_Department
CREATE TABLE IF NOT EXISTS dwh.dim_department (
    department_key    SERIAL PRIMARY KEY,
    department_id     INT,
    department_name   VARCHAR(50)
);

-- Dim_Date
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key          INT PRIMARY KEY,
    full_date         DATE NOT NULL UNIQUE,
    day_of_week       VARCHAR(20),
    day_of_month      INT,
    month_number      INT,
    month_name        VARCHAR(20),
    quarter_number    INT,
    year              INT,
    is_weekend        BOOLEAN
);

-- Dim_Geography
CREATE TABLE IF NOT EXISTS dwh.dim_geography (
    geography_key     SERIAL PRIMARY KEY,
    city              VARCHAR(50),
    state             VARCHAR(50),
    country           VARCHAR(50),
    region            VARCHAR(50),
    market            VARCHAR(50),
    latitude          DECIMAL(10,6),
    longitude         DECIMAL(10,6),
    UNIQUE(city, state, country, market)
);

-- Dim_Shipping
CREATE TABLE IF NOT EXISTS dwh.dim_shipping (
    shipping_key      SERIAL PRIMARY KEY,
    shipping_mode     VARCHAR(50),
    delivery_status   VARCHAR(50),
    delivery_risk     VARCHAR(50),
    UNIQUE(shipping_mode, delivery_status, delivery_risk)
);

-- ========================================
-- STEP 3: Create Fact Table
-- ========================================

CREATE TABLE IF NOT EXISTS dwh.order_items (
    order_item_key          BIGSERIAL PRIMARY KEY,
    customer_key            INT,
    order_date_key          INT,
    shipping_date_key       INT,
    customer_geography_key  INT,
    order_geography_key     INT,
    shipping_key            INT,
    product_key             INT,
    order_id                VARCHAR(50),
    order_item_id           VARCHAR(50),
    order_status            VARCHAR(50),
    order_item_quantity     INT,
    sales                   DECIMAL(12,2),
    order_item_discount     DECIMAL(12,2),
    order_item_profit       DECIMAL(12,2),
    order_item_discount_rate DECIMAL(5,4),
    order_item_profit_ratio  DECIMAL(5,4),
    etl_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key)           REFERENCES dwh.dim_customer(customer_key),
    FOREIGN KEY (order_date_key)         REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (shipping_date_key)      REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (customer_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (order_geography_key)    REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (shipping_key)           REFERENCES dwh.dim_shipping(shipping_key),
    FOREIGN KEY (product_key)            REFERENCES dwh.dim_product(product_key)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_order_items_customer ON dwh.order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_order_items_order_date ON dwh.order_items(order_date_key);
CREATE INDEX IF NOT EXISTS idx_order_items_shipping_date ON dwh.order_items(shipping_date_key);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON dwh.order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_order_items_order_geo ON dwh.order_items(order_geography_key);

-- ========================================
-- STEP 4: Create Staging Tables
-- ========================================

CREATE TABLE IF NOT EXISTS staging.stg_oms_orders (
    order_id          VARCHAR(50),
    customer_id       INT,
    customer_fname    VARCHAR(50),
    customer_lname    VARCHAR(50),
    customer_segment  VARCHAR(50),
    order_city        VARCHAR(50),
    order_state       VARCHAR(50),
    order_country     VARCHAR(50),
    order_date        DATE,
    order_status      VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system     VARCHAR(10) DEFAULT 'OMS'
);

CREATE TABLE IF NOT EXISTS staging.stg_oms_order_items (
    order_item_id     VARCHAR(50),
    order_id          VARCHAR(50),
    product_id        INT,
    product_card_id   VARCHAR(20),
    product_name      VARCHAR(100),
    product_price     DECIMAL(10,2),
    quantity          INT,
    sales             DECIMAL(10,2),
    discount          DECIMAL(10,2),
    profit            DECIMAL(10,2),
    discount_rate     DECIMAL(5,2),
    profit_ratio      DECIMAL(5,2),
    category_id       INT,
    category_name     VARCHAR(50),
    department_id     INT,
    department_name   VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system     VARCHAR(10) DEFAULT 'OMS'
);

CREATE TABLE IF NOT EXISTS staging.stg_slms_shipments (
    shipment_id       VARCHAR(50),
    order_id          VARCHAR(50),
    shipping_mode     VARCHAR(50),
    ship_date         DATE,
    delivery_date     DATE,
    delivery_status   VARCHAR(50),
    delivery_risk     VARCHAR(50),
    ship_city         VARCHAR(50),
    ship_state        VARCHAR(50),
    ship_country      VARCHAR(50),
    latitude          DECIMAL(10,6),
    longitude         DECIMAL(10,6),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system     VARCHAR(10) DEFAULT 'SLMS'
);
