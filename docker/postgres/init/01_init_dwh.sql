-- ========================================
-- STEP 1: Create Schemas
-- ========================================
CREATE SCHEMA IF NOT EXISTS oms_oltp;
CREATE SCHEMA IF NOT EXISTS slms_oltp;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ========================================
-- STEP 2: Create Tables in dwh Schema
-- ========================================

-- Dim_Customer
CREATE TABLE dwh.dim_customer (
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
CREATE TABLE dwh.dim_product (
    product_key       SERIAL PRIMARY KEY,
    category_key      INT,
    product_id        INT,
    product_name      VARCHAR(100),
    product_card_id   VARCHAR(20),
    product_price     DECIMAL(10,2),
    product_image     VARCHAR(255)
);

-- Dim_Category
CREATE TABLE dwh.dim_category (
    category_key      SERIAL PRIMARY KEY,
    category_id       INT,
    category_name     VARCHAR(50),
    department_key    INT
);

-- Dim_Department
CREATE TABLE dwh.dim_department (
    department_key    SERIAL PRIMARY KEY,
    department_id     INT,
    department_name   VARCHAR(50)
);

-- Dim_Date (dùng chung cho order_date và shipping_date)
CREATE TABLE dwh.dim_date (
    date_key          SERIAL PRIMARY KEY,
    full_date         DATE NOT NULL UNIQUE,
    day_of_week       INT,
    month_number      INT,
    quarter_number    INT,
    year              INT,
    is_weekend        BOOLEAN
);

-- Dim_Geography
CREATE TABLE dwh.dim_geography (
    geography_key     SERIAL PRIMARY KEY,
    city              VARCHAR(50),
    state             VARCHAR(50),
    country           VARCHAR(50),
    region            VARCHAR(50),
    market            VARCHAR(50),
    latitude          DECIMAL(10,6),
    longitude         DECIMAL(10,6)
);

-- Dim_Shipping
CREATE TABLE dwh.dim_shipping (
    shipping_key      SERIAL PRIMARY KEY,
    shipping_mode     VARCHAR(50),
    delivery_status   VARCHAR(50),
    delivery_risk     VARCHAR(50)
);

-- Fact Table: Order_Items
CREATE TABLE dwh.order_items (
    order_item_key          SERIAL PRIMARY KEY,
    customer_key            INT,
    order_date_key          INT,
    shipping_date_key       INT,
    customer_geography_key  INT,
    order_geography_key     INT,
    shipping_key            INT,
    product_key             INT,
    order_item_quantity     INT,
    sales                   DECIMAL(10,2),
    order_item_discount     DECIMAL(10,2),
    order_item_profit       DECIMAL(10,2),
    order_item_discount_rate DECIMAL(5,2),
    order_item_profit_ratio  DECIMAL(5,2),
    FOREIGN KEY (customer_key)           REFERENCES dwh.dim_customer(customer_key),
    FOREIGN KEY (order_date_key)         REFERENCES dwh.dim_date(date_key),
FOREIGN KEY (shipping_date_key)      REFERENCES dwh.dim_date(date_key),
    FOREIGN KEY (customer_geography_key) REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (order_geography_key)    REFERENCES dwh.dim_geography(geography_key),
    FOREIGN KEY (shipping_key)           REFERENCES dwh.dim_shipping(shipping_key),
    FOREIGN KEY (product_key)            REFERENCES dwh.dim_product(product_key)
);

-- ========================================
-- STEP 3: Create Staging Tables (no optional columns)
-- ========================================

-- STAGING: OMS – Orders
CREATE TABLE staging.stg_oms_orders (
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

-- STAGING: OMS – Order Items
CREATE TABLE staging.stg_oms_order_items (
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

-- STAGING: SLMS – Shipments
CREATE TABLE staging.stg_slms_shipments (
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
