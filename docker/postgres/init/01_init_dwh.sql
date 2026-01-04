-- ========================================
-- NORTHWIND DATA WAREHOUSE - DATABASE INITIALIZATION
-- ========================================
-- This script runs automatically when PostgreSQL container starts.
-- It creates all required schemas and tables for the Northwind DWH.
-- ========================================

-- ========================================
-- STEP 1: Create Schemas
-- ========================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS reporting;

-- ========================================
-- STEP 2: Create Staging Tables
-- ========================================

-- Staging: Customers
CREATE TABLE staging.stg_customers (
    customer_id       INTEGER,
    company           VARCHAR(200),
    last_name         VARCHAR(100),
    first_name        VARCHAR(100),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    business_phone    VARCHAR(50),
    home_phone        VARCHAR(50),
    mobile_phone      VARCHAR(50),
    fax_number        VARCHAR(50),
    address           TEXT,
    city              VARCHAR(100),
    state_province    VARCHAR(100),
    zip_postal_code   VARCHAR(20),
    country_region    VARCHAR(100),
    web_page          VARCHAR(255),
    notes             TEXT,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Employees
CREATE TABLE staging.stg_employees (
    employee_id       INTEGER,
    company           VARCHAR(200),
    last_name         VARCHAR(100),
    first_name        VARCHAR(100),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    business_phone    VARCHAR(50),
    home_phone        VARCHAR(50),
    mobile_phone      VARCHAR(50),
    fax_number        VARCHAR(50),
    address           TEXT,
    city              VARCHAR(100),
    state_province    VARCHAR(100),
    zip_postal_code   VARCHAR(20),
    country_region    VARCHAR(100),
    web_page          VARCHAR(255),
    notes             TEXT,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Orders
CREATE TABLE staging.stg_orders (
    order_id              INTEGER,
    employee_id           INTEGER,
    customer_id           INTEGER,
    order_date            TIMESTAMP,
    shipped_date          TIMESTAMP,
    shipper_id            INTEGER,
    ship_name             VARCHAR(200),
    ship_address          TEXT,
    ship_city             VARCHAR(100),
    ship_state_province   VARCHAR(100),
    ship_zip_postal_code  VARCHAR(20),
    ship_country_region   VARCHAR(100),
    shipping_fee          DECIMAL(12,2),
    taxes                 DECIMAL(12,2),
    payment_type          VARCHAR(50),
    paid_date             TIMESTAMP,
    notes                 TEXT,
    tax_rate              DECIMAL(10,4),
    tax_status_id         INTEGER,
    status_id             INTEGER,
    load_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file           VARCHAR(100)
);

-- Staging: Order Details
CREATE TABLE staging.stg_order_details (
    order_detail_id     INTEGER,
    order_id            INTEGER,
    product_id          INTEGER,
    quantity            DECIMAL(10,2),
    unit_price          DECIMAL(12,2),
    discount            DECIMAL(5,4),
    status_id           INTEGER,
    date_allocated      TIMESTAMP,
    purchase_order_id   INTEGER,
    inventory_id        INTEGER,
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(100)
);

-- Staging: Products
CREATE TABLE staging.stg_products (
    product_id                INTEGER,
    supplier_ids              VARCHAR(100),
    product_code              VARCHAR(50),
    product_name              VARCHAR(200),
    description               TEXT,
    standard_cost             DECIMAL(12,2),
    list_price                DECIMAL(12,2),
    reorder_level             INTEGER,
    target_level              INTEGER,
    quantity_per_unit         VARCHAR(100),
    discontinued              INTEGER,
    minimum_reorder_quantity  INTEGER,
    category                  VARCHAR(100),
    load_timestamp            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file               VARCHAR(100)
);

-- Staging: Shippers
CREATE TABLE staging.stg_shippers (
    shipper_id        INTEGER,
    company           VARCHAR(200),
    last_name         VARCHAR(100),
    first_name        VARCHAR(100),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    business_phone    VARCHAR(50),
    home_phone        VARCHAR(50),
    mobile_phone      VARCHAR(50),
    fax_number        VARCHAR(50),
    address           TEXT,
    city              VARCHAR(100),
    state_province    VARCHAR(100),
    zip_postal_code   VARCHAR(20),
    country_region    VARCHAR(100),
    web_page          VARCHAR(255),
    notes             TEXT,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Suppliers
CREATE TABLE staging.stg_suppliers (
    supplier_id       INTEGER,
    company           VARCHAR(200),
    last_name         VARCHAR(100),
    first_name        VARCHAR(100),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    business_phone    VARCHAR(50),
    home_phone        VARCHAR(50),
    mobile_phone      VARCHAR(50),
    fax_number        VARCHAR(50),
    address           TEXT,
    city              VARCHAR(100),
    state_province    VARCHAR(100),
    zip_postal_code   VARCHAR(20),
    country_region    VARCHAR(100),
    web_page          VARCHAR(255),
    notes             TEXT,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Invoices
CREATE TABLE staging.stg_invoices (
    invoice_id        INTEGER,
    order_id          INTEGER,
    invoice_date      TIMESTAMP,
    due_date          TIMESTAMP,
    tax               DECIMAL(12,2),
    shipping          DECIMAL(12,2),
    amount_due        DECIMAL(12,2),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Orders Status
CREATE TABLE staging.stg_orders_status (
    status_id         INTEGER,
    status_name       VARCHAR(100),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Order Details Status
CREATE TABLE staging.stg_order_details_status (
    status_id         INTEGER,
    status_name       VARCHAR(100),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Inventory Transactions
CREATE TABLE staging.stg_inventory_transactions (
    transaction_id              INTEGER,
    transaction_type            INTEGER,
    transaction_created_date    TIMESTAMP,
    transaction_modified_date   TIMESTAMP,
    product_id                  INTEGER,
    quantity                    INTEGER,
    purchase_order_id           INTEGER,
    customer_order_id           INTEGER,
    comments                    TEXT,
    load_timestamp              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file                 VARCHAR(100)
);

-- Staging: Inventory Transaction Types
CREATE TABLE staging.stg_inventory_transaction_types (
    type_id           INTEGER,
    type_name         VARCHAR(100),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- Staging: Purchase Orders
CREATE TABLE staging.stg_purchase_orders (
    purchase_order_id   INTEGER,
    supplier_id         INTEGER,
    created_by          INTEGER,
    submitted_date      TIMESTAMP,
    creation_date       TIMESTAMP,
    status_id           INTEGER,
    expected_date       TIMESTAMP,
    shipping_fee        DECIMAL(12,2),
    taxes               DECIMAL(12,2),
    payment_date        TIMESTAMP,
    payment_amount      DECIMAL(12,2),
    payment_method      VARCHAR(100),
    notes               TEXT,
    approved_by         INTEGER,
    approved_date       TIMESTAMP,
    submitted_by        INTEGER,
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(100)
);

-- Staging: Purchase Order Details
CREATE TABLE staging.stg_purchase_order_details (
    purchase_order_detail_id  INTEGER,
    purchase_order_id         INTEGER,
    product_id                INTEGER,
    quantity                  DECIMAL(10,2),
    unit_cost                 DECIMAL(12,2),
    date_received             TIMESTAMP,
    posted_to_inventory       INTEGER,
    inventory_id              INTEGER,
    load_timestamp            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file               VARCHAR(100)
);

-- Staging: Purchase Order Status
CREATE TABLE staging.stg_purchase_order_status (
    status_id         INTEGER,
    status            VARCHAR(100),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(100)
);

-- ========================================
-- STEP 3: Create DWH Dimension Tables
-- ========================================

-- Dim_Customer
CREATE TABLE dwh.dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       INTEGER NOT NULL,
    customer_fname    VARCHAR(100),
    customer_lname    VARCHAR(100),
    company           VARCHAR(200),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    customer_city     VARCHAR(100),
    customer_state    VARCHAR(100),
    customer_country  VARCHAR(100),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Employee
CREATE TABLE dwh.dim_employee (
    employee_key      SERIAL PRIMARY KEY,
    employee_id       INTEGER NOT NULL,
    employee_fname    VARCHAR(100),
    employee_lname    VARCHAR(100),
    company           VARCHAR(200),
    email_address     VARCHAR(200),
    job_title         VARCHAR(100),
    employee_city     VARCHAR(100),
    employee_state    VARCHAR(100),
    employee_country  VARCHAR(100),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Shipper
CREATE TABLE dwh.dim_shipper (
    shipper_key       SERIAL PRIMARY KEY,
    shipper_id        INTEGER NOT NULL,
    shipper_name      VARCHAR(200),
    shipper_fname     VARCHAR(100),
    shipper_lname     VARCHAR(100),
    shipper_city      VARCHAR(100),
    shipper_state     VARCHAR(100),
    shipper_country   VARCHAR(100),
    business_phone    VARCHAR(50),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Category
CREATE TABLE dwh.dim_category (
    category_key      SERIAL PRIMARY KEY,
    category_name     VARCHAR(100) NOT NULL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Product
CREATE TABLE dwh.dim_product (
    product_key       SERIAL PRIMARY KEY,
    product_id        INTEGER NOT NULL,
    product_code      VARCHAR(50),
    product_name      VARCHAR(200),
    description       TEXT,
    standard_cost     DECIMAL(12,2),
    list_price        DECIMAL(12,2),
    category_key      INTEGER REFERENCES dwh.dim_category(category_key),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Date
CREATE TABLE dwh.dim_date (
    date_key          SERIAL PRIMARY KEY,
    full_date         DATE NOT NULL,
    day_of_week       INTEGER,
    day_of_month      INTEGER,
    month_number      INTEGER,
    month_name        VARCHAR(20),
    quarter_number    INTEGER,
    year              INTEGER,
    is_weekend        BOOLEAN,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dim_Geography
CREATE TABLE dwh.dim_geography (
    geography_key     SERIAL PRIMARY KEY,
    city              VARCHAR(100),
    state             VARCHAR(100),
    country           VARCHAR(100),
    zip_postal_code   VARCHAR(20),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- STEP 4: Create DWH Fact Table
-- ========================================

CREATE TABLE dwh.order_items (
    order_item_key            SERIAL PRIMARY KEY,
    -- Foreign Keys
    customer_key              INTEGER REFERENCES dwh.dim_customer(customer_key),
    employee_key              INTEGER REFERENCES dwh.dim_employee(employee_key),
    shipper_key               INTEGER REFERENCES dwh.dim_shipper(shipper_key),
    product_key               INTEGER REFERENCES dwh.dim_product(product_key),
    order_date_key            INTEGER REFERENCES dwh.dim_date(date_key),
    shipped_date_key          INTEGER REFERENCES dwh.dim_date(date_key),
    customer_geography_key    INTEGER REFERENCES dwh.dim_geography(geography_key),
    shipping_geography_key    INTEGER REFERENCES dwh.dim_geography(geography_key),
    -- Degenerate Dimensions
    order_id                  INTEGER,
    order_detail_id           INTEGER,
    -- Measures
    quantity                  DECIMAL(10,2),
    unit_price                DECIMAL(12,2),
    discount                  DECIMAL(5,4),
    line_total                DECIMAL(12,2),
    shipping_fee              DECIMAL(12,2),
    taxes                     DECIMAL(12,2),
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- STEP 5: Create Audit Table
-- ========================================

CREATE TABLE dwh.pipeline_audit (
    audit_id          SERIAL PRIMARY KEY,
    pipeline_name     VARCHAR(100),
    execution_date    TIMESTAMP,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    status            VARCHAR(20),
    records_processed INTEGER,
    error_message     TEXT
);

-- ========================================
-- STEP 6: Verification
-- ========================================

-- List all tables created
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname IN ('staging', 'dwh', 'reporting')
ORDER BY schemaname, tablename;