/*
============================================================================
CREATE DWH SCHEMA AND TABLES FOR NORTHWIND DATA WAREHOUSE
============================================================================
Run this script ONCE to create all required schemas and tables.
This should be run before the first DAG execution.
============================================================================
*/

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS reporting;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- dim_customer
DROP TABLE IF EXISTS dwh.dim_customer CASCADE;
CREATE TABLE dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_fname VARCHAR(100),
    customer_lname VARCHAR(100),
    company VARCHAR(200),
    email_address VARCHAR(200),
    job_title VARCHAR(100),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_employee
DROP TABLE IF EXISTS dwh.dim_employee CASCADE;
CREATE TABLE dwh.dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    employee_fname VARCHAR(100),
    employee_lname VARCHAR(100),
    company VARCHAR(200),
    email_address VARCHAR(200),
    job_title VARCHAR(100),
    employee_city VARCHAR(100),
    employee_state VARCHAR(100),
    employee_country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_shipper
DROP TABLE IF EXISTS dwh.dim_shipper CASCADE;
CREATE TABLE dwh.dim_shipper (
    shipper_key SERIAL PRIMARY KEY,
    shipper_id INTEGER NOT NULL,
    shipper_name VARCHAR(200),
    shipper_fname VARCHAR(100),
    shipper_lname VARCHAR(100),
    shipper_city VARCHAR(100),
    shipper_state VARCHAR(100),
    shipper_country VARCHAR(100),
    business_phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_category
DROP TABLE IF EXISTS dwh.dim_category CASCADE;
CREATE TABLE dwh.dim_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_product
DROP TABLE IF EXISTS dwh.dim_product CASCADE;
CREATE TABLE dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_code VARCHAR(50),
    product_name VARCHAR(200),
    description TEXT,
    standard_cost DECIMAL(12,2),
    list_price DECIMAL(12,2),
    category_key INTEGER REFERENCES dwh.dim_category(category_key),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_date
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
CREATE TABLE dwh.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_of_month INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter_number INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_geography
DROP TABLE IF EXISTS dwh.dim_geography CASCADE;
CREATE TABLE dwh.dim_geography (
    geography_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    zip_postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

DROP TABLE IF EXISTS dwh.order_items CASCADE;
CREATE TABLE dwh.order_items (
    order_item_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    customer_key INTEGER REFERENCES dwh.dim_customer(customer_key),
    employee_key INTEGER REFERENCES dwh.dim_employee(employee_key),
    shipper_key INTEGER REFERENCES dwh.dim_shipper(shipper_key),
    product_key INTEGER REFERENCES dwh.dim_product(product_key),
    order_date_key INTEGER REFERENCES dwh.dim_date(date_key),
    shipped_date_key INTEGER REFERENCES dwh.dim_date(date_key),
    customer_geography_key INTEGER REFERENCES dwh.dim_geography(geography_key),
    shipping_geography_key INTEGER REFERENCES dwh.dim_geography(geography_key),
    
    -- Degenerate Dimensions (order identifiers)
    order_id INTEGER,
    order_detail_id INTEGER,
    
    -- Measures
    quantity DECIMAL(10,2),
    unit_price DECIMAL(12,2),
    discount DECIMAL(5,4),
    line_total DECIMAL(12,2),
    shipping_fee DECIMAL(12,2),
    taxes DECIMAL(12,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- AUDIT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS dwh.pipeline_audit (
    audit_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    execution_date TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_processed INT,
    error_message TEXT
);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check all tables were created
SELECT 
    schemaname, 
    tablename 
FROM pg_tables 
WHERE schemaname IN ('staging', 'dwh', 'reporting')
ORDER BY schemaname, tablename;
