/*
Truncate all DWH tables before rebuilding.
Uses DROP + CREATE approach to handle tables that may not exist.
*/

-- Create tables if they don't exist (with proper structure)

-- dim_category (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_customer (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
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

-- dim_employee (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_employee (
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

-- dim_shipper (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_shipper (
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

-- dim_date (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_of_month INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter_number INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_geography (no dependencies)
CREATE TABLE IF NOT EXISTS dwh.dim_geography (
    geography_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    zip_postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_product (depends on dim_category)
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_code VARCHAR(50),
    product_name VARCHAR(200),
    description TEXT,
    standard_cost DECIMAL(12,2),
    list_price DECIMAL(12,2),
    category_key INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- order_items fact table (depends on all dimensions)
CREATE TABLE IF NOT EXISTS dwh.order_items (
    order_item_key SERIAL PRIMARY KEY,
    customer_key INTEGER,
    employee_key INTEGER,
    shipper_key INTEGER,
    product_key INTEGER,
    order_date_key INTEGER,
    shipped_date_key INTEGER,
    customer_geography_key INTEGER,
    shipping_geography_key INTEGER,
    order_id INTEGER,
    order_detail_id INTEGER,
    quantity DECIMAL(10,2),
    unit_price DECIMAL(12,2),
    discount DECIMAL(5,4),
    line_total DECIMAL(12,2),
    shipping_fee DECIMAL(12,2),
    taxes DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Now truncate all tables (they all exist now)
TRUNCATE 
    dwh.order_items,
    dwh.dim_product,
    dwh.dim_category,
    dwh.dim_customer,
    dwh.dim_employee,
    dwh.dim_shipper,
    dwh.dim_date,
    dwh.dim_geography
RESTART IDENTITY CASCADE;