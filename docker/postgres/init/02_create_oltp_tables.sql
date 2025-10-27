-- =====================================================================
-- File: 02_create_oltp_tables.sql
-- Description: Creates all tables for the sales_oltp schema based on the ERD.
-- Author: Your AI Assistant
-- Date: 2025-10-26
-- =====================================================================

-- Switch to the correct schema
SET search_path TO sales_oltp;

-- Table: department
CREATE TABLE IF NOT EXISTS department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(255) NOT NULL
);

-- Table: category
CREATE TABLE IF NOT EXISTS category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL,
    department_id INT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(department_id)
);

-- Table: products
CREATE TABLE IF NOT EXISTS products (
    product_card_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    product_image VARCHAR(500),
    product_price NUMERIC(10, 2) NOT NULL,
    product_status INT, -- e.g., 0 for inactive, 1 for active
    category_id INT NOT NULL,
    FOREIGN KEY (category_id) REFERENCES category(category_id)
);

-- Table: customer
CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL PRIMARY KEY,
    customer_fname VARCHAR(255) NOT NULL,
    customer_lname VARCHAR(255) NOT NULL,
    customer_segment VARCHAR(50), -- Proposed: 'Consumer', 'Corporate', 'Home Office'
    customer_street VARCHAR(255),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_zipcode VARCHAR(20),
    customer_country VARCHAR(100),
    sales_per_customer NUMERIC(12, 2) -- Note: This is an aggregated field, usually calculated, not stored in OLTP. Included as per ERD.
);

-- Table: "order" (using quotes because ORDER is a reserved keyword in SQL)
CREATE TABLE IF NOT EXISTS "order" (
    order_id SERIAL PRIMARY KEY,
    order_customer_id INT NOT NULL,
    order_status VARCHAR(50), -- Proposed: 'Processing', 'Shipped', 'Delivered', 'Canceled', 'Returned'
    order_date TIMESTAMP NOT NULL,
    type VARCHAR(50), -- Proposed: 'Online', 'Retail'
    days_for_shipment_scheduled INT,
    market VARCHAR(50), -- Proposed: 'APAC', 'EMEA', 'LATAM', 'USCA'
    order_region VARCHAR(100),
    order_city VARCHAR(100),
    order_state VARCHAR(100),
    order_country VARCHAR(100),
    order_zipcode VARCHAR(20),
    FOREIGN KEY (order_customer_id) REFERENCES customer(customer_id)
);

-- Table: order_items
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_card_id INT NOT NULL,
    order_item_product_price NUMERIC(10, 2) NOT NULL,
    order_item_discount NUMERIC(10, 2) DEFAULT 0,
    order_item_discount_rate NUMERIC(5, 4) DEFAULT 0,
    order_item_quantity INT NOT NULL,
    sales NUMERIC(12, 2) NOT NULL,
    order_item_total NUMERIC(12, 2) NOT NULL,
    order_item_profit_ratio NUMERIC(5, 4),
    FOREIGN KEY (order_id) REFERENCES "order"(order_id),
    FOREIGN KEY (product_card_id) REFERENCES products(product_card_id)
);

-- Table: shipments
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    order_customer_id INT, -- Denormalized for convenience, can be retrieved via order_id
    shipping_mode VARCHAR(100),
    days_for_shipping INT,
    delivery_status VARCHAR(50), -- Proposed: 'Shipping on time', 'Late delivery', 'Advance shipping', 'Returned to sender'
    late_delivery_risk BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(100),
    customer_country VARCHAR(100),
    shipping_date TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES "order"(order_id)
);