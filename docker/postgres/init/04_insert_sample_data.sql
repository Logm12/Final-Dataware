-- =====================================================================
-- File: 04_insert_sample_data.sql
-- Description: Inserts sample data into the sales_oltp schema tables.
--              Data is inserted in an order that respects foreign key constraints.
-- Author: Your AI Assistant
-- Date: 2025-10-26
-- =====================================================================

-- Switch to the correct schema
SET search_path TO sales_oltp;

-- Step 1: Insert data into tables with no dependencies
-- Departments (Generated IDs: 1, 2)
INSERT INTO department (department_name) VALUES
('Electronics'),
('Home & Garden');

-- Customers (Generated IDs: 1, 2)
INSERT INTO customer (customer_fname, customer_lname, customer_segment, customer_street, customer_city, customer_state, customer_zipcode, customer_country) VALUES
('John', 'Smith', 'Consumer', '123 Maple St', 'Anytown', 'CA', '90210', 'USA'),
('Jane', 'Doe', 'Corporate', '456 Oak Ave', 'Metropolis', 'NY', '10001', 'USA');

-- Step 2: Insert data into tables that depend on Step 1
-- Categories (Generated IDs: 1, 2, 3)
INSERT INTO category (category_name, department_id) VALUES
('Smartphones & Accessories', 1), -- Belongs to Electronics (ID: 1)
('Laptops & Desktops', 1),        -- Belongs to Electronics (ID: 1)
('Kitchen Appliances', 2);       -- Belongs to Home & Garden (ID: 2)

-- Orders (Generated IDs: 1, 2, 3)
-- Order 1 for John Smith (customer_id=1)
INSERT INTO "order" (order_customer_id, order_status, order_date, type, market, order_region, days_for_shipment_scheduled) VALUES
(1, 'Delivered', '2025-09-10 10:00:00', 'Online', 'USCA', 'West', 4);
-- Order 2 for Jane Doe (customer_id=2)
INSERT INTO "order" (order_customer_id, order_status, order_date, type, market, order_region, days_for_shipment_scheduled) VALUES
(2, 'Shipped', '2025-10-15 14:30:00', 'Online', 'USCA', 'East', 4);
-- Order 3 for Jane Doe (customer_id=2)
INSERT INTO "order" (order_customer_id, order_status, order_date, type, market, order_region, days_for_shipment_scheduled) VALUES
(2, 'Processing', '2025-10-25 11:00:00', 'Retail', 'USCA', 'East', 2);

-- Step 3: Insert data into tables that depend on Step 2
-- Products (Generated IDs: 1, 2, 3, 4)
INSERT INTO products (product_name, product_price, category_id, product_status) VALUES
('SuperPhone X', 999.99, 1, 1),                 -- Belongs to Smartphones (ID: 1)
('PowerBook Pro 15', 1799.50, 2, 1),            -- Belongs to Laptops (ID: 2)
('Smart Blender 5000', 129.00, 3, 1),           -- Belongs to Kitchen (ID: 3)
('UltraThin Case for SuperPhone X', 25.50, 1, 1); -- Belongs to Smartphones (ID: 1)

-- Step 4: Insert data into the most dependent tables
-- Order Items (The heart of the transactions)
-- Items for John's Order (order_id=1)
INSERT INTO order_items (order_id, product_card_id, order_item_quantity, order_item_product_price, sales, order_item_total) VALUES
(1, 2, 1, 1799.50, 1799.50, 1799.50); -- 1 PowerBook Pro
-- Items for Jane's First Order (order_id=2)
INSERT INTO order_items (order_id, product_card_id, order_item_quantity, order_item_product_price, sales, order_item_total) VALUES
(2, 1, 1, 999.99, 999.99, 999.99),   -- 1 SuperPhone X
(2, 4, 1, 25.50, 25.50, 25.50);     -- 1 Case for the phone
-- Items for Jane's Second Order (order_id=3)
INSERT INTO order_items (order_id, product_card_id, order_item_quantity, order_item_product_price, sales, order_item_discount, order_item_total) VALUES
(3, 3, 2, 129.00, 258.00, 10.00, 248.00); -- 2 Smart Blenders with a $10 discount

-- Shipments (Only for orders that have been shipped or delivered)
-- Shipment for John's order (order_id=1)
INSERT INTO shipments (order_id, shipping_mode, delivery_status, late_delivery_risk, shipping_date, days_for_shipping) VALUES
(1, 'First Class', 'Shipping on time', FALSE, '2025-09-12 18:00:00', 2);
-- Shipment for Jane's first order (order_id=2)
INSERT INTO shipments (order_id, shipping_mode, delivery_status, late_delivery_risk, shipping_date, days_for_shipping) VALUES
(2, 'Standard Class', 'Late delivery', TRUE, '2025-10-20 09:00:00', 5);
-- Note: Order 3 has no shipment because its status is 'Processing'.