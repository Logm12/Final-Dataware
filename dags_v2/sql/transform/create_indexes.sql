/*
Create indexes on staging tables for better join performance during transformation.
Also creates indexes on DWH dimension tables for fact table lookups.
*/

-- ============================================================================
-- STAGING TABLE INDEXES
-- ============================================================================

-- stg_orders indexes
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id ON staging.stg_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_employee_id ON staging.stg_orders(employee_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_shipper_id ON staging.stg_orders(shipper_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_order_date ON staging.stg_orders(order_date);

-- stg_order_details indexes
CREATE INDEX IF NOT EXISTS idx_stg_order_details_order_id ON staging.stg_order_details(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_details_product_id ON staging.stg_order_details(product_id);

-- stg_products indexes
CREATE INDEX IF NOT EXISTS idx_stg_products_category ON staging.stg_products(category);

-- stg_customers indexes
CREATE INDEX IF NOT EXISTS idx_stg_customers_city ON staging.stg_customers(city);

-- ============================================================================
-- DWH DIMENSION INDEXES (for fact table lookups)
-- ============================================================================

-- dim_customer indexes
CREATE INDEX IF NOT EXISTS idx_dim_customer_customer_id ON dwh.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_geography ON dwh.dim_customer(customer_city, customer_state, customer_country);

-- dim_employee indexes
CREATE INDEX IF NOT EXISTS idx_dim_employee_employee_id ON dwh.dim_employee(employee_id);

-- dim_shipper indexes
CREATE INDEX IF NOT EXISTS idx_dim_shipper_shipper_id ON dwh.dim_shipper(shipper_id);

-- dim_product indexes
CREATE INDEX IF NOT EXISTS idx_dim_product_product_id ON dwh.dim_product(product_id);

-- dim_date indexes
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dwh.dim_date(full_date);

-- dim_geography indexes
CREATE INDEX IF NOT EXISTS idx_dim_geography_lookup ON dwh.dim_geography(city, state, country);

-- dim_category indexes
CREATE INDEX IF NOT EXISTS idx_dim_category_name ON dwh.dim_category(category_name);

-- ============================================================================
-- FACT TABLE INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_order_items_customer_key ON dwh.order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_order_items_product_key ON dwh.order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_order_items_order_date_key ON dwh.order_items(order_date_key);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON dwh.order_items(order_id);
