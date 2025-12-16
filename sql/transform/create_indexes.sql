-- Create indexes on staging tables to speed up joins and lookups

-- staging.stg_oms_orders
CREATE INDEX IF NOT EXISTS idx_stg_oms_orders_order_id ON staging.stg_oms_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_orders_customer_id ON staging.stg_oms_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_orders_order_date ON staging.stg_oms_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_stg_oms_orders_geo ON staging.stg_oms_orders(order_city, order_state, order_country);

-- staging.stg_oms_order_items
CREATE INDEX IF NOT EXISTS idx_stg_oms_order_items_order_id ON staging.stg_oms_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_order_items_order_item_id ON staging.stg_oms_order_items(order_item_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_order_items_product_id ON staging.stg_oms_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_order_items_category_id ON staging.stg_oms_order_items(category_id);
CREATE INDEX IF NOT EXISTS idx_stg_oms_order_items_department_id ON staging.stg_oms_order_items(department_id);

-- staging.stg_slms_shipments
CREATE INDEX IF NOT EXISTS idx_stg_slms_shipments_shipment_id ON staging.stg_slms_shipments(shipment_id);
CREATE INDEX IF NOT EXISTS idx_stg_slms_shipments_ship_date ON staging.stg_slms_shipments(ship_date);
