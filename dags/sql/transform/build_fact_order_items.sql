
INSERT INTO dwh.order_items (
    -- Foreign Keys
    customer_key,
    order_date_key,
    shipping_date_key,
    customer_geography_key,
    order_geography_key,
    shipping_key,
    product_key,
    
    -- Measures
    order_item_quantity,
    sales,
    order_item_discount,
    order_item_profit,
    order_item_discount_rate,
    order_item_profit_ratio
)
SELECT
    -- Lookups (Tra cứu khóa)
    c.customer_key,
    d_order.date_key AS order_date_key,
    d_ship.date_key AS shipping_date_key,
    g_cust.geography_key AS customer_geography_key,
    g_order.geography_key AS order_geography_key,
    s.shipping_key,
    p.product_key,

    -- Measures (Số liệu)
    s_item.quantity AS order_item_quantity,
    s_item.sales,
    s_item.discount AS order_item_discount,
    s_item.profit AS order_item_profit,
    s_item.discount_rate AS order_item_discount_rate,
    s_item.profit_ratio AS order_item_profit_ratio

FROM staging.stg_oms_order_items AS s_item

LEFT JOIN staging.stg_oms_orders AS s_order
    ON s_item.order_id = s_order.order_id

-- (Dựa trên logic ingest_dag: order_item_id = shipment_id)
LEFT JOIN staging.stg_slms_shipments AS s_ship
    ON s_item.order_item_id = s_ship.shipment_id 
    

LEFT JOIN dwh.dim_customer AS c
    ON s_order.customer_id = c.customer_id

LEFT JOIN dwh.dim_product AS p
    ON s_item.product_id = p.product_id

LEFT JOIN dwh.dim_date AS d_order
    ON s_order.order_date = d_order.full_date

LEFT JOIN dwh.dim_date AS d_ship
    ON s_ship.ship_date = d_ship.full_date

LEFT JOIN dwh.dim_geography AS g_cust
    ON c.customer_city = g_cust.city
    AND c.customer_state = g_cust.state
    AND c.customer_country = g_cust.country

LEFT JOIN dwh.dim_geography AS g_order
    ON s_order.order_city = g_order.city
    AND s_order.order_state = g_order.state
    AND s_order.order_country = g_order.country

LEFT JOIN dwh.dim_shipping AS s
    ON s_ship.shipping_mode = s.shipping_mode
    AND s_ship.delivery_status = s.delivery_status

    AND s_ship.delivery_risk = s.delivery_risk;
