/*
Build order_items fact table
Main grain: one row per order_detail (line item)

Joins staging tables with dimension tables to get surrogate keys
Calculates derived measures like line_total
*/
INSERT INTO dwh.order_items (
    -- Foreign Keys (surrogate keys from dimensions)
    customer_key,
    employee_key,
    shipper_key,
    product_key,
    order_date_key,
    shipped_date_key,
    customer_geography_key,
    shipping_geography_key,
    
    -- Degenerate dimensions (from order)
    order_id,
    order_detail_id,
    
    -- Measures
    quantity,
    unit_price,
    discount,
    line_total,
    shipping_fee,
    taxes
)
SELECT
    -- Dimension lookups
    c.customer_key,
    e.employee_key,
    sh.shipper_key,
    p.product_key,
    d_order.date_key AS order_date_key,
    d_ship.date_key AS shipped_date_key,
    g_cust.geography_key AS customer_geography_key,
    g_ship.geography_key AS shipping_geography_key,
    
    -- Degenerate dimensions
    od.order_id,
    od.order_detail_id,
    
    -- Measures
    od.quantity,
    od.unit_price,
    od.discount,
    -- Calculated: line_total = quantity * unit_price * (1 - discount)
    ROUND((od.quantity * od.unit_price * (1 - COALESCE(od.discount, 0)))::NUMERIC, 2) AS line_total,
    o.shipping_fee,
    o.taxes

FROM staging.stg_order_details AS od

-- Join to get order header info
INNER JOIN staging.stg_orders AS o
    ON od.order_id = o.order_id

-- === DIMENSION LOOKUPS ===

-- Customer dimension
LEFT JOIN dwh.dim_customer AS c
    ON o.customer_id = c.customer_id

-- Employee dimension
LEFT JOIN dwh.dim_employee AS e
    ON o.employee_id = e.employee_id

-- Shipper dimension
LEFT JOIN dwh.dim_shipper AS sh
    ON o.shipper_id = sh.shipper_id

-- Product dimension
LEFT JOIN dwh.dim_product AS p
    ON od.product_id = p.product_id

-- Order Date dimension (role-playing)
LEFT JOIN dwh.dim_date AS d_order
    ON o.order_date::DATE = d_order.full_date

-- Shipped Date dimension (role-playing)
LEFT JOIN dwh.dim_date AS d_ship
    ON o.shipped_date::DATE = d_ship.full_date

-- Customer Geography (from customer address)
LEFT JOIN dwh.dim_geography AS g_cust
    ON c.customer_city = g_cust.city
    AND c.customer_state = g_cust.state
    AND c.customer_country = g_cust.country

-- Shipping Geography (from order ship address)
LEFT JOIN dwh.dim_geography AS g_ship
    ON o.ship_city = g_ship.city
    AND o.ship_state_province = g_ship.state
    AND o.ship_country_region = g_ship.country;