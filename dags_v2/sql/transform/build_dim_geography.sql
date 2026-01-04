/*
Build dim_geography from various staging tables
Collects unique geography combinations from:
- Customer addresses (stg_customers)
- Shipping addresses (stg_orders)
- Employee addresses (stg_employees)
- Shipper addresses (stg_shippers)
- Supplier addresses (stg_suppliers)
*/
INSERT INTO dwh.dim_geography (
    city,
    state,
    country,
    zip_postal_code
)
-- Customer addresses
SELECT DISTINCT 
    city, 
    state_province AS state, 
    country_region AS country,
    zip_postal_code
FROM staging.stg_customers
WHERE city IS NOT NULL

UNION

-- Shipping addresses from orders
SELECT DISTINCT 
    ship_city AS city, 
    ship_state_province AS state, 
    ship_country_region AS country,
    ship_zip_postal_code AS zip_postal_code
FROM staging.stg_orders
WHERE ship_city IS NOT NULL

UNION

-- Employee addresses
SELECT DISTINCT 
    city, 
    state_province AS state, 
    country_region AS country,
    zip_postal_code
FROM staging.stg_employees
WHERE city IS NOT NULL

UNION

-- Shipper addresses
SELECT DISTINCT 
    city, 
    state_province AS state, 
    country_region AS country,
    zip_postal_code
FROM staging.stg_shippers
WHERE city IS NOT NULL

UNION

-- Supplier addresses
SELECT DISTINCT 
    city, 
    state_province AS state, 
    country_region AS country,
    zip_postal_code
FROM staging.stg_suppliers
WHERE city IS NOT NULL;