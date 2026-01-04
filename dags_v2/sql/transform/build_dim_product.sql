/*
Build dim_product from staging.stg_products
Source: products.csv
*/
INSERT INTO dwh.dim_product (
    product_id,
    product_code,
    product_name,
    description,
    standard_cost,
    list_price,
    category_key
)
SELECT DISTINCT
    p.product_id,
    p.product_code,
    p.product_name,
    p.description,
    p.standard_cost,
    p.list_price,
    c.category_key  -- Lookup surrogate key from dim_category
FROM staging.stg_products p
LEFT JOIN dwh.dim_category c 
    ON p.category = c.category_name
WHERE p.product_id IS NOT NULL;