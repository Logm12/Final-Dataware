/*
Build dim_category from staging.stg_products.category
Extract unique category names from products table
*/
INSERT INTO dwh.dim_category (
    category_name
)
SELECT DISTINCT
    category AS category_name
FROM staging.stg_products
WHERE category IS NOT NULL
  AND category != '';