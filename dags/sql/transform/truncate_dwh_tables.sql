TRUNCATE 
    dwh.dim_customer,
    dwh.dim_product,
    dwh.dim_category,
    dwh.dim_department,
    dwh.dim_date,
    dwh.dim_geography,
    dwh.dim_shipping,
    dwh.order_items

RESTART IDENTITY CASCADE;
