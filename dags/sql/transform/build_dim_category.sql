INSERT INTO dwh.dim_category (
    category_id,
    category_name,
    department_key
)
SELECT DISTINCT
    s.category_id,
    s.category_name,
    d.department_key
FROM staging.stg_oms_order_items s
LEFT JOIN dwh.dim_department d 
    ON s.department_id = d.department_id

WHERE s.category_id IS NOT NULL;
