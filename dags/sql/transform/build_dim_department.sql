INSERT INTO dwh.dim_department (
    department_id,
    department_name
)
SELECT DISTINCT
    department_id,
    department_name
FROM staging.stg_oms_order_items
WHERE department_id IS NOT NULL;