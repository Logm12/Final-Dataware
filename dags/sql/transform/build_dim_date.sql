
WITH all_dates AS (
    SELECT order_date AS full_date FROM staging.stg_oms_orders
    UNION
    SELECT ship_date AS full_date FROM staging.stg_slms_shipments
)
INSERT INTO dwh.dim_date (
    full_date,
    day_of_week,
    month_number,
    quarter_number,
    year,
    is_weekend
)
SELECT 
    d.full_date,
    EXTRACT(ISODOW FROM d.full_date) AS day_of_week,
    EXTRACT(MONTH FROM d.full_date) AS month_number,
    EXTRACT(QUARTER FROM d.full_date) AS quarter_number,
    EXTRACT(YEAR FROM d.full_date) AS year,
    (EXTRACT(ISODOW FROM d.full_date) IN (6, 7)) AS is_weekend
FROM all_dates d
WHERE d.full_date IS NOT NULL

GROUP BY d.full_date; -- Đảm bảo duy nhất
