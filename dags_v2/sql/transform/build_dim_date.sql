/*
Build dim_date from all date columns in staging tables
Collects unique dates from:
- Order dates (stg_orders)
- Shipped dates (stg_orders)
- Paid dates (stg_orders)
- Invoice dates (stg_invoices)
- Purchase order dates (stg_purchase_orders)
*/
WITH all_dates AS (
    -- Order dates
    SELECT order_date AS full_date FROM staging.stg_orders WHERE order_date IS NOT NULL
    UNION
    SELECT shipped_date AS full_date FROM staging.stg_orders WHERE shipped_date IS NOT NULL
    UNION
    SELECT paid_date AS full_date FROM staging.stg_orders WHERE paid_date IS NOT NULL
    UNION
    -- Invoice dates
    SELECT invoice_date AS full_date FROM staging.stg_invoices WHERE invoice_date IS NOT NULL
    UNION
    SELECT due_date AS full_date FROM staging.stg_invoices WHERE due_date IS NOT NULL
    UNION
    -- Purchase order dates
    SELECT creation_date AS full_date FROM staging.stg_purchase_orders WHERE creation_date IS NOT NULL
    UNION
    SELECT submitted_date AS full_date FROM staging.stg_purchase_orders WHERE submitted_date IS NOT NULL
    UNION
    SELECT expected_date AS full_date FROM staging.stg_purchase_orders WHERE expected_date IS NOT NULL
    UNION
    SELECT payment_date AS full_date FROM staging.stg_purchase_orders WHERE payment_date IS NOT NULL
)
INSERT INTO dwh.dim_date (
    full_date,
    day_of_week,
    day_of_month,
    month_number,
    month_name,
    quarter_number,
    year,
    is_weekend
)
SELECT DISTINCT
    d.full_date::DATE,
    EXTRACT(ISODOW FROM d.full_date) AS day_of_week,  -- 1=Monday, 7=Sunday
    EXTRACT(DAY FROM d.full_date) AS day_of_month,
    EXTRACT(MONTH FROM d.full_date) AS month_number,
    TO_CHAR(d.full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d.full_date) AS quarter_number,
    EXTRACT(YEAR FROM d.full_date) AS year,
    (EXTRACT(ISODOW FROM d.full_date) IN (6, 7)) AS is_weekend
FROM all_dates d
WHERE d.full_date IS NOT NULL;