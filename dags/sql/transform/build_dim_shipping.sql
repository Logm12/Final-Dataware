
INSERT INTO dwh.dim_shipping (
    shipping_mode,
    delivery_status,
    delivery_risk
)
SELECT DISTINCT
    shipping_mode,
    delivery_status,
    delivery_risk
FROM staging.stg_slms_shipments
WHERE shipping_mode IS NOT NULL
   OR delivery_status IS NOT NULL

   OR delivery_risk IS NOT NULL;
