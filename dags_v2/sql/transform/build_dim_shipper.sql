/*
Build dim_shipper from staging.stg_shippers
Source: shippers.csv
Replaces the old dim_shipping
*/
INSERT INTO dwh.dim_shipper (
    shipper_id,
    shipper_name,
    shipper_fname,
    shipper_lname,
    shipper_city,
    shipper_state,
    shipper_country,
    business_phone
)
SELECT DISTINCT 
    shipper_id,
    company AS shipper_name,
    first_name AS shipper_fname,
    last_name AS shipper_lname,
    city AS shipper_city,
    state_province AS shipper_state,
    country_region AS shipper_country,
    business_phone
FROM staging.stg_shippers
WHERE shipper_id IS NOT NULL;
