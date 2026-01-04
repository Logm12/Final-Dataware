/*
Build dim_customer from staging.stg_customers
Source: customer.csv
*/
INSERT INTO dwh.dim_customer (
    customer_id,
    customer_fname,
    customer_lname,
    company,
    email_address,
    job_title,
    customer_city,
    customer_state,
    customer_country
)
SELECT DISTINCT 
    customer_id,
    first_name AS customer_fname,
    last_name AS customer_lname,
    company,
    email_address,
    job_title,
    city AS customer_city,
    state_province AS customer_state,
    country_region AS customer_country
FROM staging.stg_customers
WHERE customer_id IS NOT NULL;