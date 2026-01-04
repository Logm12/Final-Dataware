/*
Build dim_employee from staging.stg_employees
Source: employees.csv
Replaces the old dim_department
*/
INSERT INTO dwh.dim_employee (
    employee_id,
    employee_fname,
    employee_lname,
    company,
    email_address,
    job_title,
    employee_city,
    employee_state,
    employee_country
)
SELECT DISTINCT 
    employee_id,
    first_name AS employee_fname,
    last_name AS employee_lname,
    company,
    email_address,
    job_title,
    city AS employee_city,
    state_province AS employee_state,
    country_region AS employee_country
FROM staging.stg_employees
WHERE employee_id IS NOT NULL;
