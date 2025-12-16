INSERT INTO dwh.dim_customer (
    customer_id,
    customer_fname,
    customer_lname,
    customer_segment,
    customer_city,
    customer_state,
    customer_country
)
SELECT DISTINCT 
    customer_id,
    customer_fname,
    customer_lname,
    customer_segment,
    -- Dùng thông tin geography từ Bảng Orders (giả định là nơi ở của KH)
    order_city AS customer_city,
    order_state AS customer_state,
    order_country AS customer_country
FROM staging.stg_oms_orders;