INSERT INTO dwh.dim_product (
    product_id,
    product_name,
    product_card_id,
    product_price,
    -- product_image (Không có trong staging)
    category_key
)
SELECT DISTINCT
    s.product_id,
    s.product_name,
    s.product_card_id,
    s.product_price,
    c.category_key -- Lấy khóa surrogate từ dim_category
FROM staging.stg_oms_order_items s
LEFT JOIN dwh.dim_category c 
    ON s.category_id = c.category_id
WHERE s.product_id IS NOT NULL;