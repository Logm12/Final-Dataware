/*
Lấy TẤT CẢ các địa điểm duy nhất từ các hệ thống
(UNION tự động loại bỏ trùng lặp)
*/
INSERT INTO dwh.dim_geography (
    city,
    state,
    country,
    latitude,
    longitude
    -- region, market (Không có trong staging, sẽ là NULL)
)
SELECT DISTINCT 
    order_city AS city, 
    order_state AS state, 
    order_country AS country,
    
    -- === SỬA LỖI TẠI ĐÂY ===
    -- Phải "ép kiểu" NULL sang đúng kiểu numeric
    -- để khớp với vế UNION thứ hai
    CAST(NULL AS DECIMAL(10,6)) AS latitude,
    CAST(NULL AS DECIMAL(10,6)) AS longitude
    -- === KẾT THÚC SỬA LỖI ===

FROM staging.stg_oms_orders
WHERE order_city IS NOT NULL

UNION

SELECT DISTINCT 
    ship_city AS city, 
    ship_state AS state, 
    ship_country AS country,
    latitude,
    longitude
FROM staging.stg_slms_shipments
WHERE ship_city IS NOT NULL;