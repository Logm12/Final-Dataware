/*
Dọn dẹp TOÀN BỘ DWH trước khi build.
RESTART IDENTITY: Reset các cột SERIAL (xxx_key) về 1.
CASCADE: Tự động xóa các Foreign Key đang trỏ đến (từ bảng Fact),
         cho phép TRUNCATE các bảng Dim.
*/
TRUNCATE 
    dwh.dim_customer,
    dwh.dim_product,
    dwh.dim_category,
    dwh.dim_department,
    dwh.dim_date,
    dwh.dim_geography,
    dwh.dim_shipping,
    dwh.order_items
RESTART IDENTITY CASCADE;