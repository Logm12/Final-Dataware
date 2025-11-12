-- ========================================
-- 02_create_aggregates.sql
-- Create Aggregate Tables in DWH Schema
-- ========================================

DROP TABLE IF EXISTS dwh.agg_sales_monthly_category;
DROP TABLE IF EXISTS dwh.agg_profit_quarterly_market;
DROP TABLE IF EXISTS dwh.agg_sales_monthly_region;
DROP TABLE IF EXISTS dwh.agg_late_risk_monthly_shipping;
DROP TABLE IF EXISTS dwh.agg_sales_yearly_category;

-- ========================================
-- 1) Monthly Sales by Category
-- ========================================
CREATE TABLE dwh.agg_sales_monthly_category AS
SELECT 
    d.year,
    d.month_number,
    d.quarter_number,
    cat.category_name,
    dept.department_name,
    SUM(oi.sales) AS total_sales,
    SUM(oi.order_item_quantity) AS total_quantity,
    AVG(oi.order_item_profit_ratio) AS avg_profit_ratio,
    SUM(oi.order_item_profit) AS total_profit,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT oi.customer_key) AS unique_customers,
    AVG(oi.sales) AS avg_order_value
FROM dwh.order_items oi
JOIN dwh.dim_date d ON oi.order_date_key = d.date_key
JOIN dwh.dim_product p ON oi.product_key = p.product_key
JOIN dwh.dim_category cat ON p.category_key = cat.category_key
JOIN dwh.dim_department dept ON cat.department_key = dept.department_key
GROUP BY d.year, d.month_number, d.quarter_number, cat.category_name, dept.department_name;

CREATE INDEX idx_agg_monthly_cat ON dwh.agg_sales_monthly_category(year, month_number);

-- ========================================
-- 2) Quarterly Profit by Market
-- ========================================
CREATE TABLE dwh.agg_profit_quarterly_market AS
SELECT 
    d.year,
    d.quarter_number,
    geo.market,
    geo.region,
    SUM(oi.order_item_profit) AS total_profit,
    SUM(oi.sales) AS total_sales,
    AVG(CASE WHEN oi.sales = 0 THEN 0 ELSE oi.order_item_profit / oi.sales END) AS avg_profit_margin,
    COUNT(*) AS order_count
FROM dwh.order_items oi
JOIN dwh.dim_date d ON oi.order_date_key = d.date_key
JOIN dwh.dim_geography geo ON oi.order_geography_key = geo.geography_key
GROUP BY d.year, d.quarter_number, geo.market, geo.region;

CREATE INDEX idx_agg_quarterly_market ON dwh.agg_profit_quarterly_market(year, quarter_number);

-- ========================================
-- 3) Monthly Sales by Region
-- ========================================
CREATE TABLE dwh.agg_sales_monthly_region AS
SELECT 
    d.year,
    d.month_number,
    geo.region,
    geo.market,
    SUM(oi.sales) AS total_sales,
    SUM(oi.order_item_quantity) AS total_quantity,
    AVG(oi.sales) AS avg_sales,
    COUNT(*) AS order_count
FROM dwh.order_items oi
JOIN dwh.dim_date d ON oi.order_date_key = d.date_key
JOIN dwh.dim_geography geo ON oi.order_geography_key = geo.geography_key
GROUP BY d.year, d.month_number, geo.region, geo.market;

CREATE INDEX idx_agg_monthly_region ON dwh.agg_sales_monthly_region(year, month_number);

-- ========================================
-- 4) Late Delivery by Shipping Mode
-- ========================================
CREATE TABLE dwh.agg_late_risk_monthly_shipping AS
SELECT 
    d.year,
    d.month_number,
    ship.shipping_mode,
    ship.delivery_status,
    COUNT(CASE WHEN ship.delivery_risk = '1' THEN 1 END) AS late_delivery_count,
    COUNT(*) AS shipment_count,
    CAST(COUNT(CASE WHEN ship.delivery_risk = '1' THEN 1 END) AS NUMERIC) / NULLIF(COUNT(*), 0) AS late_delivery_percentage
FROM dwh.order_items oi
JOIN dwh.dim_date d ON oi.shipping_date_key = d.date_key
JOIN dwh.dim_shipping ship ON oi.shipping_key = ship.shipping_key
GROUP BY d.year, d.month_number, ship.shipping_mode, ship.delivery_status;

CREATE INDEX idx_agg_late_shipping ON dwh.agg_late_risk_monthly_shipping(year, month_number);

-- ========================================
-- 5) Yearly Sales by Category
-- ========================================
CREATE TABLE dwh.agg_sales_yearly_category AS
SELECT 
    d.year,
    cat.category_name,
    dept.department_name,
    SUM(oi.sales) AS total_sales,
    SUM(oi.order_item_profit) AS total_profit,
    COUNT(*) AS transaction_count
FROM dwh.order_items oi
JOIN dwh.dim_date d ON oi.order_date_key = d.date_key
JOIN dwh.dim_product p ON oi.product_key = p.product_key
JOIN dwh.dim_category cat ON p.category_key = cat.category_key
JOIN dwh.dim_department dept ON cat.department_key = dept.department_key
GROUP BY d.year, cat.category_name, dept.department_name;

CREATE INDEX idx_agg_yearly_cat ON dwh.agg_sales_yearly_category(year);

-- ========================================
-- Aggregate Summary View
-- ========================================
CREATE OR REPLACE VIEW dwh.v_aggregate_summary AS
SELECT 'agg_sales_monthly_category' AS table_name, COUNT(*) AS row_count FROM dwh.agg_sales_monthly_category
UNION ALL
SELECT 'agg_profit_quarterly_market', COUNT(*) FROM dwh.agg_profit_quarterly_market
UNION ALL
SELECT 'agg_sales_monthly_region', COUNT(*) FROM dwh.agg_sales_monthly_region
UNION ALL
SELECT 'agg_late_risk_monthly_shipping', COUNT(*) FROM dwh.agg_late_risk_monthly_shipping
UNION ALL
SELECT 'agg_sales_yearly_category', COUNT(*) FROM dwh.agg_sales_yearly_category;
