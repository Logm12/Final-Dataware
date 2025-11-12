# Validation Script for Demo Results
Write-Host ""
Write-Host "========================================"
Write-Host "VALIDATING DEMO RESULTS"
Write-Host "========================================" 
Write-Host ""

# 1. Check container
Write-Host "[1/5] Checking Docker container..."
docker ps --filter "name=jovial_mayer" --format "table {{.Names}}\t{{.Status}}"
Write-Host ""

# 2. Check database
Write-Host "[2/5] Checking database..."
docker exec jovial_mayer psql -U root -c "\l" | Select-String "dwh_demo"
Write-Host ""

# 3. Check dimensions
Write-Host "[3/5] Dimension tables row count:"
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT 'dim_date' AS tbl, COUNT(*) FROM dwh.dim_date UNION ALL SELECT 'dim_product', COUNT(*) FROM dwh.dim_product UNION ALL SELECT 'dim_customer', COUNT(*) FROM dwh.dim_customer UNION ALL SELECT 'dim_category', COUNT(*) FROM dwh.dim_category;" | Select-Object -Last 8
Write-Host ""

# 4. Check fact table
Write-Host "[4/5] Fact table row count:"
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT COUNT(*) AS fact_rows FROM dwh.order_items;"
Write-Host ""

# 5. Check aggregates
Write-Host "[5/5] Aggregate tables:"
docker exec jovial_mayer psql -U root -d dwh_demo -c "SELECT * FROM dwh.v_aggregate_summary ORDER BY table_name;"
Write-Host ""

Write-Host "========================================" -ForegroundColor Green
Write-Host "View detailed results in DEMO_RESULTS.md"
Write-Host "========================================" -ForegroundColor Green
