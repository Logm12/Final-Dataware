# ========================================
# QUICKSTART: Chạy Demo trong 5 Phút
# ========================================

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "  AGGREGATE BUILDER DEMO - QUICKSTART" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# Step 1: Check Prerequisites
Write-Host "`n[1/6] Checking prerequisites..." -ForegroundColor Yellow

$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Python not found! Please install Python 3.8+" -ForegroundColor Red
    exit 1
}
Write-Host "✓ $pythonVersion" -ForegroundColor Green

# Check PostgreSQL
$pgCheck = docker ps --filter "name=postgres" --format "{{.Names}}" 2>&1
if ($pgCheck -notlike "*postgres*") {
    Write-Host "⚠ PostgreSQL container not running. Starting..." -ForegroundColor Yellow
    docker run --name postgres-dwh `
        -e POSTGRES_PASSWORD=postgres `
        -e POSTGRES_DB=dwh_demo `
        -p 5432:5432 `
        -d postgres:15
    Start-Sleep -Seconds 10
}
Write-Host "✓ PostgreSQL running" -ForegroundColor Green

# Step 2: Create Virtual Environment
Write-Host "`n[2/6] Setting up Python environment..." -ForegroundColor Yellow

if (!(Test-Path "venv")) {
    python -m venv venv
    Write-Host "✓ Virtual environment created" -ForegroundColor Green
} else {
    Write-Host "✓ Virtual environment exists" -ForegroundColor Green
}

# Activate venv
& ".\venv\Scripts\Activate.ps1"

# Step 3: Install Dependencies
Write-Host "`n[3/6] Installing dependencies..." -ForegroundColor Yellow
pip install -q -r requirements.txt
Write-Host "✓ Dependencies installed" -ForegroundColor Green

# Step 4: Initialize Database
Write-Host "`n[4/6] Initializing database schemas..." -ForegroundColor Yellow

# Wait for PostgreSQL to be ready
Start-Sleep -Seconds 3

$env:PGPASSWORD = "postgres"
psql -U postgres -h localhost -d dwh_demo -f sql/01_init_hybrid_schema.sql 2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Database schemas created" -ForegroundColor Green
} else {
    Write-Host "⚠ Schema creation had warnings (might already exist)" -ForegroundColor Yellow
}

# Step 5: Run ETL Pipeline
Write-Host "`n[5/6] Running ETL pipeline..." -ForegroundColor Yellow
Write-Host "This may take 2-5 minutes depending on your machine..." -ForegroundColor Gray

cd etl
python hybrid_aggregate_builder.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✓ ETL completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`n✗ ETL failed. Check logs above." -ForegroundColor Red
    exit 1
}

cd ..

# Step 6: Validate Results
Write-Host "`n[6/6] Validating aggregates..." -ForegroundColor Yellow

$validateSQL = @"
SELECT 
    table_name,
    row_count,
    CASE 
        WHEN row_count > 0 THEN '✓ OK'
        ELSE '✗ EMPTY'
    END AS status
FROM dwh.v_aggregate_summary
ORDER BY row_count DESC;
"@

$env:PGPASSWORD = "postgres"
$result = psql -U postgres -h localhost -d dwh_demo -c $validateSQL -t

Write-Host "`n$result" -ForegroundColor Cyan

# Success Message
Write-Host "`n=====================================" -ForegroundColor Green
Write-Host "  🎉 DEMO SETUP COMPLETE! 🎉" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green

Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. Open Power BI Desktop" -ForegroundColor White
Write-Host "  2. Get Data → PostgreSQL Database" -ForegroundColor White
Write-Host "  3. Server: localhost, Database: dwh_demo" -ForegroundColor White
Write-Host "  4. Load tables: dwh.agg_*" -ForegroundColor White
Write-Host "`n  Or run sample queries:" -ForegroundColor Yellow
Write-Host "  psql -U postgres -h localhost -d dwh_demo" -ForegroundColor White

Write-Host "`n📖 Full documentation: README.md" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Green
