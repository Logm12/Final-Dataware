# ========================================
# TEST CONNECTION & VALIDATE SETUP
# ========================================

param(
    [string]$DBHost = "localhost",
    [string]$DBPort = "5432",
    [string]$DBName = "dwh_demo",
    [string]$DBUser = "postgres",
    [string]$DBPassword = "postgres"
)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  AGGREGATE DEMO - VALIDATION TEST" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$tests = @()

# Test 1: PostgreSQL Connection
Write-Host "[1/7] Testing PostgreSQL connection..." -ForegroundColor Yellow
try {
    $env:PGPASSWORD = $DBPassword
    $result = psql -U $DBUser -h $DBHost -p $DBPort -d $DBName -c "SELECT version();" -t 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ PostgreSQL connected" -ForegroundColor Green
        $tests += @{Name="PostgreSQL Connection"; Status="PASS"}
    } else {
        throw "Connection failed"
    }
} catch {
    Write-Host "✗ PostgreSQL connection failed!" -ForegroundColor Red
    $tests += @{Name="PostgreSQL Connection"; Status="FAIL"}
}

# Test 2: Check Schemas
Write-Host "`n[2/7] Checking schemas..." -ForegroundColor Yellow
try {
    $schemas = psql -U $DBUser -h $DBHost -d $DBName -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('oms_oltp', 'slms_oltp', 'staging', 'dwh');" -t 2>&1
    $schemaCount = ($schemas | Measure-Object).Count
    
    if ($schemaCount -eq 4) {
        Write-Host "✓ All 4 schemas exist (oms_oltp, slms_oltp, staging, dwh)" -ForegroundColor Green
        $tests += @{Name="Schemas"; Status="PASS"}
    } else {
        Write-Host "⚠ Only $schemaCount/4 schemas found" -ForegroundColor Yellow
        $tests += @{Name="Schemas"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Schema check failed" -ForegroundColor Red
    $tests += @{Name="Schemas"; Status="FAIL"}
}

# Test 3: Check Dimension Tables
Write-Host "`n[3/7] Checking dimension tables..." -ForegroundColor Yellow
try {
    $dimTables = @("dim_customer", "dim_product", "dim_category", "dim_department", "dim_date", "dim_geography", "dim_shipping")
    $foundTables = 0
    
    foreach ($table in $dimTables) {
        $check = psql -U $DBUser -h $DBHost -d $DBName -c "SELECT COUNT(*) FROM dwh.$table;" -t 2>&1
        if ($LASTEXITCODE -eq 0) {
            $foundTables++
        }
    }
    
    if ($foundTables -eq 7) {
        Write-Host "✓ All 7 dimension tables exist" -ForegroundColor Green
        $tests += @{Name="Dimension Tables"; Status="PASS"}
    } else {
        Write-Host "⚠ Only $foundTables/7 dimension tables found" -ForegroundColor Yellow
        $tests += @{Name="Dimension Tables"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Dimension table check failed" -ForegroundColor Red
    $tests += @{Name="Dimension Tables"; Status="FAIL"}
}

# Test 4: Check Fact Table
Write-Host "`n[4/7] Checking fact table..." -ForegroundColor Yellow
try {
    $factCount = psql -U $DBUser -h $DBHost -d $DBName -c "SELECT COUNT(*) FROM dwh.order_items;" -t 2>&1
    $factCount = $factCount.Trim()
    
    if ([int]$factCount -gt 0) {
        Write-Host "✓ Fact table exists with $factCount rows" -ForegroundColor Green
        $tests += @{Name="Fact Table"; Status="PASS"}
    } else {
        Write-Host "⚠ Fact table exists but empty" -ForegroundColor Yellow
        $tests += @{Name="Fact Table"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Fact table check failed" -ForegroundColor Red
    $tests += @{Name="Fact Table"; Status="FAIL"}
}

# Test 5: Check Aggregate Tables
Write-Host "`n[5/7] Checking aggregate tables..." -ForegroundColor Yellow
try {
    $aggSummary = psql -U $DBUser -h $DBHost -d $DBName -c "SELECT * FROM dwh.v_aggregate_summary;" -t 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Aggregate tables validated:" -ForegroundColor Green
        Write-Host $aggSummary -ForegroundColor Cyan
        $tests += @{Name="Aggregate Tables"; Status="PASS"}
    } else {
        Write-Host "⚠ Aggregates not found or empty" -ForegroundColor Yellow
        $tests += @{Name="Aggregate Tables"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Aggregate check failed" -ForegroundColor Red
    $tests += @{Name="Aggregate Tables"; Status="FAIL"}
}

# Test 6: Performance Test
Write-Host "`n[6/7] Running performance test..." -ForegroundColor Yellow
try {
    $query = "SELECT year, month_number, SUM(total_sales) FROM dwh.agg_sales_monthly_category WHERE year = 2018 GROUP BY year, month_number;"
    
    $start = Get-Date
    $result = psql -U $DBUser -h $DBHost -d $DBName -c $query -t 2>&1
    $end = Get-Date
    
    $duration = ($end - $start).TotalMilliseconds
    
    if ($duration -lt 1000) {
        Write-Host "✓ Query executed in $([math]::Round($duration, 2))ms (< 1 second)" -ForegroundColor Green
        $tests += @{Name="Performance Test"; Status="PASS"}
    } else {
        Write-Host "⚠ Query took $([math]::Round($duration, 2))ms (> 1 second)" -ForegroundColor Yellow
        $tests += @{Name="Performance Test"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Performance test failed" -ForegroundColor Red
    $tests += @{Name="Performance Test"; Status="FAIL"}
}

# Test 7: Python Environment
Write-Host "`n[7/7] Checking Python environment..." -ForegroundColor Yellow
try {
    $pythonCheck = python --version 2>&1
    $pandasCheck = python -c "import pandas; print('OK')" 2>&1
    $sqlalchemyCheck = python -c "import sqlalchemy; print('OK')" 2>&1
    
    if ($pandasCheck -like "*OK*" -and $sqlalchemyCheck -like "*OK*") {
        Write-Host "✓ Python environment ready" -ForegroundColor Green
        $tests += @{Name="Python Environment"; Status="PASS"}
    } else {
        Write-Host "⚠ Some Python packages missing" -ForegroundColor Yellow
        $tests += @{Name="Python Environment"; Status="WARN"}
    }
} catch {
    Write-Host "✗ Python check failed" -ForegroundColor Red
    $tests += @{Name="Python Environment"; Status="FAIL"}
}

# Summary Report
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  VALIDATION SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$passCount = ($tests | Where-Object {$_.Status -eq "PASS"}).Count
$warnCount = ($tests | Where-Object {$_.Status -eq "WARN"}).Count
$failCount = ($tests | Where-Object {$_.Status -eq "FAIL"}).Count
$totalTests = $tests.Count

Write-Host "`nResults: $passCount PASS | $warnCount WARN | $failCount FAIL (Total: $totalTests)" -ForegroundColor White

foreach ($test in $tests) {
    $color = switch ($test.Status) {
        "PASS" { "Green" }
        "WARN" { "Yellow" }
        "FAIL" { "Red" }
    }
    Write-Host "  [$($test.Status)] $($test.Name)" -ForegroundColor $color
}

if ($failCount -eq 0 -and $warnCount -eq 0) {
    Write-Host "`n🎉 ALL TESTS PASSED! Demo is ready to use." -ForegroundColor Green
} elseif ($failCount -eq 0) {
    Write-Host "`n⚠ Setup complete with warnings. Review above." -ForegroundColor Yellow
} else {
    Write-Host "`n✗ Setup incomplete. Fix errors above." -ForegroundColor Red
}

Write-Host "========================================`n" -ForegroundColor Cyan
