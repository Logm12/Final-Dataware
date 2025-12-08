"""
Data Validation Script
Author: Data Engineering Team
Date: 2025-12-03

This script validates the quality of the cleaned dataset.
"""

import pandas as pd
import numpy as np

def validate_cleaned_data(file_path):
    print("=" * 80)
    print(f"🔍 VALIDATING FILE: {file_path}")
    print("=" * 80)
    
    try:
        df = pd.read_csv(file_path, low_memory=False)
        print(f"✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return

    validation_results = {
        'passed': 0,
        'failed': 0,
        'warnings': 0
    }

    def check(name, condition, success_msg, fail_msg, is_warning=False):
        if condition:
            print(f"✅ [PASS] {name}: {success_msg}")
            validation_results['passed'] += 1
        else:
            if is_warning:
                print(f"⚠️ [WARN] {name}: {fail_msg}")
                validation_results['warnings'] += 1
            else:
                print(f"❌ [FAIL] {name}: {fail_msg}")
                validation_results['failed'] += 1

    print("\n1. INTEGRITY CHECKS")
    # Check 1: Duplicates
    duplicates = df.duplicated().sum()
    check("Duplicates", duplicates == 0, 
          "No duplicate rows found", 
          f"Found {duplicates:,} duplicate rows")

    # Check 2: Empty Columns
    empty_cols = [col for col in df.columns if df[col].isnull().all()]
    check("Empty Columns", len(empty_cols) == 0, 
          "No empty columns found", 
          f"Found empty columns: {empty_cols}")

    print("\n2. COMPLETENESS CHECKS")
    # Check 3: Critical Fields (should not be null)
    critical_fields = ['Order Id', 'Customer Id', 'Product Name', 'Sales']
    for field in critical_fields:
        if field in df.columns:
            null_count = df[field].isnull().sum()
            check(f"Critical Field '{field}'", null_count == 0, 
                  "No missing values", 
                  f"Found {null_count:,} missing values")

    print("\n3. DATA QUALITY CHECKS")
    # Check 4: Sensitive Data Masking
    sensitive_cols = ['Customer Email', 'Customer Password']
    for col in sensitive_cols:
        if col in df.columns:
            masked_correctly = not df[col].astype(str).str.contains('XXXXXXXXX').any()
            check(f"Masked '{col}'", masked_correctly, 
                  "No raw 'XXXXXXXXX' values found (converted to NaN/Null)", 
                  "Found raw 'XXXXXXXXX' values")

    # Check 5: Numeric Validity (Negative values)
    numeric_checks = ['Sales', 'Product Price', 'Order Item Quantity']
    for col in numeric_checks:
        if col in df.columns:
            negative_vals = (df[col] < 0).sum()
            check(f"Non-negative '{col}'", negative_vals == 0, 
                  "All values are non-negative", 
                  f"Found {negative_vals:,} negative values", is_warning=True)

    print("\n4. METADATA CHECKS")
    # Check 6: Data Quality Score
    if 'data_quality_score' in df.columns:
        avg_score = df['data_quality_score'].mean()
        check("Data Quality Score", avg_score >= 80, 
              f"Average score is high ({avg_score:.2f}/100)", 
              f"Average score is low ({avg_score:.2f}/100)", is_warning=True)
        
        perfect_records = (df['data_quality_score'] == 100).sum()
        print(f"   ℹ️  Perfect Records (100/100): {perfect_records:,} ({perfect_records/len(df)*100:.1f}%)")

    print("\n" + "=" * 80)
    print("📊 VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Passed:   {validation_results['passed']}")
    print(f"Failed:   {validation_results['failed']}")
    print(f"Warnings: {validation_results['warnings']}")
    
    if validation_results['failed'] == 0:
        print("\n✨ RESULT: DATA IS CLEAN AND READY FOR WAREHOUSE! ✨")
    else:
        print("\n🚫 RESULT: DATA NEEDS MORE ATTENTION")

if __name__ == "__main__":
    # Tự động tìm file cleaned mới nhất
    import glob
    import os
    
    list_of_files = glob.glob(r'd:\VSC_FINAL DWH\*_cleaned_*.csv') 
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        validate_cleaned_data(latest_file)
    else:
        print("No cleaned file found to validate.")
