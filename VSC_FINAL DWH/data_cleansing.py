"""
Data Cleansing Script for Supply Chain Data Warehouse
Author: Data Engineering Team
Date: 2025-12-03

This script performs comprehensive data cleansing on the Supply Chain dataset
to prepare it for Data Warehouse loading.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class SupplyChainDataCleaner:
    """
    A comprehensive data cleansing class for Supply Chain data
    """
    
    def __init__(self, input_file):
        """
        Initialize the cleaner with input file path
        
        Args:
            input_file (str): Path to the input CSV file
        """
        self.input_file = input_file
        self.df = None
        self.cleaning_report = {
            'total_rows_original': 0,
            'total_rows_cleaned': 0,
            'duplicates_removed': 0,
            'missing_values_handled': 0,
            'invalid_values_fixed': 0,
            'columns_dropped': []
        }
    
    def load_data(self):
        """Load data from CSV file"""
        print("=" * 80)
        print("STEP 1: LOADING DATA")
        print("=" * 80)
        
        try:
            self.df = pd.read_csv(self.input_file, encoding='utf-8', low_memory=False)
            self.cleaning_report['total_rows_original'] = len(self.df)
            print(f"✓ Successfully loaded {len(self.df)} rows and {len(self.df.columns)} columns")
            print(f"✓ File: {self.input_file}")
            return True
        except Exception as e:
            print(f"✗ Error loading file: {e}")
            return False
    
    def explore_data(self):
        """Explore and display data quality issues"""
        print("\n" + "=" * 80)
        print("STEP 2: DATA EXPLORATION")
        print("=" * 80)
        
        print("\n📊 Dataset Shape:")
        print(f"   Rows: {self.df.shape[0]:,}")
        print(f"   Columns: {self.df.shape[1]}")
        
        print("\n📋 Column Names:")
        for i, col in enumerate(self.df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        print("\n🔍 Missing Values:")
        missing = self.df.isnull().sum()
        missing_pct = (missing / len(self.df) * 100).round(2)
        missing_df = pd.DataFrame({
            'Column': missing.index,
            'Missing Count': missing.values,
            'Missing %': missing_pct.values
        })
        missing_df = missing_df[missing_df['Missing Count'] > 0].sort_values('Missing Count', ascending=False)
        
        if len(missing_df) > 0:
            print(missing_df.to_string(index=False))
        else:
            print("   ✓ No missing values found!")
        
        print("\n🔢 Data Types:")
        dtype_counts = self.df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"   {dtype}: {count} columns")
        
        print("\n📦 Duplicate Rows:")
        duplicates = self.df.duplicated().sum()
        print(f"   Total duplicates: {duplicates:,}")
        
        return missing_df
    
    def remove_empty_columns(self):
        """Remove completely empty or unnamed columns"""
        print("\n" + "=" * 80)
        print("STEP 3: REMOVING EMPTY/UNNAMED COLUMNS")
        print("=" * 80)
        
        # Remove columns with empty names
        empty_name_cols = [col for col in self.df.columns if str(col).strip() == '' or 'Unnamed' in str(col)]
        
        # Remove columns that are completely null
        null_cols = self.df.columns[self.df.isnull().all()].tolist()
        
        cols_to_drop = list(set(empty_name_cols + null_cols))
        
        if cols_to_drop:
            self.df.drop(columns=cols_to_drop, inplace=True)
            self.cleaning_report['columns_dropped'].extend(cols_to_drop)
            print(f"✓ Removed {len(cols_to_drop)} empty/unnamed columns:")
            for col in cols_to_drop:
                print(f"   - {col}")
        else:
            print("✓ No empty columns to remove")
    
    def remove_duplicates(self):
        """Remove duplicate rows"""
        print("\n" + "=" * 80)
        print("STEP 4: REMOVING DUPLICATES")
        print("=" * 80)
        
        before = len(self.df)
        self.df.drop_duplicates(inplace=True)
        after = len(self.df)
        removed = before - after
        
        self.cleaning_report['duplicates_removed'] = removed
        
        if removed > 0:
            print(f"✓ Removed {removed:,} duplicate rows")
            print(f"✓ Remaining rows: {after:,}")
        else:
            print("✓ No duplicates found")
    
    def clean_customer_data(self):
        """Clean customer-related columns"""
        print("\n" + "=" * 80)
        print("STEP 5: CLEANING CUSTOMER DATA")
        print("=" * 80)
        
        # Replace XXXXXXXXX with NULL in sensitive fields
        sensitive_cols = ['Customer Email', 'Customer Password']
        for col in sensitive_cols:
            if col in self.df.columns:
                mask = self.df[col] == 'XXXXXXXXX'
                count = mask.sum()
                if count > 0:
                    self.df.loc[mask, col] = np.nan
                    print(f"✓ Masked {count:,} values in '{col}'")
        
        # Clean customer names - remove extra spaces
        name_cols = ['Customer Fname', 'Customer Lname']
        for col in name_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].str.strip()
                print(f"✓ Cleaned '{col}' - removed extra spaces")
    
    def clean_numeric_columns(self):
        """Clean and validate numeric columns"""
        print("\n" + "=" * 80)
        print("STEP 6: CLEANING NUMERIC DATA")
        print("=" * 80)
        
        # Define expected numeric columns
        numeric_cols = [
            'Days for shipping (real)', 'Days for shipment (scheduled)',
            'Benefit per order', 'Sales per customer', 'Order Item Discount',
            'Order Item Discount Rate', 'Order Item Product Price',
            'Order Item Profit Ratio', 'Order Item Quantity', 'Sales',
            'Order Item Total', 'Order Profit Per Order', 'Product Price',
            'Latitude', 'Longitude'
        ]
        
        for col in numeric_cols:
            if col in self.df.columns:
                # Convert to numeric, coercing errors
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                
                # Check for negative values in columns that shouldn't have them
                if col in ['Days for shipping (real)', 'Days for shipment (scheduled)', 
                          'Order Item Quantity', 'Sales', 'Product Price']:
                    negative_count = (self.df[col] < 0).sum()
                    if negative_count > 0:
                        print(f"⚠ Warning: {negative_count:,} negative values in '{col}'")
                        # Optionally replace with absolute value or null
                        # self.df.loc[self.df[col] < 0, col] = np.nan
        
        print("✓ Numeric columns validated and converted")
    
    def clean_date_columns(self):
        """Clean and standardize date columns"""
        print("\n" + "=" * 80)
        print("STEP 7: CLEANING DATE DATA")
        print("=" * 80)
        
        date_cols = ['order date (DateOrders)', 'shipping date (DateOrders)']
        
        for col in date_cols:
            if col in self.df.columns:
                try:
                    # Convert to datetime
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    
                    # Check for invalid dates
                    invalid_count = self.df[col].isnull().sum()
                    if invalid_count > 0:
                        print(f"⚠ Warning: {invalid_count:,} invalid dates in '{col}'")
                    else:
                        print(f"✓ Cleaned '{col}' - all dates valid")
                except Exception as e:
                    print(f"✗ Error cleaning '{col}': {e}")
    
    def clean_categorical_columns(self):
        """Clean categorical columns"""
        print("\n" + "=" * 80)
        print("STEP 8: CLEANING CATEGORICAL DATA")
        print("=" * 80)
        
        # Trim whitespace from all string columns
        string_cols = self.df.select_dtypes(include=['object']).columns
        
        for col in string_cols:
            if col not in ['Customer Email', 'Customer Password']:  # Skip already processed
                self.df[col] = self.df[col].astype(str).str.strip()
        
        print(f"✓ Cleaned {len(string_cols)} categorical columns")
        
        # Standardize specific columns
        if 'Delivery Status' in self.df.columns:
            unique_statuses = self.df['Delivery Status'].unique()
            print(f"\n📋 Delivery Status values: {len(unique_statuses)}")
            for status in sorted(unique_statuses):
                count = (self.df['Delivery Status'] == status).sum()
                print(f"   - {status}: {count:,}")
    
    def handle_missing_values(self):
        """Handle missing values based on column type"""
        print("\n" + "=" * 80)
        print("STEP 9: HANDLING MISSING VALUES")
        print("=" * 80)
        
        missing_before = self.df.isnull().sum().sum()
        
        # For numeric columns, you might want to fill with median or mean
        # For categorical, fill with mode or 'Unknown'
        # For this example, we'll just report them
        
        missing_summary = self.df.isnull().sum()
        missing_summary = missing_summary[missing_summary > 0].sort_values(ascending=False)
        
        if len(missing_summary) > 0:
            print("📊 Missing values summary:")
            for col, count in missing_summary.items():
                pct = (count / len(self.df) * 100)
                print(f"   - {col}: {count:,} ({pct:.2f}%)")
        else:
            print("✓ No missing values found")
        
        self.cleaning_report['missing_values_handled'] = missing_before
    
    def add_data_quality_flags(self):
        """Add quality flags for data warehouse"""
        print("\n" + "=" * 80)
        print("STEP 10: ADDING DATA QUALITY FLAGS")
        print("=" * 80)
        
        # Add a data quality score column
        self.df['data_quality_score'] = 100
        
        # Reduce score for missing critical fields
        critical_fields = ['Order Id', 'Customer Id', 'Product Name', 'Sales']
        for field in critical_fields:
            if field in self.df.columns:
                self.df.loc[self.df[field].isnull(), 'data_quality_score'] -= 25
        
        # Add cleansing timestamp
        self.df['cleansing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Add record source
        self.df['record_source'] = 'DataCoSupplyChain'
        
        print("✓ Added data quality flags:")
        print("   - data_quality_score")
        print("   - cleansing_timestamp")
        print("   - record_source")
        
        # Show quality distribution
        quality_dist = self.df['data_quality_score'].value_counts().sort_index(ascending=False)
        print("\n📊 Data Quality Score Distribution:")
        for score, count in quality_dist.items():
            pct = (count / len(self.df) * 100)
            print(f"   Score {score}: {count:,} records ({pct:.2f}%)")
    
    def save_cleaned_data(self, output_file=None):
        """Save cleaned data to CSV"""
        print("\n" + "=" * 80)
        print("STEP 11: SAVING CLEANED DATA")
        print("=" * 80)
        
        if output_file is None:
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = self.input_file.replace('.csv', f'_cleaned_{timestamp}.csv')
        
        try:
            self.df.to_csv(output_file, index=False, encoding='utf-8')
            self.cleaning_report['total_rows_cleaned'] = len(self.df)
            print(f"✓ Cleaned data saved to: {output_file}")
            print(f"✓ Total rows: {len(self.df):,}")
            print(f"✓ Total columns: {len(self.df.columns)}")
            return output_file
        except Exception as e:
            print(f"✗ Error saving file: {e}")
            return None
    
    def generate_report(self):
        """Generate and display cleaning report"""
        print("\n" + "=" * 80)
        print("DATA CLEANSING REPORT")
        print("=" * 80)
        
        print(f"\n📊 Summary:")
        print(f"   Original rows: {self.cleaning_report['total_rows_original']:,}")
        print(f"   Cleaned rows: {self.cleaning_report['total_rows_cleaned']:,}")
        print(f"   Rows removed: {self.cleaning_report['total_rows_original'] - self.cleaning_report['total_rows_cleaned']:,}")
        print(f"   Duplicates removed: {self.cleaning_report['duplicates_removed']:,}")
        print(f"   Missing values handled: {self.cleaning_report['missing_values_handled']:,}")
        
        if self.cleaning_report['columns_dropped']:
            print(f"\n🗑️  Columns dropped: {len(self.cleaning_report['columns_dropped'])}")
            for col in self.cleaning_report['columns_dropped']:
                print(f"   - {col}")
        
        print(f"\n✅ Data cleansing completed successfully!")
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def run_full_cleansing(self, output_file=None):
        """Run the complete cleansing pipeline"""
        print("\n" + "🔧" * 40)
        print("SUPPLY CHAIN DATA CLEANSING PIPELINE")
        print("🔧" * 40)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Execute all steps
        if not self.load_data():
            return None
        
        self.explore_data()
        self.remove_empty_columns()
        self.remove_duplicates()
        self.clean_customer_data()
        self.clean_numeric_columns()
        self.clean_date_columns()
        self.clean_categorical_columns()
        self.handle_missing_values()
        self.add_data_quality_flags()
        output_path = self.save_cleaned_data(output_file)
        self.generate_report()
        
        print("\n" + "🔧" * 40)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔧" * 40 + "\n")
        
        return output_path


def main():
    """Main execution function"""
    # Input file path
    input_file = r'd:\VSC_FINAL DWH\DataCoSupplyChainDataset_4k_with_13cols.csv'
    
    # Optional: specify output file path
    # output_file = r'd:\VSC_FINAL DWH\DataCoSupplyChain_cleaned.csv'
    output_file = None  # Auto-generate filename with timestamp
    
    # Create cleaner instance
    cleaner = SupplyChainDataCleaner(input_file)
    
    # Run full cleansing pipeline
    cleaned_file = cleaner.run_full_cleansing(output_file)
    
    if cleaned_file:
        print(f"\n✅ SUCCESS! Cleaned data is ready for Data Warehouse loading.")
        print(f"📁 File location: {cleaned_file}")
    else:
        print(f"\n❌ FAILED! Please check the errors above.")


if __name__ == "__main__":
    main()
