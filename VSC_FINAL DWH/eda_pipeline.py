"""
Supply Chain Analysis & Architecture Splitter
Author: Data Engineering Team
Date: 2025-12-03

This script performs:
1. Dataset Description & Statistics
2. Splitting data into Sales Service and Logistics Service
3. Basic Exploratory Data Analysis (EDA)
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_and_split(file_path):
    print("=" * 80)
    print("🚀 STARTING DATA ANALYSIS AND ARCHITECTURE SPLIT")
    print("=" * 80)

    # 1. Load Data
    try:
        df = pd.read_csv(file_path, low_memory=False)
        print(f"✓ Loaded data: {len(df):,} rows")
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return

    # ==============================================================================
    # PART 1: DATASET DESCRIPTION
    # ==============================================================================
    print("\n📊 PART 1: DATASET DESCRIPTION")
    
    desc_stats = df.describe().transpose()
    cat_stats = df.describe(include='object').transpose()
    
    print(f"   - Numerical Columns: {len(desc_stats)}")
    print(f"   - Categorical Columns: {len(cat_stats)}")
    
    # ==============================================================================
    # PART 2: SPLITTING INTO 2 SERVICES (Microservices Architecture)
    # ==============================================================================
    print("\n🏗️  PART 2: SPLITTING INTO MICROSERVICES")
    
    # Service 1: Sales & Order Management Service (OMS)
    # Focus: Customers, Products, Sales, Financials
    oms_columns = [
        'Order Id', 'order date (DateOrders)', 'Order Status', 
        'Customer Id', 'Customer Fname', 'Customer Lname', 'Customer Segment', 
        'Customer City', 'Customer Country', 'Market', 'Order Region',
        'Product Card Id', 'Product Name', 'Category Name', 'Product Price',
        'Sales', 'Order Item Quantity', 'Order Item Discount', 'Order Item Total',
        'Benefit per order', 'Order Profit Per Order'
    ]
    
    # Filter columns that actually exist
    oms_cols_final = [c for c in oms_columns if c in df.columns]
    df_oms = df[oms_cols_final].copy()
    
    # Service 2: Logistics & Fulfillment Service (LFS)
    # Focus: Shipping, Delivery, Location, Risk
    logistics_columns = [
        'Order Id', # Foreign Key to link with OMS
        'shipping date (DateOrders)', 'Shipping Mode',
        'Days for shipping (real)', 'Days for shipment (scheduled)',
        'Delivery Status', 'Late_delivery_risk',
        'Latitude', 'Longitude', 'Order City', 'Order Country', 'Order State'
    ]
    
    # Filter columns that actually exist
    logistics_cols_final = [c for c in logistics_columns if c in df.columns]
    df_logistics = df[logistics_cols_final].copy()
    
    # Save split files
    oms_file = file_path.replace('.csv', '_SERVICE_SALES.csv')
    logistics_file = file_path.replace('.csv', '_SERVICE_LOGISTICS.csv')
    
    df_oms.to_csv(oms_file, index=False)
    df_logistics.to_csv(logistics_file, index=False)
    
    print(f"   ✓ Created Sales Service Data: {oms_file} ({len(df_oms.columns)} cols)")
    print(f"   ✓ Created Logistics Service Data: {logistics_file} ({len(df_logistics.columns)} cols)")

    # ==============================================================================
    # PART 3: EXPLORATORY DATA ANALYSIS (EDA)
    # ==============================================================================
    print("\n🔍 PART 3: EXPLORATORY DATA ANALYSIS (EDA)")
    
    eda_report = []
    eda_report.append("# 📊 Supply Chain EDA Report\n")
    
    # 3.1 Sales Performance
    total_sales = df['Sales'].sum()
    avg_order_value = df['Sales'].mean()
    total_profit = df['Benefit per order'].sum()
    
    eda_report.append("## 1. Sales Performance")
    eda_report.append(f"- **Total Revenue:** ${total_sales:,.2f}")
    eda_report.append(f"- **Total Profit:** ${total_profit:,.2f}")
    eda_report.append(f"- **Avg Order Value:** ${avg_order_value:,.2f}")
    
    # Top 5 Products
    top_products = df.groupby('Product Name')['Sales'].sum().sort_values(ascending=False).head(5)
    eda_report.append("\n### Top 5 Best Selling Products")
    for product, sales in top_products.items():
        eda_report.append(f"- {product}: ${sales:,.2f}")

    # 3.2 Logistics Performance
    eda_report.append("\n## 2. Logistics Performance")
    
    if 'Delivery Status' in df.columns:
        status_counts = df['Delivery Status'].value_counts(normalize=True) * 100
        eda_report.append("\n### Delivery Status Distribution")
        for status, pct in status_counts.items():
            eda_report.append(f"- {status}: {pct:.2f}%")
            
    if 'Late_delivery_risk' in df.columns:
        late_risk = df['Late_delivery_risk'].mean() * 100
        eda_report.append(f"\n- **Overall Late Delivery Risk:** {late_risk:.2f}%")

    # 3.3 Market Analysis
    eda_report.append("\n## 3. Market Analysis")
    top_markets = df.groupby('Market')['Sales'].sum().sort_values(ascending=False)
    eda_report.append("\n### Sales by Market")
    for market, sales in top_markets.items():
        eda_report.append(f"- {market}: ${sales:,.2f}")

    # Save EDA Report
    report_path = os.path.join(os.path.dirname(file_path), 'EDA_REPORT.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(eda_report))
        
    print(f"   ✓ Generated EDA Report: {report_path}")
    print("\n" + "=" * 80)
    print("✅ ANALYSIS COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    # Find the latest cleaned file
    import glob
    list_of_files = glob.glob(r'd:\VSC_FINAL DWH\*_cleaned_*.csv') 
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        analyze_and_split(latest_file)
    else:
        print("No cleaned file found.")
