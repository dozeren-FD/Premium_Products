#!/usr/bin/env python
"""
Premium Products Analysis Script
Identifies premium products based on statistical price analysis within product categories
"""

import pandas as pd
import numpy as np
import connectorx as cx
from datetime import datetime
import sys
from openpyxl.utils import get_column_letter


class PremiumProductAnalyzer:
    def __init__(self, db_config):
        """Initialize with database configuration"""
        self.db_url = f"oracle://{db_config['user']}:{db_config['password']}@{db_config['dsn']}"
        pd.set_option('display.max_columns', None)
        pd.options.mode.chained_assignment = None
        
    def batch_query_with_keys(self, query_template, key_list, batch_size=1000):
        """Execute query in batches for a list of keys"""
        result_dfs = []
        num_batches = len(key_list) // batch_size + (1 if len(key_list) % batch_size > 0 else 0)
        
        print(f"Processing {len(key_list)} keys in {num_batches} batch(es)")
        
        for i in range(num_batches):
            try:
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(key_list))
                batch_keys = key_list[start_idx:end_idx]
                
                if not batch_keys:
                    continue
                
                print(f"Processing batch {i+1}/{num_batches} ({len(batch_keys)} keys)...")
                
                # Format keys for SQL IN clause
                formatted_keys = ", ".join([f"'{key}'" for key in batch_keys])
                query = query_template.replace(":client_keys", f"({formatted_keys})")
                
                # Execute query
                batch_df = cx.read_sql(self.db_url, query, return_type="pandas")
                print(f"Batch {i+1} completed: {batch_df.shape[0]} rows")
                
                result_dfs.append(batch_df)
                
            except Exception as e:
                print(f"Error in batch {i+1}: {str(e)}")
                continue
        
        # Combine results
        if result_dfs:
            final_df = pd.concat(result_dfs, ignore_index=True)
            print(f"Total rows collected: {final_df.shape[0]}")
            return final_df
        else:
            print("No data collected")
            return pd.DataFrame()
    
    def get_main_data(self, test_mode=False, test_category=None, row_limit=None):
        """
        Get main product data with dynamic date range
        
        Args:
            test_mode: If True, adds filters for testing
            test_category: Specific TIER3 category to test with
            row_limit: Limit number of rows returned (for testing)
        """
        base_query = '''
        SELECT
            olf.MATERIAL_NUMBER,
            pmd.TIER1,
            pmd.TIER2,
            pmd.TIER3,
            pmd.TIER4,
            AVG(olf.INV_ACTUAL_PRICE) as AVG_PRICE,
            AVG(CASE WHEN pd.pricing_unit = 'LB' 
                THEN olf.inv_actual_weight 
                ELSE olf.inv_actual_quantity END) as UNITS,
            AVG(INV_ACTUAL_WEIGHT) as AVG_WEIGHT,
            CASE
                WHEN SUM(INV_ACTUAL_WEIGHT) = 0 OR SUM(INV_ACTUAL_WEIGHT) IS NULL
                THEN 0
                ELSE SUM(olf.INV_ACTUAL_PRICE) / SUM(INV_ACTUAL_WEIGHT)
            END as WEIGHTED_AVG_PRICE
            --MAX(pmld.UNRESTRICTED_USE_INVENTORY) as MAX_INVENTORY,
            --MAX(pmld.UNRESTRICTED_USE_INVENTORY_CS) as MAX_INVENTORY_CS,
            --MAX(pmld.OOS_CURRENT_STATUS) as OOS_STATUS,
            --MAX(pmld.DAYS_ON_HAND) as MAX_DAYS_ON_HAND
        FROM fd_dw.DW_OLF_L2Y olf
            INNER JOIN fd_dw.product_material_dim pmd
                ON pmd.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                AND olf.plant = pmd.plant
                AND olf.sales_org = pmd.sales_org
                AND olf.distribution_channel = pmd.distribution_channel
            INNER JOIN fd_dw.PRODUCT_DIM pd
                ON olf.product_key = pd.PRODUCT_KEY
            INNER JOIN FD_DW.PRODUCT_MATERIAL_LIVE_DIM pmld
                ON pmld.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                AND olf.PLANT = pmld.PLANT
                AND olf.sales_org = pmld.sales_org
                AND olf.distribution_channel = pmld.distribution_channel
        WHERE ORDER_CREATION_DATE_KEY BETWEEN 
              TO_NUMBER(TO_CHAR(SYSDATE - 366, 'YYYYMMDD'))
              AND TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
            AND olf.MG_ORDER_CODE NOT IN ('M', 'MO')
            AND olf.REG_NON_CAN_FLAG = 'Y'
            AND pmd.MATERIAL_TYPE <> 'ZALT'
            AND pmd.ACTIVE_PRODUCT_FLAG = 'Y'
        '''
        
        # Add test filters if in test mode
        if test_category:
            base_query += f" AND pmd.TIER3 = '{test_category}'"
            
        base_query += " GROUP BY TIER4, TIER3, olf.MATERIAL_NUMBER, TIER1, TIER2"
        
        # Add row limit if specified - wrap in subquery for ROWNUM
        if row_limit:
            base_query = f"SELECT * FROM ({base_query}) WHERE ROWNUM <= {row_limit}"
        
        print(f"Fetching main product data...")
        if test_mode:
            print(f"TEST MODE: Category={test_category}, Limit={row_limit}")
        
        df = cx.read_sql(self.db_url, base_query, return_type="pandas")
        print(f"Main data retrieved: {df.shape[0]} products")
        return df
    
    def calculate_tier3_statistics(self, df):
        """Calculate statistics for TIER3 categories"""
        # Group by TIER3 and calculate metrics
        tier3_stats = df.groupby('TIER3').agg({
            'MATERIAL_NUMBER': 'nunique',
            'AVG_PRICE': 'mean',
            'WEIGHTED_AVG_PRICE': 'median'
        }).reset_index()
        
        tier3_stats.columns = ['TIER3', 'SIZE_OF_TIER3', 'AVG_PRICE_TIER3', 'MEDIAN_PRICE_TIER3']
        
        # Calculate Median Absolute Deviation (MAD)
        median_prices = tier3_stats[['TIER3', 'MEDIAN_PRICE_TIER3']]
        df_with_medians = df.merge(median_prices, on='TIER3')
        df_with_medians['ABS_DEVIATION'] = (df_with_medians['AVG_PRICE'] - 
                                            df_with_medians['MEDIAN_PRICE_TIER3']).abs()
        
        tier3_mad = df_with_medians.groupby('TIER3')['ABS_DEVIATION'].median().reset_index()
        tier3_mad.columns = ['TIER3', 'MEDIAN_ABSOLUTE_DEVIATION']
        
        # Merge MAD back and calculate robust standard deviation
        tier3_stats = tier3_stats.merge(tier3_mad, on='TIER3', how='left')
        tier3_stats['ROBUST_SD'] = 1.4826 * tier3_stats['MEDIAN_ABSOLUTE_DEVIATION']
        
        # Filter categories with sufficient products
        tier3_stats = tier3_stats[tier3_stats['SIZE_OF_TIER3'] > 4]
        
        return tier3_stats
    
    def find_optimal_tau(self, tier3_category, df, tier3_stats, target_percentile=0.9):
        """Find optimal threshold multiplier for a category"""
        cat_info = tier3_stats[tier3_stats['TIER3'] == tier3_category].iloc[0]
        median_price = cat_info['MEDIAN_PRICE_TIER3']
        robust_sd = cat_info['ROBUST_SD']
        
        cat_products = df[df['TIER3'] == tier3_category]
        if len(cat_products) < 5:
            return 1.0
        
        # Sort by price and find threshold
        cat_products_sorted = cat_products.sort_values(by='AVG_PRICE', ascending=False)
        n_top = max(1, int(len(cat_products) * (1 - target_percentile)))
        price_threshold = cat_products_sorted.iloc[n_top-1]['AVG_PRICE']
        
        if robust_sd > 0:
            optimal_tau = (price_threshold - median_price) / robust_sd
            return max(0.5, optimal_tau)  # Minimum of 0.5 standard deviations
        else:
            return 1.0
    
    def identify_premium_products(self, df, tier3_stats, target_percentile=0.9):
        """Identify premium products based on statistical thresholds"""
        # Calculate optimal thresholds
        tier3_stats['OPTIMAL_TAU'] = tier3_stats['TIER3'].apply(
            lambda x: self.find_optimal_tau(x, df, tier3_stats, target_percentile)
        )
        
        tier3_stats['PREMIUM_PRICE_THRESHOLD'] = (
            tier3_stats['MEDIAN_PRICE_TIER3'] + 
            tier3_stats['OPTIMAL_TAU'] * tier3_stats['ROBUST_SD']
        )
        
        # Flag premium products
        df_with_premium = df.copy()
        df_with_premium['IS_PREMIUM'] = 0
        
        threshold_dict = dict(zip(tier3_stats['TIER3'], tier3_stats['PREMIUM_PRICE_THRESHOLD']))
        
        for tier3, threshold in threshold_dict.items():
            mask = (df_with_premium['TIER3'] == tier3) & (df_with_premium['AVG_PRICE'] > threshold)
            df_with_premium.loc[mask, 'IS_PREMIUM'] = 1
        
        df_premium = df_with_premium[df_with_premium['IS_PREMIUM'] == 1].copy()
        
        return df_premium, tier3_stats
    
    def get_premium_sales_details(self, material_numbers):
        """Get detailed sales information for premium products"""
        query_template = '''
        SELECT DISTINCT 
            olf.MATERIAL_NUMBER, 
            LATEST_DESCRIPTION, 
            TIER1, TIER2, TIER3, TIER4,
            SUM(CASE WHEN pd.pricing_unit = 'LB' 
                THEN olf.inv_actual_weight 
                ELSE olf.inv_actual_quantity END) as TOTAL_UNITS,
            COUNT(DISTINCT SALE_ID) as SALE_COUNT,
            COUNT(DISTINCT CUSTOMER_KEY) as CUSTOMER_COUNT,
            SUM(INV_ACTUAL_PRICE) as TOTAL_REVENUE,
            MAX(pmld.UNRESTRICTED_USE_INVENTORY) as INVENTORY,
            MAX(pmld.UNRESTRICTED_USE_INVENTORY_CS) as INVENTORY_CS,
            MAX(pmld.OOS_CURRENT_STATUS) as OOS_STATUS,
            MAX(pmld.DAYS_ON_HAND) as MAX_DAYS_ON_HAND
        FROM fd_dw.DW_OLF_L2Y olf
            INNER JOIN fd_dw.product_material_dim pmd
                ON pmd.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                AND olf.plant = pmd.plant
                AND olf.sales_org = pmd.sales_org
                AND olf.distribution_channel = pmd.distribution_channel
            INNER JOIN fd_dw.PRODUCT_DIM pd
                ON olf.product_key = pd.PRODUCT_KEY
            INNER JOIN FD_DW.PRODUCT_MATERIAL_LIVE_DIM pmld
                ON pmld.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                AND olf.PLANT = pmld.PLANT
                AND olf.sales_org = pmld.sales_org
                AND olf.distribution_channel = pmld.distribution_channel
        WHERE ORDER_CREATION_DATE_KEY BETWEEN 
              TO_NUMBER(TO_CHAR(SYSDATE - 91, 'YYYYMMDD'))
              AND TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
            AND olf.MG_ORDER_CODE NOT IN ('M', 'MO')
            AND olf.REG_NON_CAN_FLAG = 'Y'
            AND pmd.ACTIVE_PRODUCT_FLAG = 'Y'
            AND olf.MATERIAL_NUMBER IN :client_keys
        GROUP BY olf.MATERIAL_NUMBER, LATEST_DESCRIPTION, TIER1, TIER2, TIER3, TIER4
        '''

        query_template_2 = '''
            WITH zone_prices AS (
                SELECT DISTINCT 
                    material_number,
                    latest_description,
                    zone.column_value AS zone
                FROM fd_dw.product_material_dim
                CROSS JOIN TABLE(SYS.ODCIVARCHAR2LIST(
                    100000, 100001, 100002, 200601, 200602, 
                    201301, 201302, 201101, 201102, 201701, 201702
                )) zone
                WHERE sales_org = '1400'
            ),

            default_price AS (
                SELECT /*+ PARALLEL(16) */
                    material_number,
                    LTRIM(pricing_zone_id, '0') AS zone,
                    default_price
                FROM fd_dw.product_dim
                WHERE version_end_date = DATE '3000-01-01'
                    AND sales_org = '1400'
                    AND distribution_channel = '01'
                    AND LTRIM(pricing_zone_id, '0') IN (
                        100000, 100001, 100002, 200601, 200602,
                        201301, 201302, 201101, 201102, 201701, 201702
                    )
            ),

            price_with_fallback AS (
                SELECT
                    zp.material_number,
                    COALESCE(
                        dp.default_price,
                        CASE SUBSTR(zp.zone, 5, 2)
                            WHEN '01' THEN MAX(CASE WHEN zp.zone = '100001' THEN dp.default_price END) 
                                OVER (PARTITION BY zp.material_number)
                            WHEN '02' THEN MAX(CASE WHEN zp.zone = '100002' THEN dp.default_price END) 
                                OVER (PARTITION BY zp.material_number)
                        END,
                        MAX(CASE WHEN zp.zone = '100000' THEN dp.default_price END) 
                            OVER (PARTITION BY zp.material_number)
                    ) AS default_price,
                    zp.zone
                FROM zone_prices zp
                LEFT JOIN default_price dp 
                    ON zp.material_number = dp.material_number 
                    AND zp.zone = dp.zone
            ),

            zone1_prices AS (
                SELECT 
                    material_number,
                    default_price AS zone_1_res_price
                FROM price_with_fallback
                WHERE zone = '100001'
            )

            SELECT DISTINCT 
                olf.MATERIAL_NUMBER, 
                pmd.LATEST_DESCRIPTION, 
                pmd.TIER1, 
                pmd.TIER2, 
                pmd.TIER3, 
                pmd.TIER4,
                SUM(CASE WHEN pd.pricing_unit = 'LB' 
                    THEN olf.inv_actual_weight 
                    ELSE olf.inv_actual_quantity END) as TOTAL_UNITS,
                COUNT(DISTINCT olf.SALE_ID) as SALE_COUNT,
                COUNT(DISTINCT olf.CUSTOMER_KEY) as CUSTOMER_COUNT,
                SUM(olf.INV_ACTUAL_PRICE) as TOTAL_REVENUE,
                MAX(pmld.UNRESTRICTED_USE_INVENTORY) as INVENTORY,
                MAX(pmld.UNRESTRICTED_USE_INVENTORY_CS) as INVENTORY_CS,
                MAX(pmld.OOS_CURRENT_STATUS) as OOS_STATUS,
                MAX(pmld.DAYS_ON_HAND) as MAX_DAYS_ON_HAND,
                MAX(z1p.zone_1_res_price) as ZONE_1_RES_PRICE
            FROM fd_dw.DW_OLF_L2Y olf
                INNER JOIN fd_dw.product_material_dim pmd
                    ON pmd.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                    AND olf.plant = pmd.plant
                    AND olf.sales_org = pmd.sales_org
                    AND olf.distribution_channel = pmd.distribution_channel
                INNER JOIN fd_dw.PRODUCT_DIM pd
                    ON olf.product_key = pd.PRODUCT_KEY
                INNER JOIN fd_dw.PRODUCT_MATERIAL_LIVE_DIM pmld
                    ON pmld.MATERIAL_NUMBER = olf.MATERIAL_NUMBER
                    AND olf.PLANT = pmld.PLANT
                    AND olf.sales_org = pmld.sales_org
                    AND olf.distribution_channel = pmld.distribution_channel
                LEFT JOIN zone1_prices z1p
                    ON z1p.material_number = olf.MATERIAL_NUMBER
            WHERE olf.ORDER_CREATION_DATE_KEY BETWEEN 
                TO_NUMBER(TO_CHAR(SYSDATE - 91, 'YYYYMMDD'))
                AND TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
                AND olf.MG_ORDER_CODE NOT IN ('M', 'MO')
                AND olf.REG_NON_CAN_FLAG = 'Y'
                AND pmd.ACTIVE_PRODUCT_FLAG = 'Y'
                AND olf.MATERIAL_NUMBER IN :client_keys
            GROUP BY olf.MATERIAL_NUMBER, pmd.LATEST_DESCRIPTION, pmd.TIER1, pmd.TIER2, pmd.TIER3, pmd.TIER4
        '''
        
        print(f"\nFetching sales details for {len(material_numbers)} premium products...")
        return self.batch_query_with_keys(query_template_2, material_numbers)
    
    def print_summary(self, df_all, df_premium):
        """Print analysis summary"""
        print("\n" + "="*60)
        print("PREMIUM PRODUCTS ANALYSIS SUMMARY")
        print("="*60)
        print(f"Total products analyzed: {len(df_all):,}")
        print(f"Premium products identified: {len(df_premium):,} ({len(df_premium)/len(df_all)*100:.2f}%)")
        
        # Top categories by premium percentage
        premium_by_cat = df_premium.groupby('TIER3').size().reset_index(name='premium_count')
        total_by_cat = df_all.groupby('TIER3').size().reset_index(name='total_count')
        
        analysis = premium_by_cat.merge(total_by_cat, on='TIER3')
        analysis['premium_percentage'] = (analysis['premium_count'] / 
                                         analysis['total_count'] * 100).round(2)
        analysis = analysis.sort_values('premium_percentage', ascending=False)
        
        print("\nTop 10 categories by premium percentage:")
        print(analysis.head(10).to_string(index=False))


def main():
    """Main execution function"""
    
    # Check for command line arguments for testing
    test_mode = False
    test_category = None
    row_limit = None
    skip_details = False
    
    if len(sys.argv) > 1:
        if '--test' in sys.argv:
            test_mode = True
            print("RUNNING IN TEST MODE")
            
        if '--category' in sys.argv:
            idx = sys.argv.index('--category')
            if idx + 1 < len(sys.argv):
                test_category = sys.argv[idx + 1]
                
        if '--limit' in sys.argv:
            idx = sys.argv.index('--limit')
            if idx + 1 < len(sys.argv):
                row_limit = int(sys.argv[idx + 1])
                
        if '--skip-details' in sys.argv:
            skip_details = True
            print("Skipping detailed sales data fetch")
    
    # Database configuration
    db_config = {
        'user': 'FD_STG',
        'password': 'FD_STG',
        'dsn': 'NJ01PRDDWDB01.NJ01:1521/DATAWPRO'
    }
    
    try:
        # Initialize analyzer
        analyzer = PremiumProductAnalyzer(db_config)
        
        # Step 1: Get main product data
        print("Starting Premium Products Analysis")
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        df_all_products = analyzer.get_main_data(
            test_mode=test_mode,
            test_category=test_category,
            row_limit=row_limit
        )
        
        if df_all_products.empty:
            print("No data retrieved. Exiting.")
            return
        
        # Show categories if in test mode
        if test_mode:
            print("\nCategories in dataset:")
            print(df_all_products['TIER3'].value_counts().head(10))
        
        # Step 2: Calculate TIER3 statistics
        print("\nCalculating category statistics...")
        tier3_stats = analyzer.calculate_tier3_statistics(df_all_products)
        
        if tier3_stats.empty:
            print("No categories with sufficient data. Exiting.")
            return
        
        # Step 3: Identify premium products
        print("\nIdentifying premium products...")
        df_premium, tier3_stats_final = analyzer.identify_premium_products(
            df_all_products, 
            tier3_stats, 
            target_percentile=0.9
        )
        
        if df_premium.empty:
            print("No premium products identified.")
            return
        
        # Step 4: Get detailed sales data (optional in test mode)
        if not skip_details:
            material_numbers = df_premium['MATERIAL_NUMBER'].unique().tolist()
            df_premium_details = analyzer.get_premium_sales_details(material_numbers)
            
            # Step 5: Merge details with premium products
            # Convert both MATERIAL_NUMBER columns to string to avoid type mismatch
            df_premium['MATERIAL_NUMBER'] = df_premium['MATERIAL_NUMBER'].astype(str)
            df_premium_details['MATERIAL_NUMBER'] = df_premium_details['MATERIAL_NUMBER'].astype(str)
            merge_columns = ['MATERIAL_NUMBER', 'TIER1', 'TIER2', 'TIER3', 'TIER4']
            df_premium_final = df_premium.merge(df_premium_details, on=merge_columns, how='left')
        else:
            df_premium_final = df_premium
        
        # Step 6: Save results
        if test_mode:
            output_file = f'premium_products_TEST_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        else:
            output_file = f'premium_products_{datetime.now().strftime("%Y%m%d")}.xlsx'
            
        # Create Excel writer object to write multiple sheets
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Raw Data
            df_premium_final.to_excel(writer, sheet_name='Raw Data', index=False)
            
            # Sheet 2: Pivot View
            # Prepare pivot data
            pivot_data = df_premium_final.copy()
            pivot_data['AVG_QTY_SOLD_PER_DAY'] = pivot_data['TOTAL_UNITS'] / 91.0
            # Select relevant columns for pivot view
            pivot_columns = ['MATERIAL_NUMBER', 'TIER1', 'TIER2', 'TIER3', 'TIER4']
            
            # Add description if it exists
            if 'LATEST_DESCRIPTION' in pivot_data.columns:
                pivot_columns.append('LATEST_DESCRIPTION')
            
            # Add price and inventory columns
            value_columns = ['AVG_PRICE']
            if 'INVENTORY' in pivot_data.columns:
                value_columns.append('INVENTORY')
            if 'ZONE_1_RES_PRICE' in pivot_data.columns:
                value_columns.append('ZONE_1_RES_PRICE')
            if 'MAX_DAYS_ON_HAND' in pivot_data.columns:
                value_columns.append('MAX_DAYS_ON_HAND')
            if 'CUSTOMER_COUNT' in pivot_data.columns:
                value_columns.append('CUSTOMER_COUNT')
            if 'AVG_QTY_SOLD_PER_DAY' in pivot_data.columns:
                value_columns.append('AVG_QTY_SOLD_PER_DAY')
                
            # Combine columns for pivot view
            pivot_view_columns = pivot_columns + value_columns
            pivot_view = pivot_data[pivot_view_columns].copy()
            
            
            # Sort by TIER1 and then by AVG_PRICE descending within each TIER1
            pivot_view = pivot_view.sort_values(
                by=['TIER1', 'ZONE_1_RES_PRICE'], 
                ascending=[True, False]
            )
            
            # Write pivot view to second sheet
            pivot_view.to_excel(writer, sheet_name='Pivot View', index=False)
            
            # FIXED: Apply grouping to Pivot View sheet
            print("\nApplying TIER1 grouping to Pivot View...")
            worksheet = writer.sheets['Pivot View']
            
            # Find the groups in the sorted dataframe
            tier1_groups = []
            current_tier1 = None
            start_row = None
            
            for idx, (row_position, row) in enumerate(pivot_view.iterrows()):
                tier1_value = row['TIER1']
                
                if tier1_value != current_tier1:
                    # Save the previous group if it exists
                    if current_tier1 is not None and start_row is not None:
                        tier1_groups.append((current_tier1, start_row, idx - 1))
                    
                    # Start a new group
                    current_tier1 = tier1_value
                    start_row = idx
            
            # Don't forget the last group
            if current_tier1 is not None and start_row is not None:
                tier1_groups.append((current_tier1, start_row, len(pivot_view) - 1))
            
            # Apply grouping to worksheet using proper openpyxl methods
            # Excel rows are 1-based, and we have a header row, so add 2
            for tier1_name, df_start_pos, df_end_pos in tier1_groups:
                # Convert to Excel row numbers (add 2: +1 for header, +1 for 1-based indexing)
                excel_start = df_start_pos + 2
                excel_end = df_end_pos + 2
                
                # Set outline level for each row in the group
                # We skip the first row of each group (the summary row) and group the rest
                if excel_end > excel_start:  # Only group if there's more than one row
                    for row_num in range(excel_start + 1, excel_end + 1):
                        worksheet.row_dimensions[row_num].outlineLevel = 1
                        worksheet.row_dimensions[row_num].hidden = True  # Collapse the group
                
                print(f"  Grouped {tier1_name}: rows {excel_start}-{excel_end} ({df_end_pos - df_start_pos + 1} items)")
            
            # Set outline properties
            # summaryBelow = False means the summary row is above the detail rows
            worksheet.sheet_properties.outlinePr.summaryBelow = False
            worksheet.sheet_properties.outlinePr.showOutlineSymbols = True
            
            # Optional: Set the outline level view (show level 1 only, hiding level 2)
            worksheet.sheet_properties.outlinePr.summaryRight = False
            
            print(f"Applied grouping to {len(tier1_groups)} TIER1 categories")
            
            # Add a summary sheet with statistics by TIER1
            summary_by_tier1 = df_premium_final.groupby('TIER1').agg({
                'MATERIAL_NUMBER': 'count',
                'ZONE_1_RES_PRICE': ['mean', 'median', 'min', 'max']
            }).round(2)
            
            # Flatten column names
            summary_by_tier1.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                        for col in summary_by_tier1.columns.values]
            summary_by_tier1.columns = ['SKU_Count', 'Avg_Price_Mean', 'Avg_Price_Median', 
                                        'Avg_Price_Min', 'Avg_Price_Max']
            
            # Add revenue if available
            if 'TOTAL_REVENUE' in df_premium_final.columns:
                revenue_by_tier1 = df_premium_final.groupby('TIER1')['TOTAL_REVENUE'].sum().round(2)
                summary_by_tier1['Total_Revenue'] = revenue_by_tier1
            
            summary_by_tier1.reset_index(inplace=True)
            summary_by_tier1 = summary_by_tier1.sort_values('SKU_Count', ascending=False)
            
            # Write summary to third sheet
            summary_by_tier1.to_excel(writer, sheet_name='Summary by TIER1', index=False)
            
        print(f"\nResults saved to: {output_file}")
        print(f"  - Sheet 1: Raw Data ({len(df_premium_final)} rows)")
        print(f"  - Sheet 2: Pivot View (sorted by TIER1 and price, with collapsible groups)")
        print(f"  - Sheet 3: Summary by TIER1 ({len(summary_by_tier1)} categories)")
        
        # Print summary
        analyzer.print_summary(df_all_products, df_premium_final)
        
    except KeyboardInterrupt:
        print("\n\nScript interrupted by user (Ctrl+C)")
        print("Exiting gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        raise


if __name__ == "__main__":
    # Print usage instructions
    if '--help' in sys.argv or '-h' in sys.argv:
        print("""
        Premium Products Analysis Script
        
        Usage:
            python premium_products_script.py [options]
        
        Options:
            --test              Run in test mode
            --category NAME     Test with specific TIER3 category (e.g., "Dairy")
            --limit N           Limit to N rows for testing
            --skip-details      Skip fetching detailed sales data
            --help, -h          Show this help message
        
        Examples:
            # Test with one category
            python premium_products_script.py --test --category "Dairy"
            
            # Test with limited rows
            python premium_products_script.py --test --limit 1000
            
            # Quick test without sales details
            python premium_products_script.py --test --limit 500 --skip-details
            
            # Full run
            python premium_products_script.py
        """)
        sys.exit(0)
    
    main()