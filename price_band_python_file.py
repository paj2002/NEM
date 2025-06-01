# Price Band Analysis
# For STATE break-down by Demand (0,100,300 etc) just do manually cuz python cannot make an easier toggle + dynamic chart

import numpy as np
import pandas as pd
import datetime
import requests
import os
import time
from io import StringIO

# ==================== USER CONFIGURATION ====================

# Output file names (without .csv extension) --> Change name for adhoc analysis and don't disturb workflow
# Default workflow name --> 'Price_Band'
consolidated_filename = 'Price_Band'

start_month = 4
start_year = 2025

end_month = 4
end_year = 2025

# Specify which states to download data for
# Available options: 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'
states = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

# ===========================================================

def download_aemo_data(start_month, start_year, end_month, end_year, states, directory):
    """Download AEMO price and demand data for specified date range and states"""
    
    # Convert user-friendly input to proper date range
    startdate = f'{start_year}-{start_month:02d}-01'
    # Get the last day of the end month automatically
    if end_month == 12:
        next_month = 1
        next_year = end_year + 1
    else:
        next_month = end_month + 1
        next_year = end_year

    # Calculate last day of end month
    last_day = (pd.Timestamp(f'{next_year}-{next_month:02d}-01') - pd.Timedelta(days=1)).day
    enddate = f'{end_year}-{end_month:02d}-{last_day}'

    print(f"Date range: {startdate} to {enddate}")

    # Generate date range
    daterange = pd.date_range(startdate, enddate, freq='MS').strftime('%Y%m').tolist()
    print(f"Months to download: {daterange}")

    # Define the root URL for dataset retrieval
    root_url = 'https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_'

    # Generate URLs for all combinations of dates and states
    urls = []
    for date in daterange:
        for state in states:
            urls.append(root_url + date + '_' + state + '.csv')

    print(f"Will download {len(urls)} files for states: {', '.join(states)}")

    # Define headers to mimic browser behavior
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://aemo.com.au/'
    }

    # Download and process files directly without saving individually
    downloaded = 0
    failed = 0
    all_data = []

    for i, url in enumerate(urls, 1):
        print(f"\nDownloading and processing file {i}/{len(urls)}: {os.path.basename(url)}")
        
        try:
            # Create a session to maintain cookies
            session = requests.Session()
            
            # Send a GET request to the URL
            response = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Read CSV data directly from response content
                csv_data = StringIO(response.text)
                df = pd.read_csv(csv_data)
                all_data.append(df)
                print(f"✓ Downloaded and processed successfully: {os.path.basename(url)}")
                downloaded += 1
            else:
                print(f"✗ Failed to download. Status code: {response.status_code}")
                failed += 1
                # Try alternative URL format if available
                if response.status_code == 403:
                    print(f"  Access denied. This file may not be publicly available.")
        
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading: {e}")
            failed += 1
        except pd.errors.EmptyDataError:
            print(f"✗ Error: Empty or invalid CSV data")
            failed += 1
        except Exception as e:
            print(f"✗ Error processing CSV: {e}")
            failed += 1
        
        time.sleep(7)  # Increased delay to be more respectful

    print(f"\n==================== DOWNLOAD SUMMARY ====================")
    print(f"Total files processed: {len(urls)}")
    print(f"Successfully downloaded: {downloaded}")
    print(f"Failed downloads: {failed}")
    print(f"Files saved to: {directory}")
    print("="*60)
    
    return all_data

def analyze_hourly_price_bands(combined_df, start_month, start_year, end_month, end_year, states, directory, consolidated_filename):
    """Analyze hourly price band patterns"""
    
    # Determine the region column name (it might be 'REGION' or 'REGIONID')
    region_col = None
    if 'REGION' in combined_df.columns:
        region_col = 'REGION'
    elif 'REGIONID' in combined_df.columns:
        region_col = 'REGIONID'
    else:
        print("Warning: Could not find REGION or REGIONID column")
        print(f"Available columns: {list(combined_df.columns)}")
        return

    if region_col and 'TOTALDEMAND' in combined_df.columns and 'RRP' in combined_df.columns:
        # Convert SETTLEMENTDATE to datetime for filtering
        combined_df['SETTLEMENTDATE'] = pd.to_datetime(combined_df['SETTLEMENTDATE'])
        
        # Filter data to only include the months we actually requested
        start_date = pd.Timestamp(f'{start_year}-{start_month:02d}-01')
        if end_month == 12:
            end_date = pd.Timestamp(f'{end_year + 1}-01-01') - pd.Timedelta(seconds=1)
        else:
            end_date = pd.Timestamp(f'{end_year}-{end_month + 1:02d}-01') - pd.Timedelta(seconds=1)
        
        print(f"Filtering data to range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Filter the dataframe to only include requested date range
        filtered_df = combined_df[(combined_df['SETTLEMENTDATE'] >= start_date) & 
                                 (combined_df['SETTLEMENTDATE'] <= end_date)].copy()
        
        print(f"Records before filtering: {len(combined_df)}")
        print(f"Records after filtering: {len(filtered_df)}")
        
        # Get unique month-year combinations in the filtered data
        filtered_df['MONTH_YEAR'] = filtered_df['SETTLEMENTDATE'].dt.strftime('%Y-%m')
        unique_months = sorted(filtered_df['MONTH_YEAR'].unique())
        
        print(f"Found data for months: {', '.join(unique_months)}")

        # ==================== HOURLY PRICE BAND ANALYSIS ====================
        print(f"\n==================== HOURLY PRICE BAND ANALYSIS ====================")
        
        # Extract hour from SETTLEMENTDATE
        filtered_df['HOUR'] = filtered_df['SETTLEMENTDATE'].dt.hour
        
        # Create included range description
        if len(unique_months) == 1:
            # Single month: "Mar 2025"
            month_obj = pd.to_datetime(unique_months[0] + '-01')
            included_range = month_obj.strftime('%b %Y')
        else:
            # Multiple months: "Feb 2025 - Apr 2025"
            start_month_obj = pd.to_datetime(unique_months[0] + '-01')
            end_month_obj = pd.to_datetime(unique_months[-1] + '-01')
            included_range = f"{start_month_obj.strftime('%b %Y')} - {end_month_obj.strftime('%b %Y')}"
        
        print(f"Analyzing hourly patterns for period: {included_range}")
        
        hourly_data = []
        
        # Process each hour (0-23)
        for hour in range(24):
            print(f"Processing hour {hour:02d}:00...")
            
            # Filter data for current hour across ALL months in the analysis period
            hour_data = filtered_df[filtered_df['HOUR'] == hour]
            
            if not hour_data.empty:
                # Step 1: Calculate cumulative totals for each threshold (like your Table 1)
                # This matches: SUMIF($C:$C, $A:$A, STATE, $F:$F, $H20, $D:$D, "<="&I18)/12
                
                cumulative_totals = {}
                thresholds = [0, 100, 300, 1000, float('inf')]  # 0, 100, 300, 1000, Above
                
                for threshold in thresholds:
                    total_for_threshold = 0
                    
                    # Calculate for each state separately (like your Table 1)
                    for state in states:
                        state_hour_data = hour_data[hour_data[region_col] == state]
                        if not state_hour_data.empty:
                            if threshold == float('inf'):
                                # For "Above" - get all data
                                state_total = state_hour_data['TOTALDEMAND'].sum() / 12
                            else:
                                # For specific thresholds - get data <= threshold
                                state_total = state_hour_data[state_hour_data['RRP'] <= threshold]['TOTALDEMAND'].sum() / 12
                            
                            total_for_threshold += state_total
                    
                    cumulative_totals[threshold] = total_for_threshold
                
                # Step 2: Calculate differences to get the actual bands (like your Table 3)
                total_under_0 = cumulative_totals[0]  # Everything <= 0
                total_0_to_100 = cumulative_totals[100] - cumulative_totals[0]  # (<=100) - (<=0)
                total_100_to_300 = cumulative_totals[300] - cumulative_totals[100]  # (<=300) - (<=100)
                total_300_to_1000 = cumulative_totals[1000] - cumulative_totals[300]  # (<=1000) - (<=300)
                total_1000_plus = cumulative_totals[float('inf')] - cumulative_totals[1000]  # (All) - (<=1000)
                
                hourly_data.append({
                    'HOUR': hour,
                    'INCLUDED_RANGE': included_range,
                    '<0': total_under_0,
                    '0-100': total_0_to_100,
                    '100-300': total_100_to_300,
                    '300-1000': total_300_to_1000,
                    '1000+': total_1000_plus
                })
                
                print(f"  Hour {hour:02d}: <0={total_under_0:,.0f}, 0-100={total_0_to_100:,.0f}, 100-300={total_100_to_300:,.0f}, 300-1000={total_300_to_1000:,.0f}, 1000+={total_1000_plus:,.0f}")
            else:
                # Fill with zeros if no data for this hour
                hourly_data.append({
                    'HOUR': hour,
                    'INCLUDED_RANGE': included_range,
                    '<0': 0,
                    '0-100': 0,
                    '100-300': 0,
                    '300-1000': 0,
                    '1000+': 0
                })
                print(f"  Hour {hour:02d}: No data - filled with zeros")
        
        # Save hourly analysis file with configurable name
        if hourly_data:
            hourly_df = pd.DataFrame(hourly_data)
            
            # Create hourly filename using configurable name
            hourly_file_path = os.path.join(directory, f'{consolidated_filename}.csv')
            
            hourly_df.to_csv(hourly_file_path, index=False)
            print(f"✓ Hourly analysis file saved: {consolidated_filename}.csv")
            
            # Display hourly summary table
            print(f"\n==================== HOURLY ANALYSIS SUMMARY ====================")
            print(f"Hourly price band demand totals for period: {included_range}")
            print(hourly_df.to_string(index=False, float_format='%.0f'))
            print("="*60)
        else:
            print("No hourly data calculated")
    else:
        print("Required columns not found for hourly analysis")

def main():
    """Main execution function"""
    # Specify the directory to store downloaded files
    directory = os.path.expanduser('~/Desktop/NEM EXCEL/')  # macOS path to your NEM EXCEL folder

    # Create directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download data
    all_data = download_aemo_data(start_month, start_year, end_month, end_year, states, directory)

    # ==================== CONSOLIDATION AND ANALYSIS ====================
    if all_data:
        print(f"\n==================== CONSOLIDATING DATA ====================")
        
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # REMOVED: Create consolidated filename - no longer saving this file
        print(f"✓ Data consolidated in memory (not saving consolidated file)")
        
        # ==================== HOURLY PRICE BAND ANALYSIS ====================
        analyze_hourly_price_bands(combined_df, start_month, start_year, end_month, end_year, states, directory, consolidated_filename)

    else:
        print("No valid CSV files could be processed for consolidation")

if __name__ == "__main__":
    main()
