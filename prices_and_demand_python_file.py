# Price and Demand
# Outputs a download like collated from specific range from AEMO
# Metrics file which is all calculations for Total Demand by state, TWAP, VWAP and Price Bands

import numpy as np
import pandas as pd
import datetime
import requests
import os
import time
from io import StringIO

# ==================== USER CONFIGURATION ====================

start_month = 4
start_year = 2025

end_month = 4
end_year = 2025

# Specify which states to download data for
# Available options: 'NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1'
states = ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']

# Output file names (without .csv extension) --> Change name for adhoc analysis and don't disturb workflow
# Default workflow name --> 'PRICE_AND_DEMAND' , 'PRICE_AND_DEMAND_METRICS'
consolidated_filename = 'PRICE_AND_DEMAND'
metrics_filename = 'PRICE_AND_DEMAND_METRICS'

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

def calculate_metrics(combined_df, start_month, start_year, end_month, end_year, states, directory, metrics_filename):
    """Calculate price and demand metrics"""
    
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
        # Convert SETTLEMENTDATE to datetime for month extraction
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
        
        # Create metrics dataframe
        metrics_data = []
        
        # Calculate metrics for each state and each month
        for month_year in unique_months:
            print(f"\nCalculating metrics for {month_year}...")
            month_data = filtered_df[filtered_df['MONTH_YEAR'] == month_year]
            
            for state in states:
                print(f"  Processing {state} for {month_year}...")
                
                # Filter data for current state and month
                state_month_data = month_data[month_data[region_col] == state]
                
                if not state_month_data.empty:
                    # Calculate basic metrics
                    demand = state_month_data['TOTALDEMAND'].sum() / 12
                    twap = state_month_data['RRP'].mean()
                    
                    # Calculate VWAP (Volume Weighted Average Price)
                    total_value = (state_month_data['TOTALDEMAND'] * state_month_data['RRP']).sum()
                    total_demand = state_month_data['TOTALDEMAND'].sum()
                    vwap = total_value / total_demand if total_demand != 0 else 0
                    
                    # Calculate price band hours (equivalent to COUNTIFS in Excel, divided by 12)
                    hours_over_5000 = len(state_month_data[state_month_data['RRP'] >= 5000]) / 12
                    hours_300_to_5000 = len(state_month_data[(state_month_data['RRP'] >= 300) & (state_month_data['RRP'] < 5000)]) / 12
                    hours_150_to_300 = len(state_month_data[(state_month_data['RRP'] >= 150) & (state_month_data['RRP'] < 300)]) / 12
                    hours_100_to_150 = len(state_month_data[(state_month_data['RRP'] >= 100) & (state_month_data['RRP'] < 150)]) / 12
                    hours_0_to_100 = len(state_month_data[(state_month_data['RRP'] >= 0) & (state_month_data['RRP'] < 100)]) / 12
                    hours_under_0 = len(state_month_data[state_month_data['RRP'] < 0]) / 12
                    
                    # Add all metrics to the data list
                    metrics_data.append({
                        'STATE': state,
                        'MONTH_YEAR': month_year,
                        'DEMAND': demand,
                        'TWAP': twap,
                        'VWAP': vwap,
                        'Hours >$5000': hours_over_5000,
                        'Hours $5000-$300': hours_300_to_5000,
                        'Hours $300-$150': hours_150_to_300,
                        'Hours $150-$100': hours_100_to_150,
                        'Hours $0-$100': hours_0_to_100,
                        'Hours <$0': hours_under_0
                    })
                    
                    print(f"    {state}: Demand={demand:,.0f}, TWAP=${twap:.2f}, VWAP=${vwap:.2f}")
                    print(f"      Price bands - >$5K: {hours_over_5000:.1f}h, $300-5K: {hours_300_to_5000:.1f}h, $150-300: {hours_150_to_300:.1f}h")
                    print(f"      $100-150: {hours_100_to_150:.1f}h, $0-100: {hours_0_to_100:.1f}h, <$0: {hours_under_0:.1f}h")
                else:
                    print(f"    No data found for {state} in {month_year}")

        # Save metrics file
        if metrics_data:
            metrics_df = pd.DataFrame(metrics_data)
            
            # Create metrics filename
            metrics_file_path = os.path.join(directory, f'{metrics_filename}.csv')
            
            metrics_df.to_csv(metrics_file_path, index=False)
            print(f"✓ Metrics file saved: {metrics_filename}.csv")
            
            # Display summary table
            print(f"\n==================== METRICS SUMMARY ====================")
            print("Full metrics table:")
            print(metrics_df.to_string(index=False, float_format='%.2f'))
            print("="*60)
        else:
            print("No metrics calculated - check column names in your CSV files")
            print(f"Available columns: {list(combined_df.columns)}")

def main():
    """Main execution function"""
    # Specify the directory to store downloaded files
    directory = os.path.expanduser('~/Desktop/NEM EXCEL/')  # macOS path to your NEM EXCEL folder

    # Create directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download data
    all_data = download_aemo_data(start_month, start_year, end_month, end_year, states, directory)

    # ==================== CONSOLIDATION AND METRICS ====================
    if all_data:
        print(f"\n==================== CONSOLIDATING DATA ====================")
        
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Create consolidated filename
        consolidated_file_path = os.path.join(directory, f'{consolidated_filename}.csv')
        combined_df.to_csv(consolidated_file_path, index=False)
        print(f"✓ Consolidated file saved: {consolidated_filename}.csv")
        
        # ==================== CALCULATE METRICS ====================
        print(f"\n==================== CALCULATING METRICS ====================")
        calculate_metrics(combined_df, start_month, start_year, end_month, end_year, states, directory, metrics_filename)

    else:
        print("No valid CSV files could be processed for consolidation")

if __name__ == "__main__":
    main()
