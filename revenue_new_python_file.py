# Enhanced Open Electricity Data Puller with NEM Data Integration
# Will need the DUID excel from AEMO Generation Information EXCEL
# WITH MULTI-YEAR SUPPORT: Auto-handles API 365-day limit + Decommissioned facility tracking
# ✅ FIXED: Now includes ALL decommissioned DUIDs (even with minimal data)

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import openpyxl
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================== USER CONFIGURATION ====================

# Output file names (without .csv extension) --> Change name for adhoc analysis and don't disturb workflow
# Default workflow name --> 'ALLSTATES_Revenue'
consolidated_filename = 'NSW_Revenue'
decommissioned_filename = 'NSW_Decommissioned_Revenue'  # NEW: For historical DUIDs

# API Key (get from https://platform.openelectricity.org.au)
API_KEY = "oe_3ZbuDQVhMCk1guoQqd7eBcWi"

# 🌐 Network code (market you want data from)
# - "NEM" → National Electricity Market (eastern Australia)
# - "WEM" → Western Australia
# - "AEMO_ROOFTOP" → Rooftop PV estimates
# - "APVI" → Community PV data
NETWORK_CODE = "NEM"

# 🏞️ REGION FILTER - Filter by specific regions/states
REGION_FILTER = ["NSW1"]
# REGION_FILTER = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]  # All states

# 📅 Time interval 
# Options:
# - "1h" → Hourly
# - "1d" → Daily
# - "7d" → Weekly
# - "1M" → Monthly
# - "3M" → Quarterly
# - "season" → Seasonal
# - "1y" → Calendar year
# - "fy" → Financial year
INTERVAL = "1d"

# Metric (you can only choose ONE per request)
#"energy" → MWh (electricity generated/consumed)-> Volume tab in Excel
#"power" → MW (average power/generation) -> feeds in price later anyways
#"market_value" → $AUD (total market value/revenue)-> Revenue tab in Excel (NOW IN MILLIONS)
#"emissions" → tCO2e (carbon emissions)
#"renewable_proportion" → % (share of renewables)
METRIC = "market_value"

# 🆕 NEW: RETRY CONFIGURATION
MAX_RETRIES = 3  # Number of retries for failed requests
RETRY_DELAY = 5  # Base delay between retries (seconds)
BATCH_DELAY = 1.0  # Delay between batches (seconds) - increased for stability

# ==================== ENHANCED DATE CONFIGURATION ====================
# Specify the date range - just month and year!
# 🆕 NEW: Code will automatically loop if date range > 365 days (API limit)
start_month = 7  
start_year = 2014

end_month = 5      
end_year = 2025
# ===========================================================

def create_robust_session():
    """
    🆕 NEW: Create a requests session with retry strategy and SSL handling
    """
    session = requests.Session()
    
    # Define retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set timeout
    session.timeout = 30
    
    return session

def calculate_date_periods(start_month, start_year, end_month, end_year):
    """
    🆕 NEW FUNCTION: Calculate if we need multiple API calls and break into periods
    API has 365-day limit, so we break long ranges into yearly chunks
    """
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
    
    # Create start and end dates
    start_date = datetime.strptime(startdate, "%Y-%m-%d")
    end_date = datetime.strptime(enddate, "%Y-%m-%d")
    
    # Check if range > 365 days
    total_days = (end_date - start_date).days
    print(f"📅 Total date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({total_days} days)")
    
    if total_days <= 365:
        # Single period - works exactly like before
        print(f"📅 Date range: {startdate} to {enddate}")
        return [(start_date, end_date)]
    else:
        # Multiple periods needed due to API limit
        periods = []
        current_start = start_date
        
        while current_start < end_date:
            # Calculate end of current period (1 year from start)
            current_end = current_start + relativedelta(years=1) - timedelta(days=1)
            
            # Don't go past the final end date
            if current_end > end_date:
                current_end = end_date
                
            periods.append((current_start, current_end))
            
            # Next period starts the day after current period ends
            current_start = current_end + timedelta(days=1)
        
        print(f"🔄 Breaking into {len(periods)} periods due to API 365-day limit:")
        for i, (start, end) in enumerate(periods, 1):
            print(f"   Period {i}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
        
        return periods

# === FUNCTION: Load NEM reference data ===
def load_nem_reference_data(file_path="NEM DATA.xlsx"):
    """
    Load the NEM reference data and create a DUID lookup dictionary
    """
    try:
        # Read the Excel file
        nem_df = pd.read_excel(file_path, sheet_name='Sheet1')
        
        # Filter out records without DUID
        nem_df_clean = nem_df[nem_df['DUID'].notna()].copy()
        
        # Create lookup dictionary
        duid_lookup = {}
        for _, row in nem_df_clean.iterrows():
            duid = row['DUID']
            duid_lookup[duid] = {
                'Region': row.get('Region', 'N/A'),
                'Facility': row.get('Facility', 'N/A'),
                'Owner': row.get('Owner', 'N/A'),
                'Number_of_Units': row.get('Number of Units', 'N/A'),
                'Nameplate_Capacity_MW': row.get('Nameplate Capacity (MW)', 'N/A'),
                'Storage_Capacity_MWh': row.get('Storage Capacity (MWh)', 'N/A'),
                'Expected_Closure_Year': row.get('Expected Closure Year', 'N/A'),
                'Fueltech': row.get('Fueltech', 'N/A')
            }
        
        print(f"✅ Loaded {len(duid_lookup)} DUIDs from NEM reference data")
        return duid_lookup
    
    except FileNotFoundError:
        print("⚠️ NEM DATA.xlsx not found. Proceeding without reference data.")
        return {}
    except Exception as e:
        print(f"⚠️ Error loading NEM reference data: {e}")
        return {}

# === FUNCTION: Get all facilities from network ===
def fetch_all_facility_codes(api_key, network_code="NEM"):
    """Get all facilities from network with retry logic"""
    session = create_robust_session()
    
    for attempt in range(MAX_RETRIES):
        try:
            url = "https://api.openelectricity.org.au/v4/facilities/"
            headers = {"Authorization": f"Bearer {api_key}"}
            params = {"network_id": network_code, "with_clerk": "true"}
            
            response = session.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                facilities = [f["code"] for f in response.json().get("data", []) if len(f["code"]) < 30]
                session.close()
                return facilities
            else:
                print(f"❌ Error fetching facilities: {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    
    session.close()
    print("❌ Failed to fetch facilities after all retries")
    return []

# === FUNCTION: Check if DUID should be included based on region filter ===
def should_include_duid(duid, metadata, duid_lookup, region_filter):
    """
    Check if a DUID should be included based on the region filter
    """
    if not region_filter:  # No filter, include all
        return True
    
    # Get region from NEM data first (more accurate), fallback to API data
    region = None
    if duid in duid_lookup:
        region = duid_lookup[duid].get('Region', 'N/A')
    
    if region == 'N/A' and duid in metadata:
        region = metadata[duid].get('Region', 'N/A')
    
    return region in region_filter

# === FUNCTION: Fetch data for all facilities in batches ===
def fetch_data_for_period(facility_codes, metric, duid_lookup, start_date, end_date, period_num):
    """
    🆕 ENHANCED: Fetch data for a single time period with robust error handling
    """
    if period_num == 1:
        print("🔄 Fetching energy data...")
    else:
        print(f"\n🔄 Fetching Period {period_num}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    all_records = []
    all_metadata = {}
    
    # Convert dates to API format
    DATE_START = start_date.strftime("%Y-%m-%dT00:00:00")
    DATE_END = end_date.strftime("%Y-%m-%dT00:00:00")

    session = create_robust_session()
    headers = {"Authorization": f"Bearer {API_KEY}"}
    base_url = f"https://api.openelectricity.org.au/v4/data/facilities/{NETWORK_CODE}"
    BATCH_SIZE = 20

    total_batches = len(facility_codes) // BATCH_SIZE + 1
    successful_batches = 0
    failed_batches = 0

    for i in range(0, len(facility_codes), BATCH_SIZE):
        batch = facility_codes[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        params = {
            "facility_code": batch,
            "metrics": [metric],
            "interval": INTERVAL,
            "date_start": DATE_START,
            "date_end": DATE_END,
            "with_clerk": "true"
        }

        # 🆕 RETRY LOGIC FOR EACH BATCH
        batch_success = False
        for attempt in range(MAX_RETRIES):
            try:
                if period_num == 1:
                    print(f"📦 Fetching batch {batch_num} of {total_batches}")
                else:
                    print(f"   📦 Batch {batch_num}/{total_batches}")
                
                response = session.get(base_url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Process the data (same logic as before)
                    for facility_block in data.get("data", []):
                        facility_code = facility_block.get("facility_code", "N/A")
                        facility_region = facility_block.get("network_region", "N/A")
                        facility_fueltech = facility_block.get("fueltech_id", "N/A")

                        for result in facility_block.get("results", []):
                            duid = result["columns"].get("unit_code", "N/A")
                            name = result.get("name", duid)
                            key = duid

                            if duid == "N/A":
                                continue

                            # Enhanced metadata with NEM reference data
                            base_metadata = {
                                "DUID": duid,
                                "Name": name,
                                "Facility": facility_code,
                                "Region": facility_region,
                                "Fueltech": facility_fueltech
                            }
                            
                            if duid in duid_lookup:
                                nem_data = duid_lookup[duid]
                                enhanced_metadata = {
                                    "DUID": duid,
                                    "Name": name,
                                    "Facility": nem_data.get('Facility', facility_code),
                                    "Region": nem_data.get('Region', facility_region),
                                    "Fueltech": nem_data.get('Fueltech', facility_fueltech),
                                    "Owner": nem_data.get('Owner', 'N/A'),
                                    "Number_of_Units": nem_data.get('Number_of_Units', 'N/A'),
                                    "Nameplate_Capacity_MW": nem_data.get('Nameplate_Capacity_MW', 'N/A'),
                                    "Storage_Capacity_MWh": nem_data.get('Storage_Capacity_MWh', 'N/A'),  # ✅ FIXED: Correct key name
                                    "Expected_Closure_Year": nem_data.get('Expected_Closure_Year', 'N/A')
                                }
                                all_metadata[key] = enhanced_metadata
                            else:
                                enhanced_metadata = base_metadata.copy()
                                enhanced_metadata.update({
                                    "Owner": 'N/A',
                                    "Number_of_Units": 'N/A',
                                    "Nameplate_Capacity_MW": 'N/A',
                                    "Storage_Capacity_MWh": 'N/A',
                                    "Expected_Closure_Year": 'N/A'
                                })
                                all_metadata[key] = enhanced_metadata

                            if not should_include_duid(duid, all_metadata, duid_lookup, REGION_FILTER):
                                continue

                            # Process numerical data - DIVIDE BY 1,000,000 HERE
                            for timestamp, value in result.get("data", []):
                                value_in_millions = value / 1_000_000 if value is not None else 0
                                
                                all_records.append({
                                    "timestamp": timestamp[:10],  # Extract date part only
                                    "key": key,
                                    "value": value_in_millions,  # Now in millions
                                    "period": period_num  # 🆕 NEW: Track which period this data came from
                                })
                    
                    successful_batches += 1
                    batch_success = True
                    break  # Success, exit retry loop
                    
                else:
                    print(f"   ❌ HTTP {response.status_code}: {response.text[:100]}")
                    
            except Exception as e:
                print(f"   ⚠️ Batch {batch_num} attempt {attempt + 1} failed: {str(e)[:100]}")
                if attempt < MAX_RETRIES - 1:
                    print(f"   🔄 Retrying in {RETRY_DELAY * (attempt + 1)} seconds...")
                    time.sleep(RETRY_DELAY * (attempt + 1))

        if not batch_success:
            failed_batches += 1
            print(f"   ❌ Batch {batch_num} failed after all retries - continuing with next batch")

        # Friendly pause between batches
        time.sleep(BATCH_DELAY)

    session.close()
    
    if period_num == 1:
        print(f"✅ Retrieved {len(all_records)} data points (values in millions)")
    else:
        print(f"   ✅ Period {period_num}: {len(all_records)} data points, {len(all_metadata)} DUIDs")
    
    if failed_batches > 0:
        print(f"   ⚠️ Note: {failed_batches} batches failed but continuing with available data")
    
    return all_records, all_metadata

def categorize_duids(all_periods_metadata):
    """
    🆕 NEW FUNCTION: Categorize DUIDs into reference (latest period) vs decommissioned
    Reference DUIDs = present in latest period (used for main file structure)
    Decommissioned DUIDs = present in historical periods but NOT in latest period
    """
    # Get DUIDs from each period
    period_duids = {}
    for period_num, metadata in all_periods_metadata.items():
        period_duids[period_num] = set(metadata.keys())
    
    # Latest period DUIDs = reference
    latest_period = max(period_duids.keys())
    reference_duids = period_duids[latest_period]
    
    # Decommissioned = in historical periods but NOT in latest
    all_historical_duids = set()
    for period_num, duids in period_duids.items():
        if period_num < latest_period:
            all_historical_duids.update(duids)
    
    decommissioned_duids = all_historical_duids - reference_duids
    
    print(f"\n📊 DUID CATEGORIZATION:")
    print(f"   • Reference DUIDs (from latest period {latest_period}): {len(reference_duids)}")
    print(f"   • Decommissioned DUIDs (historical only): {len(decommissioned_duids)}")
    
    return reference_duids, decommissioned_duids, latest_period

def create_output_files(all_records, all_periods_metadata, reference_duids, decommissioned_duids, latest_period):
    """
    ✅ FIXED: Create two output files - now includes ALL decommissioned DUIDs
    Main file: Reference DUIDs with full time series
    Decommissioned file: ALL historical DUIDs (even with minimal data)
    """
    df = pd.DataFrame(all_records)
    if not df.empty:
        # 🧮 Add 'month' for grouping (same as your original code)
        df["timestamp"] = pd.to_datetime(df["timestamp"])  
        df["month"] = df["timestamp"].dt.to_period("M").astype(str)

        # 🗃️ Pivot into matrix format - aggregating by month (same as your original code)
        print("🔄 Aggregating data by month...")
        monthly_df = df.groupby(["month", "key"])["value"].sum().unstack(fill_value=0)
        
        # Get all unique months for consistent time series
        all_months = sorted(df["month"].unique())
    else:
        print("⚠️ No data to aggregate")
        return
    
    # === MAIN FILE: Reference DUIDs ===
    reference_columns = [col for col in monthly_df.columns if col in reference_duids]
    main_df = monthly_df[reference_columns]
    
    # Use latest period metadata for headers
    latest_metadata = all_periods_metadata[latest_period]
    
    # Show region breakdown
    if reference_columns:
        region_counts = {}
        storage_count = 0
        for col in reference_columns:
            if col in latest_metadata:
                region = latest_metadata[col].get('Region', 'Unknown')
                region_counts[region] = region_counts.get(region, 0) + 1
                
                # Count storage facilities
                storage_capacity = latest_metadata[col].get('Storage_Capacity_MWh', 'N/A')
                if storage_capacity != 'N/A' and storage_capacity != '' and storage_capacity != 0:
                    storage_count += 1
        
        print(f"📊 Region breakdown:")
        for region, count in sorted(region_counts.items()):
            print(f"   • {region}: {count} DUIDs")
        
        print(f"🔋 Storage facilities in output: {storage_count}")
    
    # 🏷️ Add enhanced metadata as header rows
    has_enhanced_data = any('Owner' in meta for meta in latest_metadata.values())

    if has_enhanced_data:
        meta_fields = ["DUID", "Name", "Facility", "Region", "Fueltech", 
                       "Owner", "Number_of_Units", "Nameplate_Capacity_MW", 
                       "Storage_Capacity_MWh", "Expected_Closure_Year"]
    else:
        meta_fields = ["DUID", "Name", "Facility", "Region", "Fueltech"]

    main_meta_rows = []
    for field in meta_fields:
        row = {}
        for col in main_df.columns:
            if col in latest_metadata:
                value = latest_metadata[col].get(field, "N/A")
                if pd.isna(value):
                    value = "N/A"
                row[col] = value
            else:
                row[col] = "N/A"
        main_meta_rows.append(row)

    main_meta_df = pd.DataFrame(main_meta_rows, index=meta_fields)
    main_separator = pd.DataFrame(index=["---"], columns=main_df.columns)
    main_final_df = pd.concat([main_meta_df, main_separator, main_df])
    
    # 💾 Save file with configurable name
    main_filename = f"{consolidated_filename}.csv"
    main_final_df.to_csv(main_filename)
    print(f"✅ Enhanced file saved: {main_filename}")
    
    # === ✅ FIXED DECOMMISSIONED FILE: Include ALL Historical DUIDs ===
    if decommissioned_duids:
        print(f"🔄 Creating decommissioned file with ALL {len(decommissioned_duids)} historical DUIDs...")
        
        # Get columns that exist in monthly_df
        existing_decomm_columns = [col for col in monthly_df.columns if col in decommissioned_duids]
        
        # Get DUIDs that were identified as decommissioned but don't have data in monthly_df  
        missing_decomm_duids = decommissioned_duids - set(existing_decomm_columns)
        
        print(f"   • DUIDs with data: {len(existing_decomm_columns)}")
        print(f"   • DUIDs with minimal/no data: {len(missing_decomm_duids)}")
        
        # Start with existing data
        if existing_decomm_columns:
            decomm_df = monthly_df[existing_decomm_columns].copy()
        else:
            # Create empty dataframe with correct months
            decomm_df = pd.DataFrame(index=all_months)
        
        # ✅ ADD MISSING DUIDs: Add columns for DUIDs that don't appear in monthly_df
        for missing_duid in missing_decomm_duids:
            decomm_df[missing_duid] = 0  # Fill with zeros since they had no data
        
        # Ensure all decommissioned DUIDs are now included
        all_decomm_columns = list(decommissioned_duids)
        decomm_df = decomm_df.reindex(columns=all_decomm_columns, fill_value=0)
        
        # Get metadata from the period where each DUID last appeared
        decomm_metadata = {}
        for duid in decommissioned_duids:
            # Find latest period where this DUID appeared
            for period_num in sorted(all_periods_metadata.keys(), reverse=True):
                if duid in all_periods_metadata[period_num]:
                    decomm_metadata[duid] = all_periods_metadata[period_num][duid]
                    break
            
            # If no metadata found, create basic entry
            if duid not in decomm_metadata:
                decomm_metadata[duid] = {
                    "DUID": duid,
                    "Name": duid,
                    "Facility": "Unknown",
                    "Region": "Unknown", 
                    "Fueltech": "Unknown",
                    "Owner": "Unknown",
                    "Number_of_Units": "N/A",
                    "Nameplate_Capacity_MW": "N/A",
                    "Storage_Capacity_MWh": "N/A",
                    "Expected_Closure_Year": "N/A"
                }
        
        # Create decommissioned metadata rows
        decomm_meta_rows = []
        for field in meta_fields:
            row = {}
            for col in decomm_df.columns:
                if col in decomm_metadata:
                    value = decomm_metadata[col].get(field, "N/A")
                    if pd.isna(value):
                        value = "N/A"
                    row[col] = value
                else:
                    row[col] = "N/A"
            decomm_meta_rows.append(row)
        
        decomm_meta_df = pd.DataFrame(decomm_meta_rows, index=meta_fields)
        decomm_separator = pd.DataFrame(index=["---"], columns=decomm_df.columns)
        decomm_final_df = pd.concat([decomm_meta_df, decomm_separator, decomm_df])
        
        # Save decommissioned file
        decomm_filename = f"{decommissioned_filename}.csv"
        decomm_final_df.to_csv(decomm_filename)
        print(f"✅ Decommissioned file saved: {decomm_filename} ({len(decomm_df.columns)} DUIDs)")
    else:
        print("✅ No decommissioned DUIDs found")

def main():
    """Main execution function"""
    # === MAIN LOGIC ===
    print("🔄 Loading NEM reference data...")
    duid_lookup = load_nem_reference_data()

    # 🆕 NEW: Calculate periods (auto-handles API limit)
    periods = calculate_date_periods(start_month, start_year, end_month, end_year)

    # Display filename configuration
    print(f"📁 Output filename: {consolidated_filename}.csv")
    if len(periods) > 1:
        print(f"📁 Decommissioned filename: {decommissioned_filename}.csv")
    print("💰 Values will be in millions of AUD (divided by 1,000,000)")

    # Display filter settings
    if REGION_FILTER:
        print(f"🏞️ Region filter active: {', '.join(REGION_FILTER)}")
    else:
        print("🏞️ No region filter - including all regions")

    print("🔄 Fetching facility codes...")
    facility_codes = fetch_all_facility_codes(API_KEY)
    print(f"✅ Retrieved {len(facility_codes)} facilities")

    # 🆕 NEW: Fetch data for all periods with robust error handling
    all_records = []
    all_periods_metadata = {}

    for period_num, (start_date, end_date) in enumerate(periods, 1):
        try:
            records, metadata = fetch_data_for_period(facility_codes, METRIC, duid_lookup, start_date, end_date, period_num)
            all_records.extend(records)
            all_periods_metadata[period_num] = metadata
            
        except Exception as e:
            print(f"❌ Period {period_num} failed with error: {str(e)}")
            print(f"⚠️ Continuing with data from completed periods...")
            break

    if not all_records:
        print("⚠️ No data returned.")
        return

    print(f"\n✅ Total data retrieved: {len(all_records)} records across {len(set(r['period'] for r in all_records))} periods")

    if len(periods) == 1:
        # Single period - create output like original code
        df = pd.DataFrame(all_records)
        metadata = all_periods_metadata[1]
        
        # 🧮 Add 'month' for grouping (same as your original code)
        df["timestamp"] = pd.to_datetime(df["timestamp"])  
        df["month"] = df["timestamp"].dt.to_period("M").astype(str)

        # 🗃️ Pivot into matrix format - aggregating by month (same as your original code)
        print("🔄 Aggregating data by month...")
        monthly_df = df.groupby(["month", "key"])["value"].sum().unstack(fill_value=0)

        # 🔧 FIXED FILTERING LOGIC - Remove the is_matched restriction that was filtering out DUIDs
        print("🔄 Filtering out N/A DUIDs...")
        valid_columns = []
        for col in monthly_df.columns:
            if col in metadata:
                duid = metadata[col].get('DUID', 'N/A')
                # ✅ FIXED: Only check if DUID is not N/A, don't require NEM match
                # This allows DUIDs that only have API data to be included
                if duid != 'N/A':
                    valid_columns.append(col)

        monthly_df = monthly_df[valid_columns]
        print(f"✅ Filtered matrix: {monthly_df.shape[0]} months × {monthly_df.shape[1]} DUIDs")

        # Show region breakdown
        if valid_columns:
            region_counts = {}
            storage_count = 0
            for col in valid_columns:
                if col in metadata:
                    region = metadata[col].get('Region', 'Unknown')
                    region_counts[region] = region_counts.get(region, 0) + 1
                    
                    # Count storage facilities
                    storage_capacity = metadata[col].get('Storage_Capacity_MWh', 'N/A')
                    if storage_capacity != 'N/A' and storage_capacity != '' and storage_capacity != 0:
                        storage_count += 1
            
            print(f"📊 Region breakdown:")
            for region, count in sorted(region_counts.items()):
                print(f"   • {region}: {count} DUIDs")
            
            print(f"🔋 Storage facilities in output: {storage_count}")

        # Alternative aggregation option (uncomment if you want averages instead of sums):
        # monthly_df = df.groupby(["month", "key"])["value"].mean().unstack(fill_value=0)

        # 🏷️ Add enhanced metadata as header rows (removed Metric field)
        # Check if we have enhanced data
        has_enhanced_data = any('Owner' in meta for meta in metadata.values())

        if has_enhanced_data:
            meta_fields = ["DUID", "Name", "Facility", "Region", "Fueltech", 
                           "Owner", "Number_of_Units", "Nameplate_Capacity_MW", 
                           "Storage_Capacity_MWh", "Expected_Closure_Year"]
        else:
            meta_fields = ["DUID", "Name", "Facility", "Region", "Fueltech"]

        meta_rows = []
        for field in meta_fields:
            row = {}
            for col in monthly_df.columns:
                if col in metadata:
                    value = metadata[col].get(field, "N/A")
                    # ✅ ADDITIONAL FIX: Handle pandas NaN values that might cause issues
                    if pd.isna(value):
                        value = "N/A"
                    row[col] = value
                else:
                    row[col] = "N/A"
            meta_rows.append(row)

        meta_df = pd.DataFrame(meta_rows, index=meta_fields)
        separator = pd.DataFrame(index=["---"], columns=monthly_df.columns)
        final_df = pd.concat([meta_df, separator, monthly_df])

        # 💾 Save file with configurable name
        filename = f"{consolidated_filename}.csv"
        final_df.to_csv(filename)
        print(f"\n✅ Enhanced file saved: {filename}")

        # 📊 Generate summary report with storage info
        matched_duids = len([key for key in metadata.keys() if key in duid_lookup])
        total_duids = len(metadata)
        filtered_duids = len(valid_columns)

        print(f"\n📊 SUMMARY REPORT:")
        print(f"   • File saved as: {filename}")
        print(f"   • Values converted to millions of AUD (÷ 1,000,000)")
        print(f"   • Region filter: {', '.join(REGION_FILTER) if REGION_FILTER else 'None (all regions)'}")
        print(f"   • Total DUIDs from API: {total_duids}")
        print(f"   • DUIDs matched with NEM data: {matched_duids}")
        print(f"   • DUIDs included in final output: {filtered_duids}")
        print(f"   • Match rate: {(matched_duids/total_duids*100):.1f}%" if total_duids > 0 else "   • Match rate: 0%")
        print(f"   • NEM reference data loaded: {len(duid_lookup)} DUIDs")

    else:
        # Multiple periods - use new logic
        # 🆕 NEW: Categorize DUIDs
        reference_duids, decommissioned_duids, latest_period = categorize_duids(all_periods_metadata)

        # 🆕 NEW: Create output files
        create_output_files(all_records, all_periods_metadata, reference_duids, decommissioned_duids, latest_period)

        # ✅ ENHANCED SUMMARY REPORTING
        matched_duids = len([key for period_meta in all_periods_metadata.values() for key in period_meta.keys() if key in duid_lookup])
        total_duids = len(set(key for period_meta in all_periods_metadata.values() for key in period_meta.keys()))
        
        # Count how many decommissioned DUIDs actually have data
        df = pd.DataFrame(all_records)
        if not df.empty:
            df["month"] = pd.to_datetime(df["timestamp"]).dt.to_period("M").astype(str)
            monthly_df = df.groupby(["month", "key"])["value"].sum().unstack(fill_value=0)
            decomm_with_data = len([col for col in monthly_df.columns if col in decommissioned_duids])
        else:
            decomm_with_data = 0

        print(f"\n📊 SUMMARY REPORT:")
        print(f"   • Files saved as: {consolidated_filename}.csv, {decommissioned_filename}.csv")
        print(f"   • Values converted to millions of AUD (÷ 1,000,000)")
        print(f"   • Date range processed: {periods[0][0].strftime('%Y-%m-%d')} to {periods[-1][1].strftime('%Y-%m-%d')}")
        print(f"   • Periods processed: {len(set(r['period'] for r in all_records))}/{len(periods)}")
        print(f"   • Region filter: {', '.join(REGION_FILTER) if REGION_FILTER else 'None (all regions)'}")
        print(f"   • Reference DUIDs (main file): {len(reference_duids)}")
        print(f"   • Decommissioned DUIDs: {len(decommissioned_duids)}")
        print(f"   • Total unique DUIDs: {total_duids}")
        print(f"   • NEM reference data loaded: {len(duid_lookup)} DUIDs")

if __name__ == "__main__":
    main()
