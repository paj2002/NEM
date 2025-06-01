# Enhanced Open Electricity Data Puller with NEM Data Integration
# Will need the DUID excel from AEMO Generation Information EXCEL

import requests
import pandas as pd
from datetime import datetime
import time
import openpyxl

# ==================== USER CONFIGURATION ====================

# Output file names (without .csv extension) --> Change name for adhoc analysis and don't disturb workflow
# Default workflow name --> 'ALLSTATES_Revenue'
consolidated_filename = 'ALLSTATES_Revenue'

# API Key (get from https://platform.openelectricity.org.au)
API_KEY = "oe_3ZbuDQVhMCk1guoQqd7eBcWi"

# 🌐 Network code (market you want data from)
# - "NEM" → National Electricity Market (eastern Australia)
# - "WEM" → Western Australia
# - "AEMO_ROOFTOP" → Rooftop PV estimates
# - "APVI" → Community PV data
NETWORK_CODE = "NEM"

# 🏞️ REGION FILTER - Filter by specific regions/states
REGION_FILTER = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
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

# ==================== DATE CONFIGURATION ====================
# Specify the date range - just month and year!
start_month = 4    
start_year = 2025

end_month = 4      
end_year = 2025
# ===========================================================

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

print(f"📅 Date range: {startdate} to {enddate}")

# Convert to API format
start_dt = datetime.strptime(startdate, "%Y-%m-%d")
end_dt = datetime.strptime(enddate, "%Y-%m-%d")
DATE_START = start_dt.strftime("%Y-%m-%dT00:00:00")
DATE_END = end_dt.strftime("%Y-%m-%dT00:00:00")

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
    url = "https://api.openelectricity.org.au/v4/facilities/"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"network_id": network_code, "with_clerk": "true"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"❌ Error fetching facilities: {response.status_code}")
        return []
    return [f["code"] for f in response.json().get("data", []) if len(f["code"]) < 30]

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
def fetch_data_for_facilities(facility_codes, metric, duid_lookup):
    all_records = []
    all_metadata = {}

    headers = {"Authorization": f"Bearer {API_KEY}"}
    base_url = f"https://api.openelectricity.org.au/v4/data/facilities/{NETWORK_CODE}"
    BATCH_SIZE = 20

    for i in range(0, len(facility_codes), BATCH_SIZE):
        batch = facility_codes[i:i + BATCH_SIZE]
        params = {
            "facility_code": batch,
            "metrics": [metric],
            "interval": INTERVAL,
            "date_start": DATE_START,
            "date_end": DATE_END,
            "with_clerk": "true"
        }

        print(f"📦 Fetching batch {i//BATCH_SIZE + 1} of {len(facility_codes) // BATCH_SIZE + 1}")
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code}: {response.text}")
            continue

        data = response.json()
        for facility_block in data.get("data", []):
            facility_code = facility_block.get("facility_code", "N/A")
            facility_region = facility_block.get("network_region", "N/A")
            facility_fueltech = facility_block.get("fueltech_id", "N/A")

            for result in facility_block.get("results", []):
                duid = result["columns"].get("unit_code", "N/A")
                name = result.get("name", duid)
                metric_name = result.get("metric", "N/A")
                key = duid  # Use just the DUID as the key

                # Skip records where DUID is N/A
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
                
                # Merge with NEM reference data if available
                if duid in duid_lookup:
                    nem_data = duid_lookup[duid]
                    # Use NEM data to fill in missing/generic values
                    enhanced_metadata = {
                        "DUID": duid,
                        "Name": name,
                        "Facility": nem_data.get('Facility', facility_code),
                        "Region": nem_data.get('Region', facility_region),
                        "Fueltech": nem_data.get('Fueltech', facility_fueltech),
                        "Owner": nem_data.get('Owner', 'N/A'),
                        "Number_of_Units": nem_data.get('Number_of_Units', 'N/A'),
                        "Nameplate_Capacity_MW": nem_data.get('Nameplate_Capacity_MW', 'N/A'),
                        "Storage_Capacity_MWh": nem_data.get('Storage_Capacity_MWh', 'N/A'),
                        "Expected_Closure_Year": nem_data.get('Expected_Closure_Year', 'N/A')
                    }
                    all_metadata[key] = enhanced_metadata
                else:
                    # Use API data only with enhanced fields set to N/A
                    enhanced_metadata = base_metadata.copy()
                    enhanced_metadata.update({
                        "Owner": 'N/A',
                        "Number_of_Units": 'N/A',
                        "Nameplate_Capacity_MW": 'N/A',
                        "Storage_Capacity_MWh": 'N/A',
                        "Expected_Closure_Year": 'N/A'
                    })
                    all_metadata[key] = enhanced_metadata

                # Check if this DUID should be included based on region filter
                if not should_include_duid(duid, all_metadata, duid_lookup, REGION_FILTER):
                    continue

                # Process numerical data - DIVIDE BY 1,000,000 HERE
                for timestamp, value in result.get("data", []):
                    # Convert value to millions (divide by 1,000,000)
                    value_in_millions = value / 1_000_000 if value is not None else 0
                    
                    all_records.append({
                        "timestamp": timestamp[:10],  # Extract date part only
                        "key": key,
                        "value": value_in_millions  # Now in millions
                    })

        time.sleep(0.3)  # Friendly pause to avoid rate limits

    return all_records, all_metadata

def main():
    """Main execution function"""
    # === MAIN LOGIC ===
    print("🔄 Loading NEM reference data...")
    duid_lookup = load_nem_reference_data()

    # Display filename configuration
    print(f"📁 Output filename: {consolidated_filename}.csv")
    print("💰 Values will be in millions of AUD (divided by 1,000,000)")

    # Display filter settings
    if REGION_FILTER:
        print(f"🏞️ Region filter active: {', '.join(REGION_FILTER)}")
    else:
        print("🏞️ No region filter - including all regions")

    print("🔄 Fetching facility codes...")
    facility_codes = fetch_all_facility_codes(API_KEY)
    print(f"✅ Retrieved {len(facility_codes)} facilities")

    print("🔄 Fetching energy data...")
    records, metadata = fetch_data_for_facilities(facility_codes, METRIC, duid_lookup)

    if not records:
        print("⚠️ No data returned.")
        return

    df = pd.DataFrame(records)
    print(f"✅ Retrieved {len(records)} data points (values in millions)")

    # 🧮 Add 'month' for grouping (same as your original code)
    df["timestamp"] = pd.to_datetime(df["timestamp"])  
    df["month"] = df["timestamp"].dt.to_period("M").astype(str)

    # 🗃️ Pivot into matrix format - aggregating by month (same as your original code)
    print("🔄 Aggregating data by month...")
    monthly_df = df.groupby(["month", "key"])["value"].sum().unstack(fill_value=0)

    # Filter out columns where DUID is N/A OR not matched with NEM data
    print("🔄 Filtering out N/A DUIDs and unmatched DUIDs...")
    valid_columns = []
    for col in monthly_df.columns:
        if col in metadata:
            duid = metadata[col].get('DUID', 'N/A')
            is_matched = col in duid_lookup  # Check if DUID exists in NEM reference data
            
            if duid != 'N/A' and is_matched:
                valid_columns.append(col)

    monthly_df = monthly_df[valid_columns]
    print(f"✅ Filtered matrix: {monthly_df.shape[0]} months × {monthly_df.shape[1]} matched DUIDs")

    # Show region breakdown
    if valid_columns:
        region_counts = {}
        for col in valid_columns:
            if col in metadata:
                region = metadata[col].get('Region', 'Unknown')
                region_counts[region] = region_counts.get(region, 0) + 1
        
        print(f"📊 Region breakdown:")
        for region, count in sorted(region_counts.items()):
            print(f"   • {region}: {count} DUIDs")

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
                row[col] = metadata[col].get(field, "N/A")
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

    # 📊 Generate summary report
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

if __name__ == "__main__":
    main()
