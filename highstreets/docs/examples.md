# Examples and Tutorials

This document provides comprehensive examples and tutorials for using the Highstreets package effectively. From basic operations to advanced workflows, these examples will help you understand and implement data processing solutions.

## Table of Contents

- [Quick Start Examples](#quick-start-examples)
- [Data Loading Examples](#data-loading-examples)
- [Data Transformation Examples](#data-transformation-examples)
- [Geographic Processing Examples](#geographic-processing-examples)
- [Sub-licensing Examples](#sub-licensing-examples)
- [Advanced Workflows](#advanced-workflows)
- [Performance Optimization](#performance-optimization)
- [Error Handling Examples](#error-handling-examples)
- [Custom Extensions](#custom-extensions)

## Quick Start Examples

### 1. Basic Package Setup

```python
"""
Basic setup and verification of the Highstreets package
"""
import os
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.hextransform import HexTransform
from highstreets.api.clientbase import APIClient

def setup_environment():
    """Initialize and verify package components"""
    
    # Set environment variables (normally in .env file)
    os.environ['PG_HOST'] = 'localhost'
    os.environ['PG_DATABASE'] = 'highstreets'
    os.environ['PG_USER'] = 'your_user'
    os.environ['PG_PASSWORD'] = 'your_password'
    os.environ['CONSUMER_KEY'] = 'your_bt_key'
    os.environ['CONSUMER_SECRET'] = 'your_bt_secret'
    
    # Initialize components
    loader = DataLoader()
    writer = DataWriter()
    transformer = HexTransform()
    api_client = APIClient()
    
    print("✓ All components initialized successfully")
    return loader, writer, transformer, api_client

if __name__ == "__main__":
    loader, writer, transformer, api_client = setup_environment()
```

### 2. Simple Data Processing

```python
"""
Simple end-to-end data processing example
"""
def process_one_day_data(date):
    """Process one day of BT footfall data"""
    
    loader = DataLoader()
    transformer = HexTransform()
    writer = DataWriter()
    
    try:
        # 1. Load data from API
        print(f"Loading data for {date}...")
        raw_data = loader.get_hex_data(date, date)
        print(f"Loaded {len(raw_data)} records")
        
        # 2. Transform data
        print("Transforming data...")
        transformed_data = transformer.transform_data(raw_data)
        print(f"Transformed {len(transformed_data)} records")
        
        # 3. Save to database
        print("Saving to database...")
        writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")
        print("✓ Processing completed successfully")
        
        return transformed_data
        
    except Exception as e:
        print(f"✗ Processing failed: {e}")
        return None

# Example usage
if __name__ == "__main__":
    result = process_one_day_data("2023-12-01")
    if result is not None:
        print(f"Final dataset shape: {result.shape}")
```

## Data Loading Examples

### 1. Loading Different Data Types

```python
"""
Examples of loading different types of data
"""
import pandas as pd
from datetime import datetime, timedelta

def load_various_data_types():
    """Demonstrate loading different BT data types"""
    
    loader = DataLoader()
    start_date = "2023-01-01"
    end_date = "2023-01-07"
    
    # Load different data types
    data_types = {
        'hex_data': loader.get_hex_data(start_date, end_date),
        'lsoa_data': loader.get_lsoa_data(start_date, end_date),
        'msoa_data': loader.get_msoa_data(start_date, end_date),
        'daily_data': loader.get_bt_daily_aggregate_customer_shapes(start_date, end_date)
    }
    
    # Display information about each data type
    for data_type, data in data_types.items():
        if data:
            print(f"{data_type}: {len(data)} records")
            # Show first record structure
            if isinstance(data, list) and data:
                print(f"  Sample fields: {list(data[0].keys())}")
        else:
            print(f"{data_type}: No data returned")
    
    return data_types

def load_data_with_date_iteration():
    """Load data day by day to handle large date ranges"""
    
    loader = DataLoader()
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    
    all_data = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Loading data for {date_str}...")
        
        try:
            daily_data = loader.get_hex_data(date_str, date_str)
            if daily_data:
                all_data.extend(daily_data)
                print(f"  ✓ {len(daily_data)} records loaded")
            else:
                print(f"  ⚠ No data for {date_str}")
                
        except Exception as e:
            print(f"  ✗ Error loading {date_str}: {e}")
        
        current_date += timedelta(days=1)
    
    print(f"Total records loaded: {len(all_data)}")
    return all_data

# Example usage
if __name__ == "__main__":
    # Load various data types
    data_types = load_various_data_types()
    
    # Load data iteratively
    monthly_data = load_data_with_date_iteration()
```

### 2. Database Data Loading

```python
"""
Examples of loading data from the database
"""
def load_database_data_examples():
    """Various ways to load data from PostgreSQL"""
    
    loader = DataLoader()
    
    # 1. Load complete tables
    print("Loading complete tables...")
    hex_data = loader.get_full_data('bt_footfall_tfl_hex_3hourly')
    print(f"Hex data: {len(hex_data)} records")
    
    # 2. Load lookup tables
    print("Loading lookup tables...")
    hex_hs_lookup = loader.get_full_data('econ_busyness_hex_highstreet_lookup')
    hex_tc_lookup = loader.get_full_data('econ_busyness_hex_towncentre_lookup')
    
    print(f"Hex-Highstreet lookup: {len(hex_hs_lookup)} mappings")
    print(f"Hex-TownCentre lookup: {len(hex_tc_lookup)} mappings")
    
    # 3. Load partial data with custom queries
    print("Loading partial data...")
    recent_data = loader.get_partial_data(
        table_name='bt_footfall_tfl_hex_3hourly',
        columns=['hex_id', 'count_date', 'visitor', 'resident'],
        where_clause="count_date >= '2023-12-01' AND visitor > 100"
    )
    print(f"Recent high-activity data: {len(recent_data)} records")
    
    return {
        'hex_data': hex_data,
        'hex_hs_lookup': hex_hs_lookup,
        'hex_tc_lookup': hex_tc_lookup,
        'recent_data': recent_data
    }

def load_aggregated_data():
    """Load pre-aggregated geographic data"""
    
    loader = DataLoader()
    
    # Load aggregated data by geography type
    aggregated_data = {
        'highstreets': loader.get_full_data('econ_busyness_bt_highstreets_3hourly_counts'),
        'towncentres': loader.get_full_data('econ_busyness_bt_towncentres_3hourly_counts'),
        'bids': loader.get_full_data('econ_busyness_bt_bids_3hourly_counts'),
        'bespoke': loader.get_full_data('econ_busyness_bt_bespokes_3hourly_counts')
    }
    
    # Display summary statistics
    for geo_type, data in aggregated_data.items():
        if data is not None and not data.empty:
            print(f"{geo_type.title()}:")
            print(f"  Records: {len(data)}")
            print(f"  Date range: {data['count_date'].min()} to {data['count_date'].max()}")
            print(f"  Areas: {data.iloc[:, 0].nunique()}")  # First column is usually the ID
            print()
    
    return aggregated_data

# Example usage
if __name__ == "__main__":
    # Load database data examples
    db_data = load_database_data_examples()
    
    # Load aggregated data
    agg_data = load_aggregated_data()
```

## Data Transformation Examples

### 1. Basic Data Transformation

```python
"""
Basic data transformation examples
"""
import pandas as pd
from highstreets.data_transformation.hextransform import HexTransform
from highstreets.data_transformation.lsoatransform import LsoaTransform
from highstreets.data_transformation.msoatransform import MsoaTransform

def transform_hex_data_example():
    """Transform raw hex data into standardized format"""
    
    # Sample raw data (normally from API)
    raw_data = [
        {
            "hex_id": "hex_001",
            "date": "2023-01-01",
            "time_indicator": "09-12",
            "total_volume": 1000,
            "worker_pop_percent": 30.0,
            "resident_pop_percent": 40.0,
            "loyalty_percentage": 65.0,
            "dwell_time": 45.5
        },
        {
            "hex_id": "hex_001", 
            "date": "2023-01-01",
            "time_indicator": "12-15",
            "total_volume": 1200,
            "worker_pop_percent": 35.0,
            "resident_pop_percent": 35.0,
            "loyalty_percentage": 70.0,
            "dwell_time": 50.2
        }
    ]
    
    transformer = HexTransform()
    
    # Transform the data
    transformed = transformer.transform_data(raw_data)
    
    print("Transformed Data Structure:")
    print(transformed.dtypes)
    print("\nSample Records:")
    print(transformed.head())
    
    return transformed

def transform_geographic_levels():
    """Transform data for different geographic levels"""
    
    loader = DataLoader()
    transformer = HexTransform()
    
    # Get some sample hex data
    hex_data = loader.get_hex_data("2023-01-01", "2023-01-03")
    transformed_hex = transformer.transform_data(hex_data)
    
    # Transform to different geographic levels
    transformations = {}
    
    # High streets transformation
    try:
        hs_data = transformer.highstreet_threehourly_transform(transformed_hex)
        transformations['highstreets'] = hs_data
        print(f"✓ High streets: {len(hs_data)} records")
    except Exception as e:
        print(f"✗ High streets transformation failed: {e}")
    
    # Town centres transformation  
    try:
        tc_data = transformer.towncentre_threehourly_transform(transformed_hex)
        transformations['towncentres'] = tc_data
        print(f"✓ Town centres: {len(tc_data)} records")
    except Exception as e:
        print(f"✗ Town centres transformation failed: {e}")
    
    # BIDs transformation
    try:
        bid_data = transformer.bid_threehourly_transform(transformed_hex)
        transformations['bids'] = bid_data
        print(f"✓ BIDs: {len(bid_data)} records")
    except Exception as e:
        print(f"✗ BIDs transformation failed: {e}")
    
    return transformations

# Example usage
if __name__ == "__main__":
    # Basic transformation
    transformed_data = transform_hex_data_example()
    
    # Geographic transformations
    geo_transformations = transform_geographic_levels()
```

### 2. Mastercard Data Transformation

```python
"""
Mastercard transaction data transformation examples
"""
from highstreets.data_transformation.mcard_transform import McardTransform

def mastercard_basic_transformation():
    """Basic Mastercard data processing"""
    
    # Sample transaction data
    sample_data = pd.DataFrame({
        'quad_id': ['Q001', 'Q001', 'Q002'],
        'count_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-01']),
        'hours': ['09-12', '09-12', '12-15'],
        'txn_amt': [1500.50, 1750.25, 980.75],
        'txn_cnt': [25, 30, 18],
        'segment': ['Overall', 'Overall', 'Overall']
    })
    
    mcard_transformer = McardTransform()
    
    # Preprocess the data
    processed_data = mcard_transformer.preprocess_mcard_data(sample_data)
    
    print("Processed Mastercard Data:")
    print(processed_data.head())
    
    return processed_data

def mastercard_inflation_adjustment():
    """Apply inflation adjustments to transaction data"""
    
    # Sample spending data
    spending_data = pd.DataFrame({
        'count_date': pd.date_range('2022-01-01', periods=12, freq='M'),
        'txn_amt': [10000, 10500, 11000, 10800, 11200, 11500, 
                   12000, 12200, 12500, 12800, 13000, 13200],
        'quad_id': ['Q001'] * 12
    })
    
    # Sample CPI data
    cpi_data = pd.DataFrame({
        'yr': [2022] * 12 + [2023] * 12,
        'month': list(range(1, 13)) * 2,
        'cpi_index': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
                     112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123]
    })
    
    mcard_transformer = McardTransform()
    
    # Apply inflation adjustment
    adjusted_data = mcard_transformer.inflation_adjust(
        spend=spending_data,
        cpi_table=cpi_data,
        reindexing_year=2022,
        col_to_adjust=['txn_amt']
    )
    
    print("Inflation Adjusted Data:")
    print(adjusted_data[['count_date', 'txn_amt', 'txn_amt_adj']].head(10))
    
    return adjusted_data

def mastercard_geographic_aggregation():
    """Aggregate Mastercard data by geographic boundaries"""
    
    mcard_transformer = McardTransform()
    
    # Process data for different geographic levels
    geographic_types = [
        ('quad_hs_transform_query.sql', 'econ_busyness_mcard_highstreets_3hourly_txn'),
        ('quad_tc_transform_query.sql', 'econ_busyness_mcard_towncentres_3hourly_txn'),
        ('quad_bid_transform_query.sql', 'econ_busyness_mcard_bids_3hourly_txn'),
        ('quad_bespoke_transform_query.sql', 'econ_busyness_mcard_bespokes_3hourly_txn')
    ]
    
    for transform_query, table_name in geographic_types:
        try:
            print(f"Processing {table_name}...")
            mcard_transformer.fetch_and_transform_mcard_data(
                transform_layer=transform_query,
                table_name=table_name,
                truncate=True,
                load_to_db=True
            )
            print(f"✓ {table_name} completed")
            
        except Exception as e:
            print(f"✗ {table_name} failed: {e}")

# Example usage
if __name__ == "__main__":
    # Basic transformation
    basic_data = mastercard_basic_transformation()
    
    # Inflation adjustment
    adjusted_data = mastercard_inflation_adjustment()
    
    # Geographic aggregation
    mastercard_geographic_aggregation()
```

## Geographic Processing Examples

### 1. Spatial Analysis

```python
"""
Geographic and spatial analysis examples
"""
import geopandas as gpd
from highstreets.data_source_sink.lookup_manager import LookupManager

def analyze_geographic_coverage():
    """Analyze geographic coverage of hex grids"""
    
    loader = DataLoader()
    
    # Load lookup tables
    hex_hs_lookup = loader.get_full_data('econ_busyness_hex_highstreet_lookup')
    hex_tc_lookup = loader.get_full_data('econ_busyness_hex_towncentre_lookup')
    hex_bid_lookup = loader.get_full_data('econ_busyness_hex_bid_lookup')
    
    # Analyze coverage
    analysis = {}
    
    # High streets coverage
    if hex_hs_lookup is not None:
        total_hs = hex_hs_lookup['highstreet_id'].nunique()
        total_hex_hs = hex_hs_lookup['hex_id'].nunique()
        analysis['highstreets'] = {
            'total_areas': total_hs,
            'total_hex_cells': total_hex_hs,
            'avg_hex_per_area': total_hex_hs / total_hs if total_hs > 0 else 0
        }
    
    # Town centres coverage
    if hex_tc_lookup is not None:
        total_tc = hex_tc_lookup['tc_id'].nunique()
        total_hex_tc = hex_tc_lookup['hex_id'].nunique()
        analysis['towncentres'] = {
            'total_areas': total_tc,
            'total_hex_cells': total_hex_tc,
            'avg_hex_per_area': total_hex_tc / total_tc if total_tc > 0 else 0
        }
    
    # BIDs coverage
    if hex_bid_lookup is not None:
        total_bid = hex_bid_lookup['bid_id'].nunique()
        total_hex_bid = hex_bid_lookup['hex_id'].nunique()
        analysis['bids'] = {
            'total_areas': total_bid,
            'total_hex_cells': total_hex_bid,
            'avg_hex_per_area': total_hex_bid / total_bid if total_bid > 0 else 0
        }
    
    # Display analysis
    for geo_type, stats in analysis.items():
        print(f"{geo_type.title()}:")
        print(f"  Areas: {stats['total_areas']}")
        print(f"  Hex cells: {stats['total_hex_cells']}")
        print(f"  Avg hex per area: {stats['avg_hex_per_area']:.1f}")
        print()
    
    return analysis

def create_custom_geographic_areas():
    """Create custom geographic area definitions"""
    
    # Define custom areas by hex IDs
    custom_areas = pd.DataFrame({
        'hex_id': ['hex_001', 'hex_002', 'hex_003', 'hex_004', 'hex_005'],
        'custom_area_id': [1, 1, 2, 2, 3],
        'custom_area_name': ['Central Area', 'Central Area', 'North Area', 'North Area', 'South Area'],
        'area_type': ['commercial', 'commercial', 'residential', 'residential', 'mixed']
    })
    
    # Load hex data
    loader = DataLoader()
    hex_data = loader.get_full_data('bt_footfall_tfl_hex_3hourly')
    
    if hex_data is not None and not hex_data.empty:
        # Merge with custom areas
        custom_aggregated = hex_data.merge(
            custom_areas, 
            on='hex_id', 
            how='inner'
        )
        
        # Aggregate by custom areas
        result = custom_aggregated.groupby([
            'custom_area_id', 'custom_area_name', 'area_type', 
            'count_date', 'hours'
        ]).agg({
            'resident': 'sum',
            'visitor': 'sum',
            'worker': 'sum',
            'loyalty_percentage': 'mean',
            'dwell_time': 'mean'
        }).reset_index()
        
        print(f"Custom aggregation created: {len(result)} records")
        print(f"Custom areas: {result['custom_area_name'].unique()}")
        
        return result
    
    return None

def analyze_area_characteristics():
    """Analyze characteristics of different area types"""
    
    loader = DataLoader()
    
    # Load aggregated data for different geographic types
    data_sources = {
        'highstreets': 'econ_busyness_bt_highstreets_3hourly_counts',
        'towncentres': 'econ_busyness_bt_towncentres_3hourly_counts', 
        'bids': 'econ_busyness_bt_bids_3hourly_counts'
    }
    
    analysis_results = {}
    
    for area_type, table_name in data_sources.items():
        data = loader.get_full_data(table_name)
        
        if data is not None and not data.empty:
            # Calculate statistics
            stats = {
                'total_areas': data.iloc[:, 0].nunique(),  # First column is usually the ID
                'avg_daily_visitors': data.groupby(data.columns[0])['visitor'].sum().mean(),
                'avg_daily_residents': data.groupby(data.columns[0])['resident'].sum().mean(),
                'avg_daily_workers': data.groupby(data.columns[0])['worker'].sum().mean(),
                'peak_hour': data.groupby('hours')['visitor'].sum().idxmax(),
                'date_range': f"{data['count_date'].min()} to {data['count_date'].max()}"
            }
            
            analysis_results[area_type] = stats
            
            print(f"{area_type.title()} Analysis:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
            print()
    
    return analysis_results

# Example usage
if __name__ == "__main__":
    # Analyze geographic coverage
    coverage_analysis = analyze_geographic_coverage()
    
    # Create custom areas
    custom_areas = create_custom_geographic_areas()
    
    # Analyze area characteristics
    area_analysis = analyze_area_characteristics()
```

### 2. Lookup Table Management

```python
"""
Lookup table creation and management examples
"""
from highstreets.data_source_sink.lookup_manager import LookupManager

def create_lookup_tables():
    """Create and update lookup tables"""
    
    lookup_manager = LookupManager()
    
    print("Generating lookup tables...")
    
    # Generate all quad lookups for Mastercard data
    try:
        lookup_manager.generate_all_quad_lookups()
        print("✓ Quad lookup tables generated")
    except Exception as e:
        print(f"✗ Quad lookup generation failed: {e}")
    
    # Process borough-highstreet lookup
    try:
        borough_hs_lookup = lookup_manager.process_borough_hs_lookup()
        print(f"✓ Borough-HS lookup: {len(borough_hs_lookup)} records")
    except Exception as e:
        print(f"✗ Borough-HS lookup failed: {e}")
    
    # Generate specific layer lookups
    layer_types = ['highstreets', 'towncentres', 'bids', 'bespoke']
    
    for layer in layer_types:
        try:
            lookup_data = lookup_manager.get_query_context([layer])
            if lookup_data is not None:
                print(f"✓ {layer.title()} lookup: {len(lookup_data)} areas")
            else:
                print(f"⚠ {layer.title()} lookup: No data")
        except Exception as e:
            print(f"✗ {layer.title()} lookup failed: {e}")

def update_hex_lookups():
    """Update hex grid lookup tables"""
    
    lookup_manager = LookupManager()
    data_writer = DataWriter()
    
    lookup_types = ['highstreet', 'towncentre', 'bid', 'bespoke']
    
    for lookup_type in lookup_types:
        try:
            print(f"Updating {lookup_type} hex lookup...")
            
            # Get lookup data
            lookup_data = lookup_manager.get_hex_lookup(lookup_type)
            
            if lookup_data is not None and not lookup_data.empty:
                # Determine table name
                table_name = f"econ_busyness_hex_{lookup_type}_lookup"
                
                # Update database table
                data_writer.truncate_and_load_to_postgres(
                    lookup_data,
                    table_name=table_name,
                    schema='gisapdata'
                )
                
                print(f"✓ {lookup_type} hex lookup updated: {len(lookup_data)} records")
            else:
                print(f"⚠ No data for {lookup_type} hex lookup")
                
        except Exception as e:
            print(f"✗ {lookup_type} hex lookup update failed: {e}")

# Example usage
if __name__ == "__main__":
    # Create lookup tables
    create_lookup_tables()
    
    # Update hex lookups
    update_hex_lookups()
```

## Sub-licensing Examples

### 1. Basic Sub-licensing Operations

```python
"""
Sub-licensing system examples
"""
from highstreets.core.sublicense_manager import SublicenseManager

def process_single_partner():
    """Process data for a single partner"""
    
    manager = SublicenseManager()
    
    # Process specific partner
    partner_name = "colliers-hsds"
    
    print(f"Processing data for {partner_name}...")
    
    results = manager.process_sublicense_complete(
        sublicense_name=partner_name,
        save_files=True,
        upload_to_datastore=True
    )
    
    print(f"Processing Results for {partner_name}:")
    print(f"  Success: {results['success']}")
    print(f"  Files created: {results.get('files_saved', 0)}")
    print(f"  Datastore uploads: {results.get('datastore_uploads', 0)}")
    print(f"  Processing time: {results.get('processing_time', 0):.2f}s")
    
    if results.get('errors'):
        print("  Errors:")
        for error in results['errors']:
            print(f"    - {error}")
    
    return results

def process_all_partners():
    """Process data for all active partners"""
    
    manager = SublicenseManager()
    
    print("Processing all active sublicenses...")
    
    results = manager.process_all_sublicenses(
        include_inactive=False,
        save_files=True,
        upload_to_datastore=True
    )
    
    summary = results['summary']
    
    print("Overall Processing Summary:")
    print(f"  Total sublicenses: {summary['total_sublicenses']}")
    print(f"  Successful: {summary['successful_sublicenses']}")
    print(f"  Total files: {summary['total_files']}")
    print(f"  Total uploads: {summary['total_uploads']}")
    print(f"  Total processing time: {results['total_processing_time']:.2f}s")
    
    # Detailed results for each partner
    print("\nDetailed Results:")
    for partner, result in results['sublicense_results'].items():
        status = "✓" if result.get('success') else "✗"
        files = result.get('total_files', 0)
        uploads = result.get('total_uploads', 0)
        print(f"  {status} {partner}: {files} files, {uploads} uploads")
    
    return results

def get_partner_information():
    """Get information about available partners"""
    
    manager = SublicenseManager()
    
    # Get overall information
    info = manager.get_sublicense_info()
    
    print("Sublicense System Information:")
    print(f"  Total sublicenses: {info['total_sublicenses']}")
    print(f"  Active sublicenses: {info['active_sublicenses']}")
    print(f"  Available partners: {', '.join(info['sublicenses'])}")
    
    # Get detailed information for each partner
    print("\nDetailed Partner Information:")
    for partner_name in info['sublicenses']:
        partner_info = manager.get_sublicense_info(partner_name)
        
        if partner_info:
            print(f"\n{partner_name}:")
            print(f"  Status: {partner_info.get('status', 'unknown')}")
            print(f"  Description: {partner_info.get('description', 'N/A')}")
            print(f"  Data sources: {', '.join(partner_info.get('data_sources', []))}")
            print(f"  Contact: {partner_info.get('contact', 'N/A')}")
    
    return info

# Example usage
if __name__ == "__main__":
    # Get partner information
    partner_info = get_partner_information()
    
    # Process single partner
    single_result = process_single_partner()
    
    # Process all partners (uncomment if needed)
    # all_results = process_all_partners()
```

### 2. Custom Sub-licensing Queries

```python
"""
Custom sub-licensing query examples
"""
def execute_custom_queries():
    """Execute specific queries for partners"""
    
    manager = SublicenseManager()
    
    # Example: Get specific data for Fitzrovia Partnership
    fitzrovia_bid_data = manager.execute_sublicense_query(
        sublicense_name="rendle-intelligence-for-fitzrovia-partnership",
        query_type="bt_footfall_bid"
    )
    
    if fitzrovia_bid_data is not None:
        print(f"Fitzrovia BID data: {len(fitzrovia_bid_data)} records")
        print(f"Date range: {fitzrovia_bid_data['count_date'].min()} to {fitzrovia_bid_data['count_date'].max()}")
        print(f"BIDs: {fitzrovia_bid_data['bid_name'].unique()}")
    
    # Example: Get hex data for Avison Young
    avison_hex_data = manager.execute_sublicense_query(
        sublicense_name="avison-young",
        query_type="bt_footfall_hex_towncentre"
    )
    
    if avison_hex_data is not None:
        print(f"Avison Young hex data: {len(avison_hex_data)} records")
        print(f"Town centres: {avison_hex_data['tc_name'].unique()}")
    
    # Example: Get yearly Mastercard data
    westminster_yearly = manager.execute_sublicense_query(
        sublicense_name="westminster-university",
        query_type="mastercard_3hourly_yearly",
        year=2023
    )
    
    if westminster_yearly is not None:
        print(f"Westminster University 2023 data: {len(westminster_yearly)} records")
    
    return {
        'fitzrovia_bid': fitzrovia_bid_data,
        'avison_hex': avison_hex_data,
        'westminster_yearly': westminster_yearly
    }

def validate_sublicense_config():
    """Validate sublicense configuration"""
    
    manager = SublicenseManager()
    
    print("Validating sublicense configuration...")
    
    validation_results = manager.validate_configuration()
    
    if validation_results['valid']:
        print("✓ Configuration is valid")
    else:
        print("✗ Configuration has errors:")
        for error in validation_results['errors']:
            print(f"  - {error}")
    
    if validation_results.get('warnings'):
        print("⚠ Configuration warnings:")
        for warning in validation_results['warnings']:
            print(f"  - {warning}")
    
    return validation_results

# Example usage
if __name__ == "__main__":
    # Validate configuration first
    validation = validate_sublicense_config()
    
    if validation['valid']:
        # Execute custom queries
        query_results = execute_custom_queries()
```

## Advanced Workflows

### 1. End-to-End Pipeline

```python
"""
Complete end-to-end data processing pipeline
"""
import os
from datetime import datetime, timedelta

def run_complete_pipeline(start_date, end_date):
    """Run complete data processing pipeline"""
    
    print(f"Starting complete pipeline for {start_date} to {end_date}")
    
    # Initialize components
    loader = DataLoader()
    transformer = HexTransform()
    writer = DataWriter()
    
    pipeline_results = {
        'start_time': datetime.now(),
        'steps_completed': [],
        'errors': [],
        'data_processed': {}
    }
    
    try:
        # Step 1: Fetch raw data
        print("Step 1: Fetching raw hex data...")
        raw_data = loader.get_hex_data(start_date, end_date)
        pipeline_results['data_processed']['raw_records'] = len(raw_data)
        pipeline_results['steps_completed'].append('data_fetch')
        print(f"✓ Fetched {len(raw_data)} raw records")
        
        # Step 2: Transform data
        print("Step 2: Transforming data...")
        transformed_data = transformer.transform_data(raw_data)
        pipeline_results['data_processed']['transformed_records'] = len(transformed_data)
        pipeline_results['steps_completed'].append('data_transform')
        print(f"✓ Transformed {len(transformed_data)} records")
        
        # Step 3: Save to database
        print("Step 3: Saving to database...")
        writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")
        pipeline_results['steps_completed'].append('data_save')
        print("✓ Data saved to database")
        
        # Step 4: Geographic aggregations
        print("Step 4: Creating geographic aggregations...")
        
        geo_transforms = [
            ('hex_hs_transform_query.sql', 'econ_busyness_bt_highstreets_3hourly_counts'),
            ('hex_tc_transform_query.sql', 'econ_busyness_bt_towncentres_3hourly_counts'),
            ('hex_bid_transform_query.sql', 'econ_busyness_bt_bids_3hourly_counts'),
            ('hex_bespoke_transform_query.sql', 'econ_busyness_bt_bespokes_3hourly_counts')
        ]
        
        for query_file, table_name in geo_transforms:
            try:
                transformer.fetch_and_transform_hex_data(
                    transform_layer=query_file,
                    table_name=table_name,
                    load_to_db=True,
                    truncate=False  # Append to existing data
                )
                print(f"✓ {table_name.split('_')[-3]} aggregation completed")
            except Exception as e:
                error_msg = f"Geographic aggregation failed for {table_name}: {e}"
                pipeline_results['errors'].append(error_msg)
                print(f"✗ {error_msg}")
        
        pipeline_results['steps_completed'].append('geographic_aggregation')
        
        # Step 5: Sub-licensing processing
        print("Step 5: Processing sub-licensing...")
        try:
            from highstreets.core.sublicense_manager import SublicenseManager
            manager = SublicenseManager()
            sublicense_results = manager.process_all_sublicenses()
            
            pipeline_results['data_processed']['sublicense_files'] = sublicense_results['summary']['total_files']
            pipeline_results['steps_completed'].append('sublicensing')
            print(f"✓ Sub-licensing completed: {sublicense_results['summary']['total_files']} files created")
            
        except Exception as e:
            error_msg = f"Sub-licensing failed: {e}"
            pipeline_results['errors'].append(error_msg)
            print(f"✗ {error_msg}")
        
        # Step 6: Data quality checks
        print("Step 6: Running data quality checks...")
        try:
            quality_results = run_data_quality_checks(start_date, end_date)
            pipeline_results['data_processed']['quality_checks'] = quality_results
            pipeline_results['steps_completed'].append('quality_checks')
            print("✓ Data quality checks completed")
            
        except Exception as e:
            error_msg = f"Quality checks failed: {e}"
            pipeline_results['errors'].append(error_msg)
            print(f"✗ {error_msg}")
        
    except Exception as e:
        error_msg = f"Pipeline failed: {e}"
        pipeline_results['errors'].append(error_msg)
        print(f"✗ {error_msg}")
    
    # Final summary
    pipeline_results['end_time'] = datetime.now()
    pipeline_results['total_time'] = (pipeline_results['end_time'] - pipeline_results['start_time']).total_seconds()
    
    print(f"\nPipeline Summary:")
    print(f"  Total time: {pipeline_results['total_time']:.2f} seconds")
    print(f"  Steps completed: {len(pipeline_results['steps_completed'])}")
    print(f"  Errors: {len(pipeline_results['errors'])}")
    
    return pipeline_results

def run_data_quality_checks(start_date, end_date):
    """Run comprehensive data quality checks"""
    
    loader = DataLoader()
    quality_results = {}
    
    # Check data freshness
    hex_data = loader.get_full_data('bt_footfall_tfl_hex_3hourly')
    if hex_data is not None and not hex_data.empty:
        latest_date = hex_data['count_date'].max()
        days_old = (datetime.now().date() - latest_date).days
        quality_results['data_freshness'] = {
            'latest_date': str(latest_date),
            'days_old': days_old,
            'status': 'fresh' if days_old <= 3 else 'stale'
        }
    
    # Check data completeness
    expected_records_per_day = 8 * 3  # 8 time periods * 3 metrics (rough estimate)
    date_range = pd.date_range(start_date, end_date)
    
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        daily_data = hex_data[hex_data['count_date'] == date_str] if hex_data is not None else pd.DataFrame()
        
        if len(daily_data) < expected_records_per_day:
            quality_results[f'incomplete_date_{date_str}'] = len(daily_data)
    
    return quality_results

# Example usage
if __name__ == "__main__":
    # Run pipeline for last week
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    pipeline_results = run_complete_pipeline(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
```

For more examples and detailed use cases, refer to the [User Guide](user_guide.md) and [API Reference](api_reference.md). 