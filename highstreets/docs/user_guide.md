# User Guide

This guide provides practical examples and workflows for using the Highstreets package to process, analyze, and manage footfall and transaction data for London's high streets and commercial areas.

## Table of Contents

- [Getting Started](#getting-started)
- [Common Workflows](#common-workflows)
- [Data Processing](#data-processing)
- [Geographic Analysis](#geographic-analysis)
- [Sub-licensing Operations](#sub-licensing-operations)
- [Data Quality and Monitoring](#data-quality-and-monitoring)
- [Advanced Features](#advanced-features)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Getting Started

### Quick Setup

```python
# Import core modules
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.hextransform import HexTransform
from highstreets.api.clientbase import APIClient

# Initialize components
loader = DataLoader()
writer = DataWriter()
transformer = HexTransform()
api_client = APIClient()

print("Highstreets package initialized successfully!")
```

### Verify Installation

```python
# Test database connection
try:
    loader = DataLoader()
    print("✓ Database connection successful")
except Exception as e:
    print(f"✗ Database connection failed: {e}")

# Test API connection
try:
    client = APIClient()
    token = client.get_access_token()
    print("✓ API authentication successful")
except Exception as e:
    print(f"✗ API authentication failed: {e}")
```

## Common Workflows

### 1. Processing BT Footfall Data

#### Basic Data Retrieval and Processing

```python
import pandas as pd
from datetime import datetime, timedelta

# Set date range
start_date = "2023-01-01"
end_date = "2023-01-31"

# Fetch raw data from BT API
print(f"Fetching BT data from {start_date} to {end_date}...")
raw_data = loader.get_hex_data(start_date, end_date)
print(f"Retrieved {len(raw_data)} records")

# Transform data
print("Transforming data...")
transformed_data = transformer.transform_data(raw_data)
print(f"Transformed to {len(transformed_data)} records")

# Save to database
print("Saving to database...")
writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")
print("Data saved successfully!")
```

#### Processing with Geographic Aggregation

```python
# Process hex data into geographic boundaries
transformer = HexTransform()

# Transform data for different geographic levels
print("Creating geographic aggregations...")

# High streets
transformer.fetch_and_transform_hex_data(
    transform_layer='hex_hs_transform_query.sql',
    table_name='econ_busyness_bt_highstreets_3hourly_counts',
    load_to_db=True,
    truncate=True
)

# Town centres
transformer.fetch_and_transform_hex_data(
    transform_layer='hex_tc_transform_query.sql',
    table_name='econ_busyness_bt_towncentres_3hourly_counts',
    load_to_db=True,
    truncate=True
)

# Business Improvement Districts (BIDs)
transformer.fetch_and_transform_hex_data(
    transform_layer='hex_bid_transform_query.sql',
    table_name='econ_busyness_bt_bids_3hourly_counts',
    load_to_db=True,
    truncate=True
)

print("Geographic aggregations completed!")
```

### 2. Mastercard Transaction Processing

```python
from highstreets.data_transformation.mcard_transform import McardTransform

# Initialize Mastercard transformer
mcard_transformer = McardTransform()

# Process 3-hourly transaction data
print("Processing Mastercard 3-hourly data...")
mcard_transformer.fetch_and_transform_mcard_data(
    transform_layer='quad_hs_transform_query.sql',
    table_name='econ_busyness_mcard_highstreets_3hourly_txn',
    load_to_db=True,
    truncate=True
)

# Apply inflation adjustments
print("Applying inflation adjustments...")
mcard_transformer.adjust_mcard_data_sql()

print("Mastercard processing completed!")
```

### 3. Daily Data Processing Pipeline

```python
from highstreets.data_transformation.dailytransform import DailyTransform

# Process daily aggregated data
daily_transformer = DailyTransform()

# Get daily aggregated data from API
daily_data = loader.get_bt_daily_aggregate_customer_shapes(start_date, end_date)

# Transform daily data
transformed_daily = daily_transformer.raw_bt_daily_preprocess_data(daily_data)

# Save to database
writer.append_data_to_postgres(
    transformed_daily, 
    table_name="econ_busyness_bt_daily_agg_cust_raw"
)

print("Daily data processing completed!")
```

## Data Processing

### Working with Different Data Sources

#### BT Footfall Data Types

```python
# Different BT data endpoints
hex_data = loader.get_hex_data("2023-01-01", "2023-01-31")           # Hex grid data
msoa_data = loader.get_msoa_data("2023-01-01", "2023-01-31")         # MSOA level data
lsoa_data = loader.get_lsoa_data("2023-01-01", "2023-01-31")         # LSOA level data
daily_data = loader.get_bt_daily_aggregate_customer_shapes("2023-01-01", "2023-01-31")

print(f"Hex data: {len(hex_data)} records")
print(f"MSOA data: {len(msoa_data)} records")
print(f"LSOA data: {len(lsoa_data)} records")
print(f"Daily data: {len(daily_data)} records")
```

#### Data Transformation Examples

```python
# Transform different data types
from highstreets.data_transformation.lsoatransform import LsoaTransform
from highstreets.data_transformation.msoatransform import MsoaTransform

# LSOA transformation
lsoa_transformer = LsoaTransform()
transformed_lsoa = lsoa_transformer.raw_lsoa_transform_data(lsoa_data)

# MSOA transformation
msoa_transformer = MsoaTransform()
transformed_msoa = msoa_transformer.raw_msoa_transform_data(msoa_data)

print("Data transformation completed for all geographic levels")
```

### Batch Processing

```python
import pandas as pd
from datetime import datetime, timedelta

def process_data_in_batches(start_date, end_date, batch_days=7):
    """Process data in weekly batches to manage memory"""
    
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_dt:
        batch_end = min(current_date + timedelta(days=batch_days), end_dt)
        
        batch_start_str = current_date.strftime("%Y-%m-%d")
        batch_end_str = batch_end.strftime("%Y-%m-%d")
        
        print(f"Processing batch: {batch_start_str} to {batch_end_str}")
        
        # Process batch
        try:
            data = loader.get_hex_data(batch_start_str, batch_end_str)
            if data:
                transformed = transformer.transform_data(data)
                writer.append_data_to_postgres(transformed, "bt_footfall_tfl_hex_3hourly")
                print(f"✓ Batch completed: {len(transformed)} records processed")
            else:
                print("⚠ No data returned for this batch")
        except Exception as e:
            print(f"✗ Batch failed: {e}")
        
        current_date = batch_end + timedelta(days=1)

# Process year 2023 in weekly batches
process_data_in_batches("2023-01-01", "2023-12-31", batch_days=7)
```

## Geographic Analysis

### Working with Lookup Tables

```python
# Load geographic lookup tables
hex_hs_lookup = loader.get_full_data('econ_busyness_hex_highstreet_lookup')
hex_tc_lookup = loader.get_full_data('econ_busyness_hex_towncentre_lookup')
hex_bid_lookup = loader.get_full_data('econ_busyness_hex_bid_lookup')

print(f"High street lookup: {len(hex_hs_lookup)} hex cells")
print(f"Town centre lookup: {len(hex_tc_lookup)} hex cells")
print(f"BID lookup: {len(hex_bid_lookup)} hex cells")

# Analyze coverage
total_hex_cells = len(set(hex_hs_lookup['hex_id']) | 
                     set(hex_tc_lookup['hex_id']) | 
                     set(hex_bid_lookup['hex_id']))
print(f"Total hex cells with geographic assignments: {total_hex_cells}")
```

### Spatial Analysis Example

```python
import geopandas as gpd
from highstreets.data_source_sink.lookup_manager import LookupManager

# Initialize lookup manager
lookup_manager = LookupManager()

# Get geographic boundaries
highstreets_gdf = lookup_manager.get_query_context(['highstreets'])
towncentres_gdf = lookup_manager.get_query_context(['towncentres'])

# Analyze high street characteristics
print("High Street Analysis:")
print(f"Total high streets: {len(highstreets_gdf)}")
print(f"Average area: {highstreets_gdf.geometry.area.mean():.2f} sq meters")

# Find high streets by borough
if 'borough' in highstreets_gdf.columns:
    borough_counts = highstreets_gdf['borough'].value_counts()
    print(f"High streets by borough:")
    print(borough_counts.head())
```

### Creating Custom Geographic Aggregations

```python
def create_custom_aggregation(hex_data, custom_areas):
    """Create custom geographic aggregations"""
    
    # Merge hex data with custom area definitions
    aggregated_data = hex_data.merge(
        custom_areas, 
        on='hex_id', 
        how='inner'
    )
    
    # Group by custom area and time period
    result = aggregated_data.groupby([
        'custom_area_id', 'custom_area_name', 'count_date', 'hours'
    ]).agg({
        'resident': 'sum',
        'visitor': 'sum', 
        'worker': 'sum',
        'loyalty_percentage': 'mean',
        'dwell_time': 'mean'
    }).reset_index()
    
    return result

# Example usage
hex_data = loader.get_full_data('bt_footfall_tfl_hex_3hourly')
custom_areas = pd.DataFrame({
    'hex_id': ['hex_001', 'hex_002', 'hex_003'],
    'custom_area_id': [1, 1, 2],
    'custom_area_name': ['Area A', 'Area A', 'Area B']
})

custom_aggregated = create_custom_aggregation(hex_data, custom_areas)
print(f"Custom aggregation created: {len(custom_aggregated)} records")
```

## Sub-licensing Operations

### Processing Partner Data Requests

```python
from highstreets.core.sublicense_manager import SublicenseManager

# Initialize sublicense manager
manager = SublicenseManager()

# Process specific partner's data
partner_name = "colliers-hsds"
results = manager.process_sublicense_complete(partner_name)

print(f"Processing results for {partner_name}:")
print(f"Success: {results['success']}")
print(f"Files created: {results['files_saved']}")
print(f"Datastore uploads: {results['datastore_uploads']}")

if results['errors']:
    print("Errors encountered:")
    for error in results['errors']:
        print(f"  - {error}")
```

### Bulk Partner Processing

```python
# Process all active sublicenses
all_results = manager.process_all_sublicenses()

summary = all_results['summary']
print("Bulk processing summary:")
print(f"Sublicenses processed: {summary['successful_sublicenses']}/{summary['total_sublicenses']}")
print(f"Total files created: {summary['total_files']}")
print(f"Total datastore uploads: {summary['total_uploads']}")
print(f"Processing time: {all_results['total_processing_time']:.2f} seconds")

# Review individual results
for sublicense_name, result in all_results['sublicense_results'].items():
    status = "✓" if result['success'] else "✗"
    print(f"{status} {sublicense_name}: {result.get('total_files', 0)} files")
```

### Custom Data Export

```python
def export_custom_dataset(area_ids, area_type, start_date, end_date, output_path):
    """Export custom dataset for specific areas"""
    
    # Build query based on area type
    if area_type == 'bid':
        query = f"""
        SELECT * FROM econ_busyness_bt_bids_3hourly_counts 
        WHERE bid_id IN ({','.join(map(str, area_ids))})
        AND count_date BETWEEN '{start_date}' AND '{end_date}'
        """
    elif area_type == 'highstreet':
        query = f"""
        SELECT * FROM econ_busyness_bt_highstreets_3hourly_counts 
        WHERE highstreet_id IN ({','.join(map(str, area_ids))})
        AND count_date BETWEEN '{start_date}' AND '{end_date}'
        """
    
    # Execute query
    data = pd.read_sql(query, loader.engine)
    
    # Export to CSV
    data.to_csv(output_path, index=False)
    print(f"Exported {len(data)} records to {output_path}")
    
    return data

# Export data for specific BIDs
bid_data = export_custom_dataset(
    area_ids=[21, 77],  # Fitzrovia BIDs
    area_type='bid',
    start_date='2023-01-01',
    end_date='2023-12-31',
    output_path='/path/to/fitzrovia_2023.csv'
)
```

## Data Quality and Monitoring

### Data Validation

```python
from highstreets.data.schema import validate_bt_hex_transformed
import pandas as pd

def validate_processed_data(data):
    """Validate processed data against schema"""
    
    try:
        # Schema validation
        validated_data = validate_bt_hex_transformed(data)
        print("✓ Schema validation passed")
        
        # Custom validation checks
        issues = []
        
        # Check for negative values
        numeric_cols = ['resident', 'visitor', 'worker']
        for col in numeric_cols:
            if (data[col] < 0).any():
                issues.append(f"Negative values found in {col}")
        
        # Check date range
        if data['count_date'].dt.date.nunique() == 0:
            issues.append("No valid dates found")
            
        # Check for outliers
        for col in numeric_cols:
            q99 = data[col].quantile(0.99)
            outliers = (data[col] > q99 * 10).sum()
            if outliers > 0:
                issues.append(f"{outliers} extreme outliers in {col}")
        
        if issues:
            print("⚠ Data quality issues found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("✓ Data quality checks passed")
            
        return validated_data, issues
        
    except Exception as e:
        print(f"✗ Validation failed: {e}")
        return None, [str(e)]

# Validate recent data
recent_data = loader.get_hex_data("2023-12-01", "2023-12-07")
transformed_data = transformer.transform_data(recent_data)
validated_data, issues = validate_processed_data(transformed_data)
```

### Monitoring Data Freshness

```python
def check_data_freshness():
    """Check how recent the data is in each table"""
    
    tables_to_check = [
        'bt_footfall_tfl_hex_3hourly',
        'econ_busyness_bt_highstreets_3hourly_counts',
        'econ_busyness_bt_towncentres_3hourly_counts',
        'econ_busyness_bt_bids_3hourly_counts'
    ]
    
    freshness_report = {}
    
    for table in tables_to_check:
        try:
            query = f"SELECT MAX(count_date) as latest_date FROM {table}"
            result = pd.read_sql(query, loader.engine)
            latest_date = result['latest_date'].iloc[0]
            
            if latest_date:
                days_old = (datetime.now().date() - latest_date).days
                freshness_report[table] = {
                    'latest_date': latest_date,
                    'days_old': days_old,
                    'status': 'fresh' if days_old <= 7 else 'stale'
                }
            else:
                freshness_report[table] = {
                    'latest_date': None,
                    'days_old': None,
                    'status': 'empty'
                }
        except Exception as e:
            freshness_report[table] = {
                'error': str(e),
                'status': 'error'
            }
    
    return freshness_report

# Check data freshness
freshness = check_data_freshness()
for table, info in freshness.items():
    if info['status'] == 'fresh':
        print(f"✓ {table}: {info['latest_date']} ({info['days_old']} days old)")
    elif info['status'] == 'stale':
        print(f"⚠ {table}: {info['latest_date']} ({info['days_old']} days old)")
    else:
        print(f"✗ {table}: {info['status']}")
```

## Advanced Features

### Working with SQL Manager

```python
from highstreets.core.sql_manager import SQLManager

# Initialize SQL manager
sql_manager = SQLManager()

# Execute parameterized queries
result = sql_manager.execute_query_to_dataframe(
    "SELECT * FROM bt_footfall_tfl_hex_3hourly WHERE count_date BETWEEN %s AND %s",
    params=('2023-01-01', '2023-01-31')
)

print(f"Query returned {len(result)} records")

# Use chunked processing for large results
for chunk in sql_manager.execute_query_chunked(
    "SELECT * FROM bt_footfall_tfl_hex_3hourly", 
    chunk_size=10000
):
    print(f"Processing chunk with {len(chunk)} records")
    # Process chunk here
```

### Custom Processors

```python
from highstreets.core.processors.sublicense_processor import SublicenseProcessor

# Create custom processor
class CustomDataProcessor:
    def __init__(self):
        self.loader = DataLoader()
        self.writer = DataWriter()
    
    def process_weekend_analysis(self, start_date, end_date):
        """Custom analysis for weekend patterns"""
        
        # Get data for weekends only
        query = """
        SELECT * FROM bt_footfall_tfl_hex_3hourly 
        WHERE count_date BETWEEN %s AND %s
        AND EXTRACT(DOW FROM count_date) IN (0, 6)  -- Sunday and Saturday
        """
        
        weekend_data = pd.read_sql(query, self.loader.engine, params=(start_date, end_date))
        
        # Analyze weekend patterns
        weekend_summary = weekend_data.groupby(['hex_id', 'hours']).agg({
            'visitor': 'mean',
            'resident': 'mean',
            'worker': 'mean'
        }).reset_index()
        
        # Save results
        self.writer.append_data_to_postgres(weekend_summary, "weekend_analysis_results")
        
        return weekend_summary

# Use custom processor
processor = CustomDataProcessor()
weekend_analysis = processor.process_weekend_analysis("2023-01-01", "2023-12-31")
print(f"Weekend analysis completed: {len(weekend_analysis)} records")
```

## Best Practices

### Performance Optimization

```python
# 1. Use chunked processing for large datasets
def process_large_dataset(table_name, chunk_size=10000):
    """Process large datasets in chunks"""
    
    total_rows = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", loader.engine)['count'].iloc[0]
    
    for offset in range(0, total_rows, chunk_size):
        chunk_query = f"""
        SELECT * FROM {table_name} 
        ORDER BY count_date 
        LIMIT {chunk_size} OFFSET {offset}
        """
        
        chunk_data = pd.read_sql(chunk_query, loader.engine)
        
        # Process chunk
        yield chunk_data

# 2. Use appropriate data types
def optimize_dataframe(df):
    """Optimize DataFrame memory usage"""
    
    # Convert object columns to category where appropriate
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
            df[col] = df[col].astype('category')
    
    # Downcast numeric types
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df

# 3. Use database indexes effectively
def create_performance_indexes():
    """Create indexes for better query performance"""
    
    indexes = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_date ON bt_footfall_tfl_hex_3hourly (count_date);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_hex_id ON bt_footfall_tfl_hex_3hourly (hex_id);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_hours ON bt_footfall_tfl_hex_3hourly (hours);",
    ]
    
    for index_sql in indexes:
        try:
            with loader.engine.begin() as conn:
                conn.execute(index_sql)
            print(f"✓ Index created: {index_sql}")
        except Exception as e:
            print(f"⚠ Index creation failed: {e}")
```

### Error Handling

```python
import logging
from typing import Optional

def robust_data_processing(start_date: str, end_date: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """Robust data processing with retry logic"""
    
    for attempt in range(max_retries):
        try:
            # Attempt data processing
            data = loader.get_hex_data(start_date, end_date)
            
            if not data:
                logging.warning(f"No data returned for {start_date} to {end_date}")
                return None
            
            transformed_data = transformer.transform_data(data)
            
            # Validate data
            if len(transformed_data) == 0:
                raise ValueError("Transformation resulted in empty dataset")
            
            # Save to database
            writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")
            
            logging.info(f"Successfully processed {len(transformed_data)} records")
            return transformed_data
            
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            
            if attempt == max_retries - 1:
                logging.error("All retry attempts exhausted")
                raise
            
            # Wait before retry (exponential backoff)
            import time
            wait_time = 2 ** attempt
            logging.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    return None
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Database Connection Issues

```python
def diagnose_database_connection():
    """Diagnose database connection issues"""
    
    import psycopg2
    import os
    
    try:
        # Test basic connection
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST'),
            database=os.getenv('PG_DATABASE'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            port=os.getenv('PG_PORT', 5432)
        )
        conn.close()
        print("✓ Database connection successful")
        
    except psycopg2.OperationalError as e:
        print(f"✗ Database connection failed: {e}")
        print("Check:")
        print("  - Database server is running")
        print("  - Connection parameters are correct")
        print("  - Network connectivity")
        print("  - Firewall settings")
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")

diagnose_database_connection()
```

#### 2. API Authentication Issues

```python
def diagnose_api_connection():
    """Diagnose API authentication issues"""
    
    try:
        client = APIClient()
        token = client.get_access_token()
        
        if token:
            print("✓ API authentication successful")
            print(f"Token preview: {token[:20]}...")
        else:
            print("✗ No token received")
            
    except Exception as e:
        print(f"✗ API authentication failed: {e}")
        print("Check:")
        print("  - CONSUMER_KEY is set correctly")
        print("  - CONSUMER_SECRET is set correctly")  
        print("  - API endpoint is accessible")
        print("  - Internet connectivity")

diagnose_api_connection()
```

#### 3. Memory Issues

```python
def diagnose_memory_usage():
    """Monitor memory usage during processing"""
    
    import psutil
    import gc
    
    # Get current memory usage
    process = psutil.Process()
    memory_info = process.memory_info()
    
    print(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
    
    # Force garbage collection
    gc.collect()
    
    # Memory optimization tips
    print("\nMemory optimization tips:")
    print("  - Process data in smaller chunks")
    print("  - Use appropriate data types")
    print("  - Clear variables when no longer needed")
    print("  - Use streaming processing for large datasets")

diagnose_memory_usage()
```

For more detailed troubleshooting, see the [Troubleshooting Guide](troubleshooting.md) or contact support at anupam.bose@london.gov.uk. 