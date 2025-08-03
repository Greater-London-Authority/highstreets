# Highstreets Sublicensing System

## Overview

The Highstreets sublicensing system provides an industrial-standard approach to managing data sharing agreements with external partners. The system uses database-side filtering to efficiently extract and process data for multiple sublicense agreements without loading full datasets into memory.

## Key Features

- **Database-Driven Processing**: All filtering happens at the SQL level for optimal performance
- **YAML Configuration**: Centralized, human-readable configuration for all sublicenses
- **Flexible Data Sources**: Support for BT footfall, Mastercard, and other data types
- **Automatic File Management**: Saves CSV files and uploads to London Datastore automatically
- **Performance Monitoring**: Tracks processing statistics and execution times
- **Configuration Validation**: Built-in validation for sublicense configurations

## Architecture

### Components

1. **SublicenseManager** (`highstreets.core.sublicense_manager`): Main processing engine
2. **Configuration** (`highstreets/core/settings/sublicenses.yaml`): YAML-based sublicense definitions
3. **SQL Templates**: Database queries with parameter substitution
4. **Output Management**: File saving and datastore uploading

### Data Flow

```
YAML Config → SQL Query Templates → Database Execution → CSV Export → Datastore Upload
```

## Sublicense Configuration

### Configuration Structure

```yaml
# Global configuration
config:
  base_schema: "gisapdata"
  date_formats:
    mastercard_3hourly: "count_date"
    mastercard_weekly: "week_start"
    bt_footfall: "count_date"
  
  data_sources:
    bt_footfall:
      aggregated_tables:
        bespoke: "econ_busyness_bt_bespoke_3hourly_counts"
        bid: "econ_busyness_bt_bids_3hourly_counts"
      raw_table: "bt_footfall_tfl_hex_3hourly"
      lookup_tables:
        hex_bid: "econ_busyness_hex_bid_lookup"

# Individual sublicenses
sublicenses:
  sublicense-name:
    slug: "datastore-slug"
    description: "Human readable description"
    contact: "contact@example.com"
    status: "active"  # or "inactive"
    
    data_sources:
      - bt_footfall
      - mastercard_3hourly
    
    filters:
      bid_ids: [21, 77]
      bespoke_area_ids: [112, 113, 114]
    
    query_templates:
      bt_footfall_bid: |
        SELECT * FROM {schema}.econ_busyness_bt_bids_3hourly_counts 
        WHERE bid_id IN ({bid_ids})
    
    output_configs:
      bt_footfall:
        resource_title: "partner_bt_data.csv"
        file_path: "bt/processed/partner/"
        custom_date_column: "count_date"
```

### Current Sublicenses

#### Active Sublicenses

1. **Colliers HSDS** (`colliers-hsds`)
   - **Description**: Colliers HSDS sublicense agreement for HOLBA sites
   - **Data Sources**: BT footfall, Mastercard 3-hourly, Mastercard weekly
   - **Coverage**: 8 bespoke areas (HOLBA sites)
   - **Contact**: Colliers team

2. **Fitzrovia Partnership** (`rendle-intelligence-for-fitzrovia-partnership`)
   - **Description**: Fitzrovia Partnership BID sublicense
   - **Data Sources**: BT footfall, BT hex, Mastercard 3-hourly, Mastercard weekly
   - **Coverage**: 2 BIDs in Fitzrovia area
   - **Contact**: Fitzrovia Partnership

3. **Knightsbridge Partnership** (`rendle-intelligence-for-knightsbridge-partnership`)
   - **Description**: Knightsbridge Partnership BID sublicense
   - **Data Sources**: BT footfall, BT hex, Mastercard 3-hourly, Mastercard weekly
   - **Coverage**: 2 BIDs in Knightsbridge area
   - **Contact**: Knightsbridge Partnership

4. **Jon Puleston Station BID** (`jon-puleston-for-station-to-station-bid`)
   - **Description**: Jon Puleston Station to Station BID sublicense
   - **Data Sources**: BT footfall, BT hex, Mastercard 3-hourly
   - **Coverage**: 1 BID
   - **Contact**: Jon Puleston

5. **Avison Young** (`avison-young`)
   - **Description**: Avison Young sublicense for town centres and bespoke areas
   - **Data Sources**: BT footfall, BT hex, Mastercard 3-hourly
   - **Coverage**: 7 town centres + 1 bespoke area
   - **Contact**: Avison Young

6. **Westminster University** (`westminster-university`)
   - **Description**: Westminster University academic research sublicense
   - **Data Sources**: Mastercard 3-hourly (yearly files)
   - **Coverage**: Full London data by year (2022-2025)
   - **Contact**: Westminster University Research

7. **Southbank Centre** (`southbank-centre`)
   - **Description**: Southbank Centre BID and cultural venue sublicense
   - **Data Sources**: BT footfall, BT hex, Mastercard 3-hourly, Mastercard weekly
   - **Coverage**: 4 BIDs + 1 HOLBA site in Southbank area
   - **Contact**: Southbank Centre

#### Inactive Sublicenses

- **Andrew Scott LTN Project**: Low Traffic Neighbourhood research (commented out)

## Database Tables

### Data Tables

#### BT Footfall
- `bt_footfall_tfl_hex_3hourly`: Raw hex-level footfall data
- `econ_busyness_bt_bespoke_3hourly_counts`: Aggregated bespoke areas
- `econ_busyness_bt_bids_3hourly_counts`: Aggregated BIDs
- `econ_busyness_bt_towncentres_3hourly_counts`: Aggregated town centres
- `econ_busyness_bt_highstreets_3hourly_counts`: Aggregated high streets

#### Mastercard Data
- `econ_busyness_mrli_3hourly_adj`: Raw 3-hourly transaction data (adjusted)
- `econ_busyness_mcard_*_3hourly_txn`: Aggregated 3-hourly tables by geography
- `econ_busyness_mcard_*_txn`: Weekly transaction tables
- `econ_busyness_mcard_*_yoy`: Year-over-year comparison tables

#### Lookup Tables
- `econ_busyness_hex_bid_lookup`: Hex to BID mapping
- `econ_busyness_hex_towncentre_lookup`: Hex to town centre mapping
- `econ_busyness_hex_bespoke_lookup`: Hex to bespoke area mapping
- `econ_busyness_mcard_BIDs_quad_lookup`: Quad to BID mapping
- `econ_busyness_mcard_TownCentres_quad_lookup`: Quad to town centre mapping
- `econ_busyness_mcard_bespoke_quad_lookup`: Quad to bespoke area mapping

## Usage Examples

### Basic Usage

```python
from highstreets.core.sublicense_manager import SublicenseManager

# Initialize the manager
manager = SublicenseManager()

# Process a single sublicense
results = manager.process_sublicense_complete('colliers-hsds')
print(f"Processing successful: {results['success']}")
print(f"Files created: {results['files_saved']}")
print(f"Datastore uploads: {results['datastore_uploads']}")
```

### Execute Specific Queries

```python
# Execute a specific query type
df = manager.execute_sublicense_query('fitzrovia-partnership', 'bt_footfall_bid')

if df is not None:
    print(f"Retrieved {len(df)} rows")
    print("Date range:", df['count_date'].min(), "to", df['count_date'].max())
```

### Process All Sublicenses

```python
# Process all active sublicenses
results = manager.process_all_sublicenses()

summary = results['summary']
print(f"Processed: {summary['successful_sublicenses']}/{summary['total_sublicenses']}")
print(f"Total queries: {summary['total_queries']}")
print(f"Files created: {summary['total_files']}")
print(f"Processing time: {results['total_processing_time']:.2f}s")
```

### Configuration Management

```python
# Get sublicense information
info = manager.get_sublicense_info('avison-young')
print(f"Status: {info['status']}")
print(f"Data sources: {info['data_sources']}")
print(f"Contact: {info['contact']}")

# Validate configuration
validation = manager.validate_configuration()
if not validation['valid']:
    print("Configuration errors:", validation['errors'])
```

### Performance Monitoring

```python
# Get performance statistics
stats = manager.get_performance_stats()
print(f"Queries executed: {stats['performance_stats']['queries_executed']}")
print(f"Rows processed: {stats['performance_stats']['total_rows_processed']}")
```

## API Reference

### SublicenseManager

#### Core Methods

- `process_sublicense_complete(sublicense_name, save_files=True, upload_to_datastore=True)`
  - Complete processing of a sublicense
  - Returns: Processing results dictionary

- `process_all_sublicenses(include_inactive=False, save_files=True, upload_to_datastore=True)`
  - Process all sublicenses
  - Returns: Overall processing results

- `execute_sublicense_query(sublicense_name, query_type, year=None)`
  - Execute specific query for a sublicense
  - Returns: DataFrame with results or None

#### Data Management

- `save_sublicense_data(df, sublicense_name, output_key, apply_formatting=True)`
  - Save DataFrame to CSV file
  - Returns: File path or None

- `upload_to_datastore(df, sublicense_name, output_key, file_path=None)`
  - Upload data to London Datastore
  - Returns: Success boolean

#### Information & Validation

- `get_sublicense_info(sublicense_name=None)`
  - Get information about sublicense(s)
  - Returns: Information dictionary

- `validate_configuration()`
  - Validate YAML configuration
  - Returns: Validation results

- `get_performance_stats()`
  - Get performance statistics
  - Returns: Statistics dictionary

## Command Line Interface

The system can be used from the command line:

```bash
# Process specific sublicense
python -m highstreets.core.sublicense_manager --sublicense colliers-hsds

# Process all sublicenses
python -m highstreets.core.sublicense_manager --all

# Validate configuration
python -m highstreets.core.sublicense_manager --validate

# Get sublicense information
python -m highstreets.core.sublicense_manager --info colliers-hsds
```

## File Organization

### Output Directory Structure

```
{base_dir}/
├── bt/processed/
│   ├── bespoke/
│   │   └── Colliers agreement - Holba sites/
│   ├── bid/
│   │   ├── fitzrovia/
│   │   ├── knightsbridge/
│   │   └── jon_puleson/
│   └── hex_grid/
│       ├── fitzrovia/
│       ├── knightsbridge/
│       └── avison_young/
└── mastercard/
    ├── mrli_3hourly/processed/
    │   ├── bespoke/
    │   ├── bid/
    │   └── MRLI_3yr_compressed/
    └── weekly/processed/
        ├── bespoke/
        └── bid/
```

### File Naming Conventions

- **BT Footfall**: `{partner}_bt_{type}_3hourly_counts.csv`
- **BT Hex**: `{partner}_bt_hex_3hourly_counts.csv`
- **Mastercard 3-hourly**: `{partner}_mcard_{type}_3hourly_txn.csv`
- **Mastercard Weekly**: `{partner}_mcard_weekly_{metric}.csv`

## Performance Considerations

### Database-Side Filtering Benefits

1. **Memory Efficiency**: Only relevant data loaded into Python
2. **Network Efficiency**: Reduced data transfer from database
3. **Processing Speed**: Database indexes optimize filtering
4. **Scalability**: Handles large datasets efficiently

### Optimization Tips

1. **Batch Processing**: Process multiple sublicenses together
2. **Selective Processing**: Use specific query types when needed
3. **Monitoring**: Track performance statistics
4. **Configuration**: Validate configuration before processing

## Error Handling

### Common Issues

1. **Configuration Errors**: Missing required fields, invalid YAML
2. **Database Connection**: Network issues, permission problems
3. **Data Issues**: Empty results, missing tables
4. **File System**: Permission issues, disk space

### Troubleshooting

1. **Validate Configuration**: Run validation before processing
2. **Check Logs**: Review detailed logging output
3. **Test Queries**: Execute individual queries first
4. **Monitor Performance**: Check statistics for bottlenecks

## Adding New Sublicenses

### Step 1: Add to YAML Configuration

```yaml
new-partner:
  slug: "new-partner-slug"
  description: "Description of the partnership"
  contact: "contact@partner.com"
  status: "active"
  
  data_sources:
    - bt_footfall
    - mastercard_3hourly
  
  filters:
    bid_ids: [123, 456]
  
  query_templates:
    bt_footfall_bid: |
      SELECT * FROM {schema}.econ_busyness_bt_bids_3hourly_counts 
      WHERE bid_id IN ({bid_ids})
  
  output_configs:
    bt_footfall:
      resource_title: "new_partner_bt_data.csv"
      file_path: "bt/processed/new_partner/"
      custom_date_column: "count_date"
```

### Step 2: Test Configuration

```python
manager = SublicenseManager()
validation = manager.validate_configuration()
assert validation['valid'], f"Errors: {validation['errors']}"
```

### Step 3: Test Processing

```python
# Test with specific query first
df = manager.execute_sublicense_query('new-partner', 'bt_footfall_bid')
assert df is not None, "No data returned"

# Full processing test
results = manager.process_sublicense_complete('new-partner')
assert results['success'], f"Errors: {results['errors']}"
```

## Integration with Existing Pipelines

### AWS Pipeline Integration

The sublicensing system can be integrated into existing AWS pipelines:

```python
# In mcard_3hourly.py
from highstreets.core.sublicense_manager import SublicenseManager

# After main processing
manager = SublicenseManager()
manager.process_all_sublicenses()
```

### Scheduled Processing

For automated processing, use the system in scheduled jobs:

```python
import schedule
import time

def process_sublicenses():
    manager = SublicenseManager()
    results = manager.process_all_sublicenses()
    # Log results or send notifications

schedule.every().day.at("02:00").do(process_sublicenses)

while True:
    schedule.run_pending()
    time.sleep(60)
```

## Best Practices

### Configuration Management

1. **Version Control**: Keep YAML configuration in version control
2. **Environment Separation**: Use different configs for dev/prod
3. **Documentation**: Document all changes to sublicense agreements
4. **Validation**: Always validate before deploying changes

### Data Processing

1. **Incremental Updates**: Process only new data when possible
2. **Error Recovery**: Implement retry logic for failed processes
3. **Monitoring**: Set up alerts for processing failures
4. **Backup**: Maintain backups of sublicense data

### Security

1. **Access Control**: Limit who can modify sublicense configurations
2. **Data Governance**: Ensure compliance with data sharing agreements
3. **Audit Trail**: Log all sublicense processing activities
4. **Encryption**: Use encrypted connections for data transfer

## Future Enhancements

### Planned Features

1. **Real-time Processing**: Stream-based sublicense processing
2. **Advanced Filtering**: More complex filtering capabilities
3. **Data Quality Checks**: Automated validation of output data
4. **Notification System**: Alerts for processing completion/failures
5. **Web Interface**: Dashboard for sublicense management

### Extensibility

The system is designed to be easily extensible:

1. **New Data Sources**: Add support for additional data types
2. **Custom Processors**: Implement specialized processing logic
3. **Output Formats**: Support for formats beyond CSV
4. **Integration APIs**: REST APIs for external system integration

---

*This documentation is maintained by the Data Engineering team. For questions or suggestions, please contact the team or raise an issue in the repository.* 