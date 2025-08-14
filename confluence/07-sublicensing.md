---
title: "Sublicensing & Data Distribution"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["sublicensing", "partners", "data-distribution", "governance"]
---

# Sublicensing & Data Distribution

## 🤝 **Overview**

The Highstreets Data Platform operates a sophisticated sublicensing system that enables controlled data sharing with external partners while maintaining data governance standards. This system uses **database-driven processing with YAML configuration** to provide automated, secure data distribution for strategic partnerships, commercial agreements, and research collaborations.

## 📋 **Sublicensing Framework**

### **Core Principles**
- **Data Governance**: All data sharing follows strict governance and privacy protocols
- **Database-Side Processing**: Efficient SQL-based filtering eliminates memory-intensive operations
- **YAML-Driven Configuration**: No code changes required for new partners or modifications
- **Automated Processing**: Single command processes all active sublicenses
- **Quality Assurance**: Comprehensive validation, monitoring, and error handling

## 🏗️ **Architecture Overview**

### **Core Components**

#### **1. YAML Configuration System**
**File**: `highstreets/core/settings/sublicenses.yaml`
- Centralized sublicense definitions
- Database table mappings  
- Partner-specific filters (BID IDs, bespoke area IDs, etc.)
- Output file configurations
- London Datastore slugs

#### **2. Sublicense Processor**
**File**: `highstreets/core/processors/sublicense_processor.py`
- Database-driven processing using SQL queries
- YAML-driven configuration management
- Automatic file generation and datastore uploads
- Performance monitoring and logging

#### **3. SQL Manager Integration**
**File**: `highstreets/core/sql_manager.py`
- Dynamic query execution with parameter substitution
- Query caching and performance optimization
- Support for both local and S3 file systems

*[📋 Interactive Miro Board -  Sublicense automation Architecture ](https://miro.com/app/board/uXjVKeHa9kQ=/?moveToWidget=3458764637355461613&cot=14)*

**Note:** Visual diagram available above. The interactive board provides complete technical workflow details.


### **Processing Workflow**

#### **Step 1: Configuration Loading**
```python
# Load sublicense definitions from YAML
config = load_config('sublicenses.yaml')
sublicenses = config.get('sublicenses', {})

# Example sublicense configuration
colliers_config = {
    'slug': 'colliers---hsds',
    'filters': {'bespoke_area_ids': [112, 113, 114, 115, 116, 117, 118, 197]},
    'query_templates': {
        'bt_footfall_bespoke': 'SELECT * FROM {schema}.econ_busyness_bt_bespokes_3hourly_counts WHERE bespoke_area_id IN ({bespoke_area_ids})'
    }
}
```

#### **Step 2: Dynamic Query Generation**
```python
# Build SQL query from template with partner filters
query = template.format(
    schema='gisapdata',
    bespoke_area_ids='112, 113, 114, 115, 116, 117, 118, 197'
)

# Execute database-side filtering
df = sql_manager.execute_query(query)
```

#### **Step 3: Automated Export Processing**
```python
# Process all sublicenses automatically
processor = SublicenseProcessor()
results = processor.process_all_sublicenses()

# Individual sublicense processing
results = processor.process_sublicense('colliers-hsds')
```

### **Key Advantages**

#### **Database-Side Filtering**
- Efficient processing: Only relevant data is extracted from PostgreSQL
- No memory-intensive DataFrame operations
- Consistent performance regardless of dataset size

#### **YAML-Driven Configuration**
- No code changes required for new partners
- Centralized partner management
- Version-controlled configuration
- Easy validation and testing

#### **Automatic Processing**
- Single command processes all active sublicenses
- Consistent file formats and naming
- Automated datastore uploads
- Performance monitoring and error handling

## 🎯 **Active Sublicenses**

### **Commercial Partners**

#### **Colliers HSDS Agreement**
- **Partner**: Colliers International - High Street Data Service
- **Slug**: `colliers---hsds`
- **Status**: Active
- **Geographic Scope**: HOLBA sites (8 bespoke areas: 112-118, 197)
- **Data Sources**: BT footfall, Mastercard 3-hourly, Mastercard weekly, International data, BT daily

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Footfall** | `colliers_hsds_footfall_3hourly_counts.csv` | Weekly | Bespoke areas 112-118, 197 |
| **Mastercard 3-Hourly** | `colliers_hsds_mcard_3hourly_txn.csv` | Monthly | Same geographic coverage |
| **Mastercard Weekly** | `colliers_hsds_mcard_weekly_txn.csv` | Monthly | Same geographic coverage |
| **BT Daily** | `colliers_bt_daily_agg_counts.csv` | Weekly | Heart of London BID |
| **Mastercard International** | `colliers_mcard_weekly_intl_txn.csv` | Monthly | HOLBA sites |

#### **Fitzrovia Partnership BID**
- **Partner**: Fitzrovia Partnership Business Improvement District
- **Slug**: `rendle-intelligence-for-fitzrovia-partnership`
- **Status**: Active
- **Geographic Scope**: BID areas 21, 77
- **Data Sources**: BT footfall, BT hex grid, Mastercard 3-hourly, Mastercard weekly

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Aggregated** | `Fitzrovia_bt_3hourly_counts.csv` | Weekly | BID boundary aggregation |
| **BT Hex Grid** | `Fitzrovia_bt_hex_3hourly_counts.csv` | Weekly | Raw hex cell data |
| **Mastercard Quad** | `Fitzrovia_mcard_quad_3hourly_txn.csv` | Monthly | Raw quad-level data |
| **Mastercard Weekly** | `Fitzrovia_hsds_mcard_weekly_txn.csv` | Monthly | BID boundary aggregation |

#### **Knightsbridge Partnership BID**
- **Partner**: Knightsbridge Partnership Business Improvement District
- **Slug**: `rendle-intelligence-for-knightsbridge-partnership`
- **Status**: Active
- **Geographic Scope**: BID areas 64, 69
- **Data Sources**: BT footfall, BT hex grid, Mastercard 3-hourly, Mastercard weekly

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Aggregated** | `Knightsbridge_bt_3hourly_counts.csv` | Weekly | BID boundary aggregation |
| **BT Hex Grid** | `Knightsbridge_bt_hex_3hourly_counts.csv` | Weekly | Raw hex cell data |
| **Mastercard Quad** | `Knightsbridge_mcard_quad_3hourly_txn.csv` | Monthly | Raw quad-level data |
| **Mastercard Weekly** | `Knightsbridge_hsds_mcard_weekly_txn.csv` | Monthly | BID boundary aggregation |

#### **Station to Station BID**
- **Partner**: Jon Puleston for Station to Station BID
- **Slug**: `jon-puleston-for-station-to-station-bid`
- **Status**: Active
- **Geographic Scope**: BID area 46
- **Data Sources**: BT hex grid, Mastercard 3-hourly, BT daily

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Hex Grid** | `jon_puleson_bt_hex_3hourly_counts.csv` | Weekly | Raw hex cell data |
| **Mastercard Quad** | `jon_puleson_mcard_quad_3hourly_txn.csv` | Monthly | Raw quad-level data |
| **BT Daily** | `jon_puleston_bt_daily_agg_counts.csv` | Daily | Station to Station area |

#### **Avison Young**
- **Partner**: Avison Young Commercial Real Estate
- **Slug**: `avison-young`
- **Status**: Active
- **Geographic Scope**: Town centres (23, 33, 29, 37, 28, 46, 31) + Bespoke area 249
- **Data Sources**: BT footfall, BT hex grid, Mastercard 3-hourly

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Town Centres** | `avison_young_bt_3hourly_counts.csv` | Weekly | Town centre aggregation |
| **BT Bespoke** | `avison_young_bt_3hourly_counts.csv` | Weekly | Bespoke area aggregation |
| **Mastercard Town Centres** | `avison_young_tc_mcard_quad_3hourly_txn.csv` | Monthly | Town centre quads |
| **Mastercard Bespoke** | `avison_young_bespoke_mcard_quad_3hourly_txn.csv` | Monthly | Bespoke area quads |

#### **Southbank Centre**
- **Partner**: Southbank Centre Cultural Venue
- **Slug**: `southbank-centre`
- **Status**: Active
- **Geographic Scope**: BID areas (35, 23, 16, 24) + HOLBA site 197
- **Data Sources**: BT footfall, BT hex grid, Mastercard 3-hourly, Mastercard weekly

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT BIDs** | `southbank_bids_bt_footfall_3hourly_counts.csv` | Weekly | BID areas |
| **BT HOLBA** | `southbank_holba_bt_footfall_3hourly_counts.csv` | Weekly | Bespoke area 197 |
| **BT Hex Grid** | `Southbank_bt_hex_3hourly_counts.csv` | Weekly | Raw hex cell data |
| **Mastercard BIDs** | `southbank_bids_mcard_3hourly_txn.csv` | Monthly | BID areas |
| **Mastercard HOLBA 3H** | `southbank_holba_mcard_3hourly_txn.csv` | Monthly | Bespoke area 197 |
| **Mastercard HOLBA Weekly** | `southbank_holba_mcard_weekly_txn.csv` | Monthly | Bespoke area 197 |

### **Academic Research Partners**

#### **Westminster University**
- **Partner**: University of Westminster Research Department
- **Slug**: `westminster-university`
- **Status**: Active
- **Geographic Scope**: London-wide (no geographic filters)
- **Data Sources**: Mastercard 3-hourly (historical years), BT hex grid

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **2022 Mastercard** | `Mastercard_3hourly_2022.csv` | Annual | Complete London dataset |
| **2023 Mastercard** | `Mastercard_3hourly_2023.csv` | Annual | Complete London dataset |
| **2024 Mastercard** | `Mastercard_3hourly_2024.csv` | Annual | Complete London dataset |
| **2025 Mastercard** | `Mastercard_3hourly_2025.csv` | Annual | Complete London dataset |
| **BT Hex Grid** | `BT_3hourly_counts_{year}.csv` | Annual | Complete London hex data |

## ⚙️ **Technical Implementation**

### **Configuration Structure**

#### **Global Configuration**
```yaml
# Global configuration settings
config:
  base_schema: "gisapdata"
  date_formats:
    mastercard_3hourly: "count_date"
    mastercard_weekly: "week_start" 
    bt_footfall: "count_date"
    bt_hex: "count_date"
  
  # Database table mappings for different data sources
  data_sources:
    bt_footfall:
      aggregated_tables:
        bespoke: "econ_busyness_bt_bespokes_3hourly_counts"
        bid: "econ_busyness_bt_bids_3hourly_counts"
        towncentre: "econ_busyness_bt_towncentres_3hourly_counts"
        highstreet: "econ_busyness_bt_highstreets_3hourly_counts"
      raw_table: "bt_footfall_tfl_hex_3hourly"
      lookup_tables:
        hex_bid: "econ_busyness_hex_bid_lookup"
        hex_towncentre: "econ_busyness_hex_towncentre_lookup"
        hex_bespoke: "econ_busyness_hex_bespoke_lookup"
        hex_highstreet: "econ_busyness_hex_highstreet_lookup"
    
    mastercard_3hourly:
      aggregated_tables:
        bespoke: "econ_busyness_mcard_bespokes_3hourly_txn"
        bid: "econ_busyness_mcard_bids_3hourly_txn"
        towncentre: "econ_busyness_mcard_towncentres_3hourly_txn"
        highstreet: "econ_busyness_mcard_highstreets_3hourly_txn"
      raw_table: "econ_busyness_mrli_3hourly_adj"
      lookup_tables:
        quad_bid: "econ_busyness_mcard_BIDs_quad_lookup"
        quad_towncentre: "econ_busyness_mcard_TownCentres_quad_lookup"
        quad_bespoke: "econ_busyness_mcard_bespoke_quad_lookup"
        quad_highstreet: "econ_busyness_mcard_Highstreets_quad_lookup"
```

#### **Partner Sublicense Configuration**
```yaml
# Example: Colliers HSDS sublicense configuration
sublicenses:
  colliers-hsds:
    slug: "colliers---hsds"
    description: "Colliers HSDS sublicense agreement for HOLBA sites"
    contact: "colliers_team@example.com"
    status: "active"
    
    # Data sources this partner receives
    data_sources:
      - bt_footfall
      - mastercard_3hourly
      - mastercard_weekly
      - mastercard_weekly_intl
      - bt_daily
    
    # Geographic and temporal filters
    filters:
      bespoke_area_ids: [112, 113, 114, 115, 116, 117, 118, 197]
    
    # SQL query templates for data extraction
    query_templates:
      bt_footfall_bespoke: |
        SELECT * FROM {schema}.econ_busyness_bt_bespokes_3hourly_counts 
        WHERE bespoke_area_id IN ({bespoke_area_ids})
      
      mastercard_3hourly_bespoke: |
        SELECT * FROM {schema}.econ_busyness_mcard_bespokes_3hourly_txn 
        WHERE bespoke_area_id IN ({bespoke_area_ids})
      
      mastercard_weekly_bespoke: |
        SELECT * FROM {schema}.econ_busyness_mcard_bespoke_txn 
        WHERE bespoke_area_id::int IN ({bespoke_area_ids})
      
      bt_daily: |
        SELECT * FROM {schema}.econ_busyness_bt_daily_agg_cust_raw
        WHERE poi_type = 'bids' AND poi_name = 'Heart of London'
    
    # Output file configurations
    output_configs:
      bt_footfall:
        resource_title: "colliers_hsds_footfall_3hourly_counts.csv"
        file_path: "bt/processed/bespoke/Colliers agreement - Holba sites/"
        custom_date_column: "count_date"
      
      mastercard_3hourly:
        resource_title: "colliers_hsds_mcard_3hourly_txn.csv"
        file_path: "mastercard/mrli_3hourly/processed/bespoke/Colliers agreement - Holba sites/"
        custom_date_column: "count_date"
      
      mastercard_weekly:
        resource_title: "colliers_hsds_mcard_weekly_txn.csv"
        file_path: "mastercard/weekly/processed/bespoke/Colliers agreement - Holba sites/"
        custom_date_column: "week_start"
      
      bt_daily:
        resource_title: "colliers_bt_daily_agg_counts.csv"
        file_path: "bt/processed/daily/Colliers agreement - Holba sites/"
        custom_date_column: "count_date"
```

#### **BID-Based Configuration Example**
```yaml
# Example: Fitzrovia Partnership BID configuration
fitzrovia-partnership:
  slug: "rendle-intelligence-for-fitzrovia-partnership"
  description: "Fitzrovia Partnership BID sublicense"
  contact: "fitzrovia@example.com"
  status: "active"
  
  data_sources:
    - bt_footfall
    - bt_hex
    - mastercard_3hourly
    - mastercard_weekly
  
  filters:
    bid_ids: [21, 77]
  
  query_templates:
    bt_footfall_bid: |
      SELECT * FROM {schema}.econ_busyness_bt_bids_3hourly_counts 
      WHERE bid_id IN ({bid_ids})
    
    bt_hex: |
      SELECT h.*, b.bid_name 
      FROM {schema}.bt_footfall_tfl_hex_3hourly h
      JOIN {schema}.econ_busyness_hex_bid_lookup b ON h.hex_id = b.hex_id
      WHERE b.bid_id IN ({bid_ids})
    
    mastercard_3hourly_quad: |
      SELECT m.*, b.bid_name 
      FROM {schema}.econ_busyness_mrli_3hourly_adj m
      JOIN {schema}.econ_busyness_mcard_BIDs_quad_lookup b ON m.quad_id = b.quad_id
      WHERE b.bid_id IN ({bid_ids})
    
    mastercard_weekly_bid: |
      SELECT * FROM {schema}.econ_busyness_mcard_bids_txn 
      WHERE bid_id IN ({bid_ids})
  
  output_configs:
    bt_footfall:
      resource_title: "Fitzrovia_bt_3hourly_counts.csv"
      file_path: "bt/processed/bid/fitzrovia/"
      custom_date_column: "count_date"
    
    bt_hex:
      resource_title: "Fitzrovia_bt_hex_3hourly_counts.csv"
      file_path: "bt/processed/hex_grid/fitzrovia/"
      custom_date_column: "count_date"
    
    mastercard_3hourly:
      resource_title: "Fitzrovia_mcard_quad_3hourly_txn.csv"
      file_path: "mastercard/mrli_3hourly/processed/MRLI_3yr_compressed/Fitzrovia/"
      custom_date_column: "count_date"
    
    mastercard_weekly:
      resource_title: "Fitzrovia_hsds_mcard_weekly_txn.csv"
      file_path: "mastercard/weekly/processed/bid/fitzrovia/"
      custom_date_column: "week_start"
```

### **Command Line Usage**

```bash
# Process all active sublicenses
python -m highstreets.core.processors.sublicense_processor --all

# Process specific sublicense
python -m highstreets.core.processors.sublicense_processor --sublicense colliers-hsds

# Validate configuration
python -m highstreets.core.processors.sublicense_processor --validate

# Get sublicense information
python -m highstreets.core.processors.sublicense_processor --info

# Include inactive sublicenses
python -m highstreets.core.processors.sublicense_processor --all --include-inactive
```

### **Geographic Filtering**
Partners receive data for specific geographic areas based on their agreement:

| Filter Type | Description | Configuration Key |
|-------------|-------------|-------------------|
| **Bespoke Area IDs** | Custom-defined project areas | `bespoke_area_ids` |
| **BID IDs** | Business Improvement Districts | `bid_ids` |
| **Town Centre IDs** | Designated town centre boundaries | `tc_ids` |
| **High Street IDs** | High street boundaries | `highstreet_ids` |
| **Borough Names** | London borough boundaries | `borough_names` |

### **Temporal Filtering**
Data can be filtered by various temporal dimensions:

| Filter Type | Description | Use Cases |
|-------------|-------------|-----------|
| **Date Ranges** | Specific start and end dates | Historical analysis |
| **Year Lists** | Multiple specific years | Academic research |
| **Week Ranges** | Week number filtering | Seasonal analysis |
| **Month Ranges** | Monthly data extraction | Trend analysis |

## 📤 **Distribution Channels**

### **1. Secure S3 Storage**
- **Location**: AWS S3 bucket with partner-specific folders
- **Access**: Secure, time-limited access URLs
- **Organization**: Hierarchical folder structure by partner and data type
- **Retention**: Configurable retention policies

### **2. London Datastore Integration**
- **Public Datasets**: Anonymized, aggregated data for public access
- **Partner Datasets**: Controlled access through datastore portal
- **Metadata**: Rich metadata including data lineage and quality indicators
- **APIs**: RESTful APIs for programmatic access

### **3. Direct Transfer**
- **SFTP**: Secure file transfer for large datasets
- **Email**: Automated notifications with download links
- **API Access**: Direct database queries for technical partners
- **Custom Integration**: Tailored solutions for specific partner needs

## 📊 **Data Quality Assurance**

### **Automated Validation**
- **Volume Checks**: Ensure expected data volumes from SQL queries
- **Schema Validation**: Verify column structures and data types
- **Geographic Coverage**: Validate complete coverage of requested areas
- **Temporal Completeness**: Check for missing time periods

### **Performance Monitoring**
```python
# Example performance tracking
{
    'queries_executed': 24,
    'total_rows_processed': 485692,
    'files_created': 18,
    'uploads_completed': 16,
    'average_query_time': 2.3,
    'total_processing_time': 127.5
}
```

## 🔧 **Partner Onboarding Process**

### **1. Agreement Development**
- **Needs Assessment**: Understanding partner data requirements
- **Geographic Scope**: Defining relevant geographic boundaries
- **Data Sources**: Selecting appropriate datasets
- **Delivery Format**: Agreeing on file formats and delivery methods

### **2. Technical Setup**
- **YAML Configuration**: Add partner definition to `sublicenses.yaml`
- **Query Development**: Create partner-specific SQL query templates
- **Testing Environment**: Sandbox testing with sample data
- **Production Deployment**: Activate sublicense in production

### **3. Quality Assurance**
- **Data Validation**: Ensuring data meets partner requirements
- **Delivery Testing**: Confirming successful file delivery
- **Documentation Provision**: Partner-specific documentation
- **Training Sessions**: Partner team training on data usage

## 📈 **System Performance**

### **Processing Efficiency**
- **Database-Side Filtering**: ~10x faster than DataFrame operations
- **Parallel Processing**: Multiple sublicenses processed simultaneously
- **Query Optimization**: Cached queries and optimized SQL execution
- **Resource Efficiency**: Minimal memory footprint

## 🔗 **Related Information**

- **[BT Data Flow](03.1-bt-data-flow.md)**: How BT sublicense data is generated
- **[Mastercard Data Flow](03.2-mastercard-data-flow.md)**: How Mastercard sublicense data is generated
- **[Database Schema](04-database-schema.md)**: Understanding data structures and table relationships
- **[Data Governance](08-data-governance.md)**: Governance framework and policies

---

**Sublicensing Summary**:
- **Active Partners**: 8+ sublicense agreements serving diverse stakeholders
- **YAML-Driven**: Centralized configuration with no code changes required
- **Database-Optimized**: Efficient SQL-based processing and filtering
- **Automated Processing**: Single command processes all partners
- **Quality Assured**: Comprehensive validation, monitoring, and error handling
- **Scalable Architecture**: Designed to support 50+ partners efficiently