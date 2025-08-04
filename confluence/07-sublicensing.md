---
title: "Sublicensing & Data Distribution"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["sublicensing", "partners", "data-distribution", "governance"]
---

# Sublicensing & Data Distribution

## 🤝 **Overview**

The Highstreets Data Platform operates a sophisticated sublicensing system that enables controlled data sharing with external partners while maintaining data governance standards. This system supports strategic partnerships, commercial agreements, and research collaborations through automated, secure data distribution.

## 📋 **Sublicensing Framework**

### **Core Principles**
- **Data Governance**: All data sharing follows strict governance and privacy protocols
- **Automated Processing**: Database-driven filtering and export generation
- **Flexible Agreements**: Customizable data scopes and delivery formats
- **Quality Assurance**: Consistent data validation and delivery monitoring
- **Compliance**: GDPR compliance and commercial data licensing requirements

### **System Architecture**
```
📊 Database Tables → 🔍 Partner Filters → 📁 Data Exports → 📤 Distribution
      │                     │                  │              │
 Production data     YAML-driven         CSV files      S3 Storage
      │              configuration           │              │
 All boundaries      Geographic &       Custom formats  London Datastore
                    temporal filters                    Partner access
```

## 📖 **Sublicense Agreement Types**

### **1. Commercial Partnerships**
- **Business Improvement Districts (BIDs)**: Local area data for business planning
- **Real Estate Consultancies**: Market analysis and investment insights
- **Commercial Property Firms**: Location intelligence and trend analysis

### **2. Academic Research**
- **Universities**: Economic research and urban studies
- **Research Institutions**: Policy analysis and academic publications
- **Student Projects**: Educational use with appropriate data governance

### **3. Public Sector Collaboration**
- **Local Authorities**: Borough-level economic insights
- **Planning Departments**: Development impact assessment
- **Transport Authorities**: Movement and activity correlation

## 🎯 **Active Sublicenses**

### **Commercial Partners**

#### **Colliers HSDS Agreement**
- **Partner**: Colliers International - High Street Data Service
- **Slug**: `colliers---hsds`
- **Status**: Active
- **Geographic Scope**: HOLBA sites (8 bespoke areas)
- **Data Sources**: BT footfall, Mastercard 3-hourly, Mastercard weekly, International data

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **BT Footfall** | `colliers_hsds_footfall_3hourly_counts.csv` | Weekly | Bespoke areas 112-118, 197 |
| **Mastercard 3-Hourly** | `colliers_hsds_mcard_3hourly_txn.csv` | Monthly | Same geographic coverage |
| **Mastercard Weekly** | `colliers_hsds_mcard_weekly_txn.csv` | Monthly | Same geographic coverage |
| **BT Daily** | `colliers_bt_daily_agg_counts.csv` | Weekly | HOLBA sites |

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
| **BT Aggregated** | `avison_young_bt_3hourly_counts.csv` | Weekly | Town centres + bespoke |
| **BT Hex (TC)** | `avison_tc_bt_hex_3hourly_counts.csv` | Weekly | Town centre hex cells |
| **BT Hex (Bespoke)** | `avison_bespoke_bt_hex_3hourly_counts.csv` | Weekly | Bespoke area hex cells |
| **Mastercard (TC)** | `avison_young_tc_mcard_quad_3hourly_txn.csv` | Monthly | Town centre quads |
| **Mastercard (Bespoke)** | `avison_young_bespoke_mcard_quad_3hourly_txn.csv` | Monthly | Bespoke area quads |

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
- **Data Sources**: Mastercard 3-hourly (historical years)

**Data Deliverables**:

| Dataset | File Name | Update Frequency | Coverage |
|---------|-----------|------------------|----------|
| **2022 Data** | `Mastercard_3hourly_2022.csv` | Annual | Complete London dataset |
| **2023 Data** | `Mastercard_3hourly_2023.csv` | Annual | Complete London dataset |
| **2024 Data** | `Mastercard_3hourly_2024.csv` | Annual | Complete London dataset |
| **2025 Data** | `Mastercard_3hourly_2025.csv` | Annual | Complete London dataset |

## ⚙️ **Technical Implementation**

### **Configuration Management**
The sublicensing system is driven by a YAML configuration file that defines all partner agreements, data sources, and processing requirements.

#### **Global Configuration Structure**
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
```

### **Database-Driven Processing**
The system executes SQL queries directly against the production database:

1. **Query Generation**: Templates populated with partner-specific filters
2. **Direct Execution**: SQL queries run against PostgreSQL database
3. **Result Export**: Query results saved as CSV files
4. **Distribution**: Files uploaded to S3 and London Datastore

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

## 🔄 **Processing Schedule**

### **Weekly Processing (BT Data)**

| Day | Activity | Partners Affected |
|-----|----------|------------------|
| **Monday** | BT data collection | All BT data partners |
| **Tuesday** | Data processing and validation | All BT data partners |
| **Wednesday** | Partner export generation | All BT data partners |
| **Thursday** | Distribution and notifications | All BT data partners |

### **Monthly Processing (Mastercard Data)**

| Week | Activity | Partners Affected |
|------|----------|------------------|
| **Week 1** | Data collection and processing | All Mastercard partners |
| **Week 2** | Adjustment application and validation | All Mastercard partners |
| **Week 3** | Export generation and quality checks | All Mastercard partners |
| **Week 4** | Distribution and delivery confirmation | All Mastercard partners |

## 📊 **Data Quality Assurance**

### **Pre-Export Validation**
- **Volume Checks**: Ensure expected data volumes
- **Completeness Validation**: Check for missing time periods or geographic coverage
- **Quality Metrics**: Validate against historical patterns
- **Schema Compliance**: Ensure consistent column structures

### **Post-Export Monitoring**
- **Delivery Confirmation**: Track successful file uploads
- **Download Monitoring**: Monitor partner access patterns
- **Error Reporting**: Automated alerts for failed deliveries
- **Partner Feedback**: Regular quality feedback collection

## 🛡️ **Data Governance & Compliance**

### **Privacy Protection**
- **Aggregation Thresholds**: Minimum aggregation levels to prevent individual identification
- **Geographic Generalization**: Appropriate spatial resolution for privacy
- **Temporal Aggregation**: Time-based aggregation where required
- **Data Anonymization**: All personal identifiers removed at source

### **Commercial Compliance**
- **License Terms**: Clearly defined usage rights and restrictions
- **Attribution Requirements**: Proper data source attribution
- **Redistribution Controls**: Limits on data sharing by partners
- **Usage Monitoring**: Tracking of data usage patterns

### **Technical Security**
- **Encrypted Transfer**: All data transfers use encryption
- **Access Controls**: Role-based access to partner data
- **Audit Logging**: Comprehensive logging of all data access
- **Retention Policies**: Automatic cleanup of expired data

## 📈 **Partner Success Metrics**

### **Engagement Metrics**

| Metric | Measurement | Target |
|--------|-------------|--------|
| **Data Access Frequency** | Downloads per month | Monthly access |
| **Query Success Rate** | Successful exports / Total exports | >98% |
| **Delivery Timeliness** | On-time delivery rate | >99% |
| **Partner Satisfaction** | Quarterly surveys | >4.5/5 |

### **Data Usage Analytics**
- **Download Patterns**: Most accessed datasets and time periods
- **Geographic Interest**: Most requested geographic areas
- **Temporal Preferences**: Preferred data time ranges
- **Format Preferences**: CSV vs API access patterns

## 🔧 **Partner Onboarding Process**

### **1. Agreement Development**
- **Needs Assessment**: Understanding partner data requirements
- **Geographic Scope**: Defining relevant geographic boundaries
- **Data Sources**: Selecting appropriate datasets
- **Delivery Format**: Agreeing on file formats and delivery methods

### **2. Technical Setup**
- **Configuration Creation**: YAML configuration development
- **Query Development**: Custom SQL query templates
- **Testing Environment**: Sandbox testing with sample data
- **Production Deployment**: Live system configuration

### **3. Quality Assurance**
- **Data Validation**: Ensuring data meets partner requirements
- **Delivery Testing**: Confirming successful file delivery
- **Documentation Provision**: Partner-specific documentation
- **Training Sessions**: Partner team training on data usage

### **4. Ongoing Support**
- **Regular Reviews**: Quarterly partnership reviews
- **Technical Support**: Ongoing technical assistance
- **Data Updates**: Notification of schema or process changes
- **Feedback Integration**: Incorporating partner feedback into system improvements

## 🔗 **Related Information**

- **[Data Processing Workflows](03-data-workflows.md)**: How sublicense data is generated
- **[Database Schema](04-database-schema.md)**: Understanding data structures
- **[Data Governance](08-data-governance.md)**: Governance framework and policies

---

**Sublicensing Summary**:
- **Active Partners**: 8 sublicense agreements serving diverse stakeholders
- **Data Coverage**: BT footfall and Mastercard transaction data across London
- **Geographic Flexibility**: From single BIDs to London-wide coverage
- **Automated Processing**: Database-driven, YAML-configured export generation
- **Quality Assurance**: Comprehensive validation and monitoring systems

**Next Steps**: Learn about [Data Governance & Quality](08-data-governance.md) 