---
title: "Database Schema & Tables"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["database", "schema", "postgresql", "tables"]
---

# Database Schema & Tables

## 🗄️ **Overview**

The Highstreets Data Platform uses a PostgreSQL database with a carefully designed schema that optimizes for both analytical queries and operational performance. The database structure reflects the geographic hierarchy of London and supports multiple data resolutions and time periods.

## 📊 **Schema Architecture**

### **Database Structure**

The database is organized into the following main categories:

#### **🏗️ Raw Data Tables**
- [`bt_footfall_tfl_hex_3hourly`](#bt_footfall_tfl_hex_3hourly) - BT footfall data at hex grid resolution
- [`econ_busyness_mrli_3hourly_adj`](#econ_busyness_mrli_3hourly_adj) - Adjusted Mastercard transaction data at quad resolution

#### **🗺️ Geographic Lookup Tables**
- [`econ_busyness_hex_highstreet_lookup`](#econ_busyness_hex_highstreet_lookup) - Hex to high street mapping
- [`econ_busyness_hex_towncentre_lookup`](#econ_busyness_hex_towncentre_lookup) - Hex to town centre mapping
- [`econ_busyness_hex_bid_lookup`](#econ_busyness_hex_bid_lookup) - Hex to BID mapping
- [`econ_busyness_hex_bespoke_lookup`](#econ_busyness_hex_bespoke_lookup) - Hex to bespoke area mapping
- [`econ_busyness_mcard_*_quad_lookup`](#econ_busyness_mcard_quad_lookup) - Quad to boundary mapping tables

#### **📈 BT Aggregated Tables**
- [`econ_busyness_bt_highstreets_3hourly_counts`](#econ_busyness_bt_highstreets_3hourly_counts) - High street aggregations
- [`econ_busyness_bt_towncentres_3hourly_counts`](#econ_busyness_bt_towncentres_3hourly_counts) - Town centre aggregations
- [`econ_busyness_bt_bids_3hourly_counts`](#econ_busyness_bt_bids_3hourly_counts) - BID aggregations
- [`econ_busyness_bt_bespokes_3hourly_counts`](#econ_busyness_bt_bespokes_3hourly_counts) - Bespoke area aggregations

#### **💳 Mastercard Transaction Tables**
- [`econ_busyness_mcard_*_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) - 3-hourly transaction data by boundary
- [`econ_busyness_mcard_*_txn`](#econ_busyness_mcard_txn) - Weekly transaction data by boundary
- [`econ_busyness_mcard_*_yoy`](#econ_busyness_mcard_yoy) - Year-over-year growth calculations

#### **🔗 Combined Views**
- [`econ_busyness_bt_3hourly_counts`](#econ_busyness_bt_3hourly_counts) - Unified BT data across all boundary types
- [`econ_busyness_mcard_3hourly_txn`](#econ_busyness_mcard_3hourly_txn_view) - Unified Mastercard data across all boundary types

#### **📋 Reference Data**
- [`econ_busyness_mcard_adjustment_factor`](#econ_busyness_mcard_adjustment_factor) - Monthly adjustment factors
- [`econ_busyness_cpih_table`](#econ_busyness_cpih_table) - Consumer Price Index data

### **Key Design Principles**
- **Temporal Partitioning**: Data organized by date for efficient queries
- **Geographic Hierarchy**: Multiple aggregation levels from raw to boundary-specific
- **Data Quality**: Built-in validation and consistency checks
- **Performance Optimization**: Indexes and partitioning for fast analytics

## 🏗️ **Raw Data Tables**

### **bt_footfall_tfl_hex_3hourly**
*Primary table for BT footfall data at hex grid resolution*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **hex_id** | VARCHAR(20) | NOT NULL | Unique TfL hex grid identifier |
| **count_date** | DATE | NOT NULL | Date of footfall measurement |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **resident** | INTEGER | NULL | Estimated resident population |
| **visitor** | INTEGER | NULL | Estimated visitor population |
| **worker** | INTEGER | NULL | Estimated worker population |
| **loyalty_percentage** | FLOAT | NULL | Visitor loyalty percentage (0-100) |
| **dwell_time** | FLOAT | NULL | Average dwell time in minutes |

**Primary Key**: `(hex_id, count_date, hours)`  
**Indexes**: `count_date`, `hex_id`, `(count_date, hours)`  
**Partitioning**: Monthly partitions by `count_date`  
**Average Records/Week**: ~750,000  

### **econ_busyness_mrli_3hourly_adj**
*Adjusted Mastercard transaction data at quad resolution*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard geographic quad identifier |
| **count_date** | DATE | NOT NULL | Transaction date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **txn_amt** | DECIMAL(15,2) | NULL | Raw transaction amount (GBP) |
| **txn_amt_adj** | DECIMAL(15,2) | NULL | Adjusted transaction amount (GBP) |
| **txn_cnt** | INTEGER | NULL | Number of transactions |
| **avg_spend_amt** | DECIMAL(10,2) | NULL | Average spend per transaction |
| **ldn_ref** | INTEGER | NULL | London reference grid identifier |

**Primary Key**: `(quad_id, count_date, hours)`  
**Indexes**: `count_date`, `quad_id`, `ldn_ref`, `(count_date, hours)`  
**Partitioning**: Monthly partitions by `count_date`  
**Average Records/Month**: ~2,000,000  

## 🗺️ **Geographic Lookup Tables**

### **econ_busyness_hex_highstreet_lookup**
*Links hex grid cells to high street boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier |
| **highstreet_id** | INTEGER | NOT NULL | Unique high street identifier |
| **highstreet_name** | VARCHAR(100) | NOT NULL | Official high street name |

**Primary Key**: `(hex_id, highstreet_id)`  
**Records**: ~15,000 hex-to-highstreet relationships  

### **econ_busyness_hex_towncentre_lookup**
*Links hex grid cells to town centre boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier |
| **tc_id** | INTEGER | NOT NULL | Unique town centre identifier |
| **tc_name** | VARCHAR(100) | NOT NULL | Official town centre name |

**Primary Key**: `(hex_id, tc_id)`  
**Records**: ~12,000 hex-to-towncentre relationships  

### **econ_busyness_hex_bid_lookup**
*Links hex grid cells to Business Improvement District boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier |
| **bid_id** | INTEGER | NOT NULL | Unique BID identifier |
| **bid_name** | VARCHAR(100) | NOT NULL | Official BID name |

**Primary Key**: `(hex_id, bid_id)`  
**Records**: ~8,000 hex-to-BID relationships  

### **econ_busyness_hex_bespoke_lookup**
*Links hex grid cells to custom-defined areas*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier |
| **bespoke_area_id** | INTEGER | NOT NULL | Unique bespoke area identifier |
| **name** | VARCHAR(100) | NOT NULL | Bespoke area name or description |

**Primary Key**: `(hex_id, bespoke_area_id)`  
**Records**: ~5,000 hex-to-bespoke relationships  

### **econ_busyness_mcard_*_quad_lookup**
*Links Mastercard quads to various boundary types*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard quad identifier |
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier (varies by type) |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name |
| **x** | DECIMAL(10,2) | NULL | X coordinate (EPSG:27700) |
| **y** | DECIMAL(10,2) | NULL | Y coordinate (EPSG:27700) |
| **borough** | VARCHAR(50) | NULL | London borough name |

**Records**: ~25,000 quad-to-boundary relationships across all boundary types  

## 📈 **BT Aggregated Tables**

### **econ_busyness_bt_highstreets_3hourly_counts**
*BT footfall data aggregated by high street boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **highstreet_id** | INTEGER | NOT NULL | High street identifier |
| **highstreet_name** | VARCHAR(100) | NOT NULL | High street name |
| **count_date** | DATE | NOT NULL | Measurement date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **x** | DECIMAL(10,2) | NULL | High street centroid X coordinate |
| **y** | DECIMAL(10,2) | NULL | High street centroid Y coordinate |
| **borough** | VARCHAR(50) | NULL | London borough |
| **resident** | INTEGER | NULL | Aggregated resident count |
| **visitor** | INTEGER | NULL | Aggregated visitor count |
| **worker** | INTEGER | NULL | Aggregated worker count |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) |

**Primary Key**: `(highstreet_id, count_date, hours)`  
**Indexes**: `count_date`, `highstreet_id`, `borough`  
**Average Records/Week**: ~50,000  

### **econ_busyness_bt_towncentres_3hourly_counts**
*BT footfall data aggregated by town centre boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **tc_id** | INTEGER | NOT NULL | Town centre identifier |
| **tc_name** | VARCHAR(100) | NOT NULL | Town centre name |
| **count_date** | DATE | NOT NULL | Measurement date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **x** | DECIMAL(10,2) | NULL | Town centre centroid X coordinate |
| **y** | DECIMAL(10,2) | NULL | Town centre centroid Y coordinate |
| **borough** | VARCHAR(50) | NULL | London borough |
| **resident** | INTEGER | NULL | Aggregated resident count |
| **visitor** | INTEGER | NULL | Aggregated visitor count |
| **worker** | INTEGER | NULL | Aggregated worker count |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) |

**Primary Key**: `(tc_id, count_date, hours)`  
**Average Records/Week**: ~40,000  

### **econ_busyness_bt_bids_3hourly_counts**
*BT footfall data aggregated by BID boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **bid_id** | INTEGER | NOT NULL | BID identifier |
| **bid_name** | VARCHAR(100) | NOT NULL | BID name |
| **count_date** | DATE | NOT NULL | Measurement date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **resident** | INTEGER | NULL | Aggregated resident count |
| **visitor** | INTEGER | NULL | Aggregated visitor count |
| **worker** | INTEGER | NULL | Aggregated worker count |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) |

**Primary Key**: `(bid_id, count_date, hours)`  
**Average Records/Week**: ~20,000  

### **econ_busyness_bt_bespokes_3hourly_counts**
*BT footfall data aggregated by bespoke area boundaries*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **bespoke_area_id** | INTEGER | NOT NULL | Bespoke area identifier |
| **name** | VARCHAR(100) | NOT NULL | Bespoke area name |
| **count_date** | DATE | NOT NULL | Measurement date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **resident** | INTEGER | NULL | Aggregated resident count |
| **visitor** | INTEGER | NULL | Aggregated visitor count |
| **worker** | INTEGER | NULL | Aggregated worker count |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) |

**Primary Key**: `(bespoke_area_id, count_date, hours)`  
**Average Records/Week**: ~15,000  

## 💳 **Mastercard Transaction Tables**

### **econ_busyness_mcard_*_3hourly_txn**
*Mastercard transaction data aggregated by boundary type (3-hourly)*

**Table Examples**:
- `econ_busyness_mcard_highstreets_3hourly_txn`
- `econ_busyness_mcard_towncentres_3hourly_txn`
- `econ_busyness_mcard_bids_3hourly_txn`
- `econ_busyness_mcard_bespokes_3hourly_txn`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier (varies by table) |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name |
| **count_date** | DATE | NOT NULL | Transaction date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **txn_amt** | DECIMAL(15,2) | NULL | Total transaction amount (adjusted) |
| **txn_cnt** | INTEGER | NULL | Total transaction count |
| **x** | DECIMAL(10,2) | NULL | Boundary centroid X coordinate |
| **y** | DECIMAL(10,2) | NULL | Boundary centroid Y coordinate |
| **borough** | VARCHAR(50) | NULL | London borough |

**Primary Key**: `(boundary_id, count_date, hours)`  
**Average Records/Month**: Varies by boundary type  

### **econ_busyness_mcard_*_txn** (Weekly Data)
*Mastercard transaction data aggregated weekly by boundary type*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name |
| **week_start** | DATE | NOT NULL | Week starting date (Monday) |
| **yr** | INTEGER | NOT NULL | Year |
| **wk** | INTEGER | NOT NULL | Week number (1-52) |
| **txn_amt_wd_retail** | DECIMAL(15,2) | NULL | Weekday retail transaction amount |
| **txn_amt_we_retail** | DECIMAL(15,2) | NULL | Weekend retail transaction amount |
| **txn_amt_wd_eating** | DECIMAL(15,2) | NULL | Weekday eating transaction amount |
| **txn_amt_we_eating** | DECIMAL(15,2) | NULL | Weekend eating transaction amount |
| **txn_amt_wd_apparel** | DECIMAL(15,2) | NULL | Weekday apparel transaction amount |
| **txn_amt_we_apparel** | DECIMAL(15,2) | NULL | Weekend apparel transaction amount |

**Primary Key**: `(boundary_id, week_start)`  
**Average Records/Month**: Varies by boundary type  

### **econ_busyness_mcard_*_yoy** (Year-over-Year Data)
*Year-over-year growth calculations for Mastercard data*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name |
| **week_start** | DATE | NOT NULL | Week starting date |
| **yr** | INTEGER | NOT NULL | Year |
| **wk** | INTEGER | NOT NULL | Week number |
| **yoy_growth_retail** | DECIMAL(8,4) | NULL | Year-over-year growth rate (retail) |
| **yoy_growth_eating** | DECIMAL(8,4) | NULL | Year-over-year growth rate (eating) |
| **yoy_growth_apparel** | DECIMAL(8,4) | NULL | Year-over-year growth rate (apparel) |

**Primary Key**: `(boundary_id, week_start)`  

## 🔗 **Combined Views**

### **econ_busyness_bt_3hourly_counts**
*Unified view combining all BT boundary types*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **count_date** | DATE | NOT NULL | Measurement date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **id** | INTEGER | NOT NULL | Boundary ID |
| **name** | VARCHAR(100) | NOT NULL | Boundary name |
| **layer** | VARCHAR(20) | NOT NULL | Boundary type |
| **resident** | INTEGER | NULL | Aggregated resident count |
| **visitor** | INTEGER | NULL | Aggregated visitor count |
| **worker** | INTEGER | NULL | Aggregated worker count |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time |

**Primary Key**: `(count_date, hours, id, layer)`  
**Purpose**: Simplified querying across all boundary types  

**Layer Values**:
- `"highstreets"` - High street boundaries
- `"towncentres"` - Town centre boundaries  
- `"bids"` - Business Improvement Districts
- `"bespoke"` - Custom-defined areas

### **econ_busyness_mcard_3hourly_txn**
*Unified view combining all Mastercard 3-hourly boundary types*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **count_date** | DATE | NOT NULL | Transaction date |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period |
| **id** | INTEGER | NOT NULL | Boundary ID |
| **name** | VARCHAR(100) | NOT NULL | Boundary name |
| **layer** | VARCHAR(20) | NOT NULL | Boundary type |
| **txn_amt** | DECIMAL(15,2) | NULL | Total transaction amount |
| **txn_cnt** | INTEGER | NULL | Total transaction count |

**Primary Key**: `(count_date, hours, id, layer)`  
**Purpose**: Simplified querying across all Mastercard boundary types  

## 📋 **Reference Data Tables**

### **econ_busyness_mcard_adjustment_factor**
*Monthly adjustment factors for Mastercard data*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **yr** | INTEGER | NOT NULL | Year |
| **month** | INTEGER | NOT NULL | Month (1-12) |
| **inner_outer** | VARCHAR(10) | NOT NULL | Inner or Outer London |
| **adjustment_factor_retail** | DECIMAL(8,6) | NULL | Retail adjustment factor |
| **adjustment_factor_eating** | DECIMAL(8,6) | NULL | Eating adjustment factor |
| **adjustment_factor_apparel** | DECIMAL(8,6) | NULL | Apparel adjustment factor |

**Primary Key**: `(yr, month, inner_outer)`  
**Purpose**: Monthly correction factors for market share and payment trends  

### **econ_busyness_cpih_table**
*Consumer Price Index data from ONS*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **yr** | INTEGER | NOT NULL | Year |
| **month** | INTEGER | NOT NULL | Month (1-12) |
| **aggregate** | VARCHAR(50) | NOT NULL | CPI category |
| **cpi_index** | DECIMAL(8,4) | NULL | CPI index value (2018=100) |

**Primary Key**: `(yr, month, aggregate)`  
**Purpose**: Inflation adjustment for monetary values  

**Common CPI Categories**:
- `"CPIH OVERALL INDEX"` - Overall inflation
- `"CPIH: RESTAURANTS & HOTELS"` - Food service inflation
- `"CPIH: CLOTHING & FOOTWEAR"` - Apparel inflation

### **econ_busyness_mcard_Inner_Outer_quad_lookup**
*Classification of London areas as Inner or Outer*

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard quad identifier |
| **inner_outer** | VARCHAR(10) | NOT NULL | Inner or Outer London designation |

**Primary Key**: `quad_id`  
**Purpose**: Geographic classification for adjustment factors  

## 🎯 **Table Relationships**

### **Data Flow Relationships**

#### **BT Data Flow**

bt_footfall_tfl_hex_3hourly (Raw)
    ├─ JOIN hex_highstreet_lookup → bt_highstreets_3hourly_counts
    ├─ JOIN hex_towncentre_lookup → bt_towncentres_3hourly_counts  
    ├─ JOIN hex_bid_lookup → bt_bids_3hourly_counts
    └─ JOIN hex_bespoke_lookup → bt_bespokes_3hourly_counts
                                        ↓
                            bt_3hourly_counts (Combined View)


#### **Mastercard Data Flow**

econ_busyness_mrli_3hourly_adj (Raw)
    ├─ JOIN mcard_Highstreets_quad_lookup → mcard_highstreets_3hourly_txn
    ├─ JOIN mcard_TownCentres_quad_lookup → mcard_towncentres_3hourly_txn
    ├─ JOIN mcard_BIDs_quad_lookup → mcard_bids_3hourly_txn
    └─ JOIN mcard_bespoke_quad_lookup → mcard_bespokes_3hourly_txn
                                        ↓
                            mcard_3hourly_txn (Combined View)


### **Key Joins**

| Join Purpose | Left Table | Right Table | Join Key |
|--------------|------------|-------------|----------|
| **Hex to High Street** | bt_footfall_tfl_hex_3hourly | hex_highstreet_lookup | hex_id |
| **Quad to BID** | econ_busyness_mrli_3hourly_adj | mcard_BIDs_quad_lookup | quad_id |
| **Adjustment Factors** | mcard data | adjustment_factor | (yr, month, inner_outer) |
| **CPI Adjustment** | mcard data | cpih_table | (yr, month, aggregate) |

## 🔍 **Performance Considerations**

### **Indexing Strategy**

| Table Type | Primary Indexes | Secondary Indexes | Partition Key |
|------------|----------------|------------------|---------------|
| **Raw Data** | Primary key | count_date, spatial_id | count_date (monthly) |
| **Aggregated** | Primary key | count_date, boundary_id | count_date (monthly) |
| **Lookup** | Primary key | boundary_id, spatial_id | None |
| **Reference** | Primary key | date columns | None |

### **Query Optimization**
- **Time-based queries**: Partitioning by month for efficient date range queries
- **Spatial queries**: Indexes on boundary IDs and spatial identifiers
- **Aggregation queries**: Pre-computed aggregations at multiple geographic levels
- **Join optimization**: Foreign key relationships and appropriate index coverage

## 🔗 **Related Information**

- **[BT Footfall Data Flow](03.1-bt-data-flow.md)**: How BT tables are populated
- **[Mastercard Transaction Data Flow](03.2-mastercard-data-flow.md)**: How Mastercard tables are populated
- **[Sublicensing & Data Distribution](07-sublicensing.md)**: How tables are used for partner exports
- **[Data Processing Workflows](03-data-workflows.md)**: Overall data processing context

---

**Schema Summary**:
- **Total Tables**: 50+ tables across raw, aggregated, lookup, and reference data
- **Primary Storage**: PostgreSQL with monthly partitioning
- **Data Volume**: 63M+ records annually across all tables
- **Performance**: Optimized for analytical queries with appropriate indexing

**Next Steps**: Review [BT Footfall Data Flow](03.1-bt-data-flow.md) to understand how these tables are populated 