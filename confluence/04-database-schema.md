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
```
📁 gisapdata (schema)
├── 🏗️ Raw Data Tables
│   ├── bt_footfall_tfl_hex_3hourly
│   └── econ_busyness_mrli_3hourly_adj
├── 🗺️ Geographic Lookup Tables  
│   ├── econ_busyness_hex_*_lookup
│   └── econ_busyness_mcard_*_quad_lookup
├── 📈 BT Aggregated Tables
│   ├── econ_busyness_bt_highstreets_3hourly_counts
│   ├── econ_busyness_bt_towncentres_3hourly_counts
│   ├── econ_busyness_bt_bids_3hourly_counts
│   └── econ_busyness_bt_bespokes_3hourly_counts
├── 💳 Mastercard Transaction Tables
│   ├── econ_busyness_mcard_*_3hourly_txn
│   ├── econ_busyness_mcard_*_txn (weekly)
│   └── econ_busyness_mcard_*_yoy (year-over-year)
├── 🔗 Combined Views
│   ├── econ_busyness_bt_3hourly_counts
│   └── econ_busyness_mcard_3hourly_txn
└── 📋 Reference Data
    ├── econ_busyness_mcard_adjustment_factor
    └── econ_busyness_cpih_table
```

### **Key Design Principles**
- **Temporal Partitioning**: Data organized by date for efficient queries
- **Geographic Hierarchy**: Multiple aggregation levels from raw to boundary-specific
- **Data Quality**: Built-in validation and consistency checks
- **Performance Optimization**: Indexes and partitioning for fast analytics

## 🏗️ **Raw Data Tables**

### **bt_footfall_tfl_hex_3hourly**
*Primary table for BT footfall data at hex grid resolution*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **hex_id** | VARCHAR(20) | NOT NULL | Unique TfL hex grid identifier | "8a34c0b18867fff", "8a34c0b1886ffff" |
| **count_date** | DATE | NOT NULL | Date of footfall measurement | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **resident** | INTEGER | NULL | Estimated resident population | 1250, 890, 2340 |
| **visitor** | INTEGER | NULL | Estimated visitor population | 430, 1200, 680 |
| **worker** | INTEGER | NULL | Estimated worker population | 2100, 150, 3200 |
| **loyalty_percentage** | FLOAT | NULL | Visitor loyalty percentage (0-100) | 67.5, 42.1, 88.9 |
| **dwell_time** | FLOAT | NULL | Average dwell time in minutes | 45.7, 120.3, 25.8 |

**Primary Key**: `(hex_id, count_date, hours)`  
**Indexes**: `count_date`, `hex_id`, `(count_date, hours)`  
**Partitioning**: Monthly partitions by `count_date`  
**Average Records/Week**: ~750,000  

### **econ_busyness_mrli_3hourly_adj**
*Adjusted Mastercard transaction data at quad resolution*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard geographic quad identifier | 1234567890123, 9876543210987 |
| **count_date** | DATE | NOT NULL | Transaction date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **txn_amt** | DECIMAL(15,2) | NULL | Raw transaction amount (GBP) | 12547.89, 5643.21, 89012.34 |
| **txn_amt_adj** | DECIMAL(15,2) | NULL | Adjusted transaction amount (GBP) | 15234.67, 6872.45, 102456.78 |
| **txn_cnt** | INTEGER | NULL | Number of transactions | 145, 67, 312 |
| **avg_spend_amt** | DECIMAL(10,2) | NULL | Average spend per transaction | 86.54, 84.21, 285.23 |
| **ldn_ref** | INTEGER | NULL | London reference grid identifier | 1001, 1002, 1003 |

**Primary Key**: `(quad_id, count_date, hours)`  
**Indexes**: `count_date`, `quad_id`, `ldn_ref`, `(count_date, hours)`  
**Partitioning**: Monthly partitions by `count_date`  
**Average Records/Month**: ~2,000,000  

## 🗺️ **Geographic Lookup Tables**

### **econ_busyness_hex_highstreet_lookup**
*Links hex grid cells to high street boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier | "8a34c0b18867fff" |
| **highstreet_id** | INTEGER | NOT NULL | Unique high street identifier | 101, 102, 103 |
| **highstreet_name** | VARCHAR(100) | NOT NULL | Official high street name | "Oxford Street", "Regent Street" |

**Primary Key**: `(hex_id, highstreet_id)`  
**Records**: ~15,000 hex-to-highstreet relationships  

### **econ_busyness_hex_towncentre_lookup**
*Links hex grid cells to town centre boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier | "8a34c0b18867fff" |
| **tc_id** | INTEGER | NOT NULL | Unique town centre identifier | 201, 202, 203 |
| **tc_name** | VARCHAR(100) | NOT NULL | Official town centre name | "Camden Town", "Covent Garden" |

**Primary Key**: `(hex_id, tc_id)`  
**Records**: ~12,000 hex-to-towncentre relationships  

### **econ_busyness_hex_bid_lookup**
*Links hex grid cells to Business Improvement District boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier | "8a34c0b18867fff" |
| **bid_id** | INTEGER | NOT NULL | Unique BID identifier | 301, 302, 303 |
| **bid_name** | VARCHAR(100) | NOT NULL | Official BID name | "Heart of London BID", "Team London Bridge" |

**Primary Key**: `(hex_id, bid_id)`  
**Records**: ~8,000 hex-to-BID relationships  

### **econ_busyness_hex_bespoke_lookup**
*Links hex grid cells to custom-defined areas*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **hex_id** | VARCHAR(20) | NOT NULL | TfL hex grid identifier | "8a34c0b18867fff" |
| **bespoke_area_id** | INTEGER | NOT NULL | Unique bespoke area identifier | 401, 402, 403 |
| **name** | VARCHAR(100) | NOT NULL | Bespoke area name or description | "Olympic Park Area", "Crossrail Impact Zone" |

**Primary Key**: `(hex_id, bespoke_area_id)`  
**Records**: ~5,000 hex-to-bespoke relationships  

### **econ_busyness_mcard_*_quad_lookup**
*Links Mastercard quads to various boundary types*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard quad identifier | 1234567890123 |
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier (varies by type) | 101, 201, 301 |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **x** | DECIMAL(10,2) | NULL | X coordinate (EPSG:27700) | 529090.45, 528234.67 |
| **y** | DECIMAL(10,2) | NULL | Y coordinate (EPSG:27700) | 181680.12, 182456.89 |
| **borough** | VARCHAR(50) | NULL | London borough name | "Westminster", "Camden" |

**Records**: ~25,000 quad-to-boundary relationships across all boundary types  

## 📈 **BT Aggregated Tables**

### **econ_busyness_bt_highstreets_3hourly_counts**
*BT footfall data aggregated by high street boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **highstreet_id** | INTEGER | NOT NULL | High street identifier | 101, 102, 103 |
| **highstreet_name** | VARCHAR(100) | NOT NULL | High street name | "Oxford Street", "Regent Street" |
| **count_date** | DATE | NOT NULL | Measurement date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **x** | DECIMAL(10,2) | NULL | High street centroid X coordinate | 529090.45, 528234.67 |
| **y** | DECIMAL(10,2) | NULL | High street centroid Y coordinate | 181680.12, 182456.89 |
| **borough** | VARCHAR(50) | NULL | London borough | "Westminster", "Camden" |
| **resident** | INTEGER | NULL | Aggregated resident count | 2580, 1450, 3200 |
| **visitor** | INTEGER | NULL | Aggregated visitor count | 8900, 12400, 5600 |
| **worker** | INTEGER | NULL | Aggregated worker count | 15600, 2300, 18900 |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage | 67.54, 72.31, 58.67 |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) | 85.67, 125.43, 45.32 |

**Primary Key**: `(highstreet_id, count_date, hours)`  
**Indexes**: `count_date`, `highstreet_id`, `borough`  
**Average Records/Week**: ~50,000  

### **econ_busyness_bt_towncentres_3hourly_counts**
*BT footfall data aggregated by town centre boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **tc_id** | INTEGER | NOT NULL | Town centre identifier | 201, 202, 203 |
| **tc_name** | VARCHAR(100) | NOT NULL | Town centre name | "Camden Town", "Covent Garden" |
| **count_date** | DATE | NOT NULL | Measurement date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **x** | DECIMAL(10,2) | NULL | Town centre centroid X coordinate | 529090.45, 528234.67 |
| **y** | DECIMAL(10,2) | NULL | Town centre centroid Y coordinate | 181680.12, 182456.89 |
| **borough** | VARCHAR(50) | NULL | London borough | "Westminster", "Camden" |
| **resident** | INTEGER | NULL | Aggregated resident count | 1890, 2340, 1567 |
| **visitor** | INTEGER | NULL | Aggregated visitor count | 6780, 8900, 4320 |
| **worker** | INTEGER | NULL | Aggregated worker count | 12400, 1890, 15600 |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage | 65.43, 71.28, 59.87 |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) | 92.45, 118.67, 67.23 |

**Primary Key**: `(tc_id, count_date, hours)`  
**Average Records/Week**: ~40,000  

### **econ_busyness_bt_bids_3hourly_counts**
*BT footfall data aggregated by BID boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **bid_id** | INTEGER | NOT NULL | BID identifier | 301, 302, 303 |
| **bid_name** | VARCHAR(100) | NOT NULL | BID name | "Heart of London BID", "Team London Bridge" |
| **count_date** | DATE | NOT NULL | Measurement date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **resident** | INTEGER | NULL | Aggregated resident count | 1234, 890, 2100 |
| **visitor** | INTEGER | NULL | Aggregated visitor count | 4567, 6780, 3456 |
| **worker** | INTEGER | NULL | Aggregated worker count | 8901, 1234, 9876 |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage | 68.92, 74.56, 61.23 |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) | 78.34, 145.67, 52.89 |

**Primary Key**: `(bid_id, count_date, hours)`  
**Average Records/Week**: ~20,000  

### **econ_busyness_bt_bespokes_3hourly_counts**
*BT footfall data aggregated by bespoke area boundaries*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **bespoke_area_id** | INTEGER | NOT NULL | Bespoke area identifier | 401, 402, 403 |
| **name** | VARCHAR(100) | NOT NULL | Bespoke area name | "Olympic Park Area", "Crossrail Impact Zone" |
| **count_date** | DATE | NOT NULL | Measurement date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **resident** | INTEGER | NULL | Aggregated resident count | 567, 1234, 890 |
| **visitor** | INTEGER | NULL | Aggregated visitor count | 2345, 4567, 1890 |
| **worker** | INTEGER | NULL | Aggregated worker count | 3456, 567, 6789 |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage | 72.45, 66.78, 80.12 |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time (minutes) | 105.67, 89.23, 134.56 |

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

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier (varies by table) | 101, 201, 301, 401 |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **count_date** | DATE | NOT NULL | Transaction date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **txn_amt** | DECIMAL(15,2) | NULL | Total transaction amount (adjusted) | 125479.89, 67834.21, 234567.45 |
| **txn_cnt** | INTEGER | NULL | Total transaction count | 456, 289, 1234 |
| **x** | DECIMAL(10,2) | NULL | Boundary centroid X coordinate | 529090.45, 528234.67 |
| **y** | DECIMAL(10,2) | NULL | Boundary centroid Y coordinate | 181680.12, 182456.89 |
| **borough** | VARCHAR(50) | NULL | London borough | "Westminster", "Camden" |

**Primary Key**: `(boundary_id, count_date, hours)`  
**Average Records/Month**: Varies by boundary type  

### **econ_busyness_mcard_*_txn** (Weekly Data)
*Mastercard transaction data aggregated weekly by boundary type*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier | 101, 201, 301 |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **week_start** | DATE | NOT NULL | Week starting date (Monday) | "2024-01-15", "2024-01-22" |
| **yr** | INTEGER | NOT NULL | Year | 2024, 2023, 2022 |
| **wk** | INTEGER | NOT NULL | Week number (1-52) | 3, 4, 5 |
| **txn_amt_wd_retail** | DECIMAL(15,2) | NULL | Weekday retail transaction amount | 456789.12, 234567.89 |
| **txn_amt_we_retail** | DECIMAL(15,2) | NULL | Weekend retail transaction amount | 234567.89, 345678.90 |
| **txn_amt_wd_eating** | DECIMAL(15,2) | NULL | Weekday eating transaction amount | 123456.78, 234567.89 |
| **txn_amt_we_eating** | DECIMAL(15,2) | NULL | Weekend eating transaction amount | 234567.89, 123456.78 |
| **txn_amt_wd_apparel** | DECIMAL(15,2) | NULL | Weekday apparel transaction amount | 67890.12, 78901.23 |
| **txn_amt_we_apparel** | DECIMAL(15,2) | NULL | Weekend apparel transaction amount | 78901.23, 67890.12 |

**Primary Key**: `(boundary_id, week_start)`  
**Average Records/Month**: Varies by boundary type  

### **econ_busyness_mcard_*_yoy** (Year-over-Year Data)
*Year-over-year growth calculations for Mastercard data*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **boundary_id** | INTEGER | NOT NULL | Boundary identifier | 101, 201, 301 |
| **boundary_name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **week_start** | DATE | NOT NULL | Week starting date | "2024-01-15", "2024-01-22" |
| **yr** | INTEGER | NOT NULL | Year | 2024, 2023, 2022 |
| **wk** | INTEGER | NOT NULL | Week number | 3, 4, 5 |
| **yoy_growth_retail** | DECIMAL(8,4) | NULL | Year-over-year growth rate (retail) | 0.1234, -0.0567, 0.2345 |
| **yoy_growth_eating** | DECIMAL(8,4) | NULL | Year-over-year growth rate (eating) | 0.0987, 0.1567, -0.0234 |
| **yoy_growth_apparel** | DECIMAL(8,4) | NULL | Year-over-year growth rate (apparel) | 0.2134, -0.1098, 0.0876 |

**Primary Key**: `(boundary_id, week_start)`  

## 🔗 **Combined Views**

### **econ_busyness_bt_3hourly_counts**
*Unified view combining all BT boundary types*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **count_date** | DATE | NOT NULL | Measurement date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **id** | INTEGER | NOT NULL | Boundary ID | 101, 201, 301, 401 |
| **name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **layer** | VARCHAR(20) | NOT NULL | Boundary type | "highstreets", "towncentres", "bids", "bespoke" |
| **resident** | INTEGER | NULL | Aggregated resident count | 1234, 2567, 890 |
| **visitor** | INTEGER | NULL | Aggregated visitor count | 4567, 8901, 2345 |
| **worker** | INTEGER | NULL | Aggregated worker count | 7890, 1234, 5678 |
| **ave_loyalty_percentage** | DECIMAL(5,2) | NULL | Average loyalty percentage | 67.89, 72.45, 59.12 |
| **ave_dwell_time** | DECIMAL(8,2) | NULL | Average dwell time | 89.34, 125.67, 67.89 |

**Primary Key**: `(count_date, hours, id, layer)`  
**Purpose**: Simplified querying across all boundary types  

### **econ_busyness_mcard_3hourly_txn**
*Unified view combining all Mastercard 3-hourly boundary types*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **count_date** | DATE | NOT NULL | Transaction date | "2024-01-15", "2024-01-16" |
| **hours** | VARCHAR(5) | NOT NULL | 3-hour time period | "09-12", "15-18", "21-00" |
| **id** | INTEGER | NOT NULL | Boundary ID | 101, 201, 301, 401 |
| **name** | VARCHAR(100) | NOT NULL | Boundary name | "Oxford Street", "Camden Town" |
| **layer** | VARCHAR(20) | NOT NULL | Boundary type | "highstreets", "towncentres", "bids", "bespoke" |
| **txn_amt** | DECIMAL(15,2) | NULL | Total transaction amount | 125479.89, 234567.45 |
| **txn_cnt** | INTEGER | NULL | Total transaction count | 456, 1234, 789 |

**Primary Key**: `(count_date, hours, id, layer)`  
**Purpose**: Simplified querying across all Mastercard boundary types  

## 📋 **Reference Data Tables**

### **econ_busyness_mcard_adjustment_factor**
*Monthly adjustment factors for Mastercard data*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **yr** | INTEGER | NOT NULL | Year | 2024, 2023, 2022 |
| **month** | INTEGER | NOT NULL | Month (1-12) | 1, 2, 3, 12 |
| **inner_outer** | VARCHAR(10) | NOT NULL | Inner or Outer London | "Inner", "Outer" |
| **adjustment_factor_retail** | DECIMAL(8,6) | NULL | Retail adjustment factor | 1.234567, 0.987654 |
| **adjustment_factor_eating** | DECIMAL(8,6) | NULL | Eating adjustment factor | 1.123456, 0.876543 |
| **adjustment_factor_apparel** | DECIMAL(8,6) | NULL | Apparel adjustment factor | 1.345678, 0.765432 |

**Primary Key**: `(yr, month, inner_outer)`  
**Purpose**: Monthly correction factors for market share and payment trends  

### **econ_busyness_cpih_table**
*Consumer Price Index data from ONS*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **yr** | INTEGER | NOT NULL | Year | 2024, 2023, 2022 |
| **month** | INTEGER | NOT NULL | Month (1-12) | 1, 2, 3, 12 |
| **aggregate** | VARCHAR(50) | NOT NULL | CPI category | "CPIH OVERALL INDEX", "CPIH: CLOTHING" |
| **cpi_index** | DECIMAL(8,4) | NULL | CPI index value (2018=100) | 112.3456, 108.7654 |

**Primary Key**: `(yr, month, aggregate)`  
**Purpose**: Inflation adjustment for monetary values  

### **econ_busyness_mcard_Inner_Outer_quad_lookup**
*Classification of London areas as Inner or Outer*

| Column | Data Type | Nullable | Description | Example Values |
|--------|-----------|----------|-------------|----------------|
| **quad_id** | BIGINT | NOT NULL | Mastercard quad identifier | 1234567890123 |
| **inner_outer** | VARCHAR(10) | NOT NULL | Inner or Outer London designation | "Inner", "Outer" |

**Primary Key**: `quad_id`  
**Purpose**: Geographic classification for adjustment factors  

## 🎯 **Table Relationships**

### **Data Flow Relationships**
```
bt_footfall_tfl_hex_3hourly
    ├─ JOIN hex_highstreet_lookup → bt_highstreets_3hourly_counts
    ├─ JOIN hex_towncentre_lookup → bt_towncentres_3hourly_counts  
    ├─ JOIN hex_bid_lookup → bt_bids_3hourly_counts
    └─ JOIN hex_bespoke_lookup → bt_bespokes_3hourly_counts
                                        ↓
                            bt_3hourly_counts (combined view)

econ_busyness_mrli_3hourly_adj
    ├─ JOIN mcard_Highstreets_quad_lookup → mcard_highstreets_3hourly_txn
    ├─ JOIN mcard_TownCentres_quad_lookup → mcard_towncentres_3hourly_txn
    ├─ JOIN mcard_BIDs_quad_lookup → mcard_bids_3hourly_txn
    └─ JOIN mcard_bespoke_quad_lookup → mcard_bespokes_3hourly_txn
                                        ↓
                            mcard_3hourly_txn (combined view)
```

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

- **[BT Footfall Tables](04.1-bt-tables.md)**: Detailed BT table specifications
- **[Mastercard Transaction Tables](04.2-mastercard-tables.md)**: Detailed Mastercard table specifications  
- **[Geographic Lookup Tables](04.3-lookup-tables.md)**: Spatial relationship tables
- **[Data Relationships](04.4-data-relationships.md)**: Join patterns and relationships

---

**Schema Summary**:
- **Total Tables**: 50+ tables across raw, aggregated, lookup, and reference data
- **Primary Storage**: PostgreSQL with monthly partitioning
- **Data Volume**: 63M+ records annually across all tables
- **Performance**: Optimized for analytical queries with appropriate indexing

**Next Steps**: Explore [BT Footfall Tables](04.1-bt-tables.md) for detailed specifications 