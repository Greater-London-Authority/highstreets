---
title: "Database Schema & Tables"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["database", "schema", "postgresql", "tables"]
---

# Database Schema & Tables

## 🗄️ Overview

The Highstreets Data Platform uses PostgreSQL with monthly partitioning on large time-series tables. Data is organized as raw, staging, lookup, aggregated, consolidated, and reference tables to support BT footfall and Mastercard transaction workflows.

## 📊 Schema Architecture

*[📋 Interactive Miro Board -  BT data schema ](https://miro.com/app/board/uXjVKeHa9kQ=/?moveToWidget=3458764637428028345&cot=14)*

*[📋 Interactive Miro Board -  Mastercard data schema ](https://miro.com/app/board/uXjVKeHa9kQ=/?moveToWidget=3458764637419889095&cot=14)*

**Note:** Visual diagram available above. The interactive board provides complete technical workflow details.

### Database Structure

#### Raw Data Tables
- [`bt_footfall_tfl_hex_3hourly`](#bt_footfall_tfl_hex_3hourly) – BT hex 3-hourly footfall
- [`bt_footfall_msoa_hourly`](#bt_footfall_msoa_hourly) – BT MSOA hourly footfall
- [`bt_footfall_lsoa_hourly`](#bt_footfall_lsoa_hourly) – BT LSOA hourly footfall
- [`econ_busyness_bt_daily_agg_cust_raw`](#econ_busyness_bt_daily_agg_cust_raw) – BT daily aggregated customer shapes
- [`econ_busyness_bt_outage_data`](#econ_busyness_bt_outage_data) – BT outage history
- [`econ_busyness_mrli_3hourly`](#econ_busyness_mrli_3hourly) – Mastercard raw 3-hourly quads
- [`econ_busyness_mrli_3hourly_adj`](#econ_busyness_mrli_3hourly_adj) – Mastercard CPI-adjusted 3-hourly quads
- [`econ_busyness_mcard_raw_18_zoom`](#econ_busyness_mcard_raw_18_zoom) – Mastercard raw weekly staging (zoom 18)

#### Staging & Processing Tables
- [`econ_busyness_mcard_clean_18_zoom`](#econ_busyness_mcard_clean_18_zoom) – Cleaned weekly data with complete records
- [`econ_busyness_mcard_stg_18_zoom`](#econ_busyness_mcard_stg_18_zoom) – Staging table for incremental processing
- [`econ_busyness_mcard_inner_outer_txn_pre_adj`](#econ_busyness_mcard_inner_outer_txn_pre_adj) – Pre-adjustment inner/outer weekly aggregation

#### Geographic Lookup Tables
- [`econ_busyness_hex_highstreet_lookup`](#econ_busyness_hex_highstreet_lookup) – Hex → Highstreet mapping
- [`econ_busyness_hex_towncentre_lookup`](#econ_busyness_hex_towncentre_lookup) – Hex → Town Centre mapping
- [`econ_busyness_hex_bid_lookup`](#econ_busyness_hex_bid_lookup) – Hex → BID mapping
- [`econ_busyness_hex_bespoke_lookup`](#econ_busyness_hex_bespoke_lookup) – Hex → Bespoke Area mapping
- [`econ_busyness_mcard_Highstreets_quad_lookup`](#econ_busyness_mcard_highstreets_quad_lookup) – Quad → Highstreet mapping
- [`econ_busyness_mcard_TownCentres_quad_lookup`](#econ_busyness_mcard_towncentres_quad_lookup) – Quad → Town Centre mapping
- [`econ_busyness_mcard_BIDs_quad_lookup`](#econ_busyness_mcard_bids_quad_lookup) – Quad → BID mapping
- [`econ_busyness_mcard_bespoke_quad_lookup`](#econ_busyness_mcard_bespoke_quad_lookup) – Quad → Bespoke Area mapping
- [`econ_busyness_mcard_inner_outer_quad_lookup`](#econ_busyness_mcard_inner_outer_quad_lookup) – Quad → Inner/Outer London mapping
- [`econ_busyness_mcard_towncentre_caz_lookup`](#econ_busyness_mcard_towncentre_caz_lookup) – Town Centre → CAZ mapping
- [`econ_busyness_mcard_boroughs_quad_lookup`](#econ_busyness_mcard_boroughs_quad_lookup) – Quad → Borough mapping
- [`econ_busyness_mcard_msoas_quad_lookup`](#econ_busyness_mcard_msoas_quad_lookup) – Quad → MSOA mapping
- [`econ_busyness_mcard_caz_quad_lookup`](#econ_busyness_mcard_caz_quad_lookup) – Quad → CAZ mapping
- [`econ_busyness_bt_uid_jan25_lookup`](#econ_busyness_bt_uid_jan25_lookup) – POI UID→ID mapping for BT daily API

#### BT Aggregated (3-hourly) Tables
- [`econ_busyness_bt_highstreets_3hourly_counts`](#econ_busyness_bt_highstreets_3hourly_counts) – BT Highstreet 3-hourly aggregation
- [`econ_busyness_bt_towncentres_3hourly_counts`](#econ_busyness_bt_towncentres_3hourly_counts) – BT Town Centre 3-hourly aggregation
- [`econ_busyness_bt_bids_3hourly_counts`](#econ_busyness_bt_bids_3hourly_counts) – BT BID 3-hourly aggregation
- [`econ_busyness_bt_bespokes_3hourly_counts`](#econ_busyness_bt_bespokes_3hourly_counts) – BT Bespoke 3-hourly aggregation
- Consolidated: [`econ_busyness_bt_3hourly_counts`](#econ_busyness_bt_3hourly_counts) – All BT boundary layers combined

#### Mastercard 3-hourly Aggregated Tables
- [`econ_busyness_mcard_highstreets_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) – Mastercard Highstreet 3-hourly transactions
- [`econ_busyness_mcard_towncentres_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) – Mastercard Town Centre 3-hourly transactions
- [`econ_busyness_mcard_bids_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) – Mastercard BID 3-hourly transactions
- [`econ_busyness_mcard_bespokes_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) – Mastercard Bespoke 3-hourly transactions
- Consolidated: [`econ_busyness_mcard_3hourly_txn`](#econ_busyness_mcard_3hourly_txn) – All Mastercard 3-hourly boundary layers combined

#### Mastercard Weekly Aggregated Tables
- [`econ_busyness_mcard_highstreets_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard Highstreet weekly transactions
- [`econ_busyness_mcard_towncentres_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard Town Centre weekly transactions
- [`econ_busyness_mcard_bids_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard BID weekly transactions
- [`econ_busyness_mcard_bespoke_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard Bespoke weekly transactions
- [`econ_busyness_mcard_boroughs_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard Borough weekly transactions
- [`econ_busyness_mcard_caz_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard CAZ weekly transactions
- [`econ_busyness_mcard_msoas_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard MSOA weekly transactions
- [`econ_busyness_mcard_inner_outer_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard Inner/Outer London weekly transactions
- [`econ_busyness_mcard_london_txn`](#econ_busyness_mcard_txn-weekly) – Mastercard London-wide weekly transactions
- Consolidated: [`econ_busyness_mcard_txn`](#econ_busyness_mcard_txn-weekly) – All Mastercard weekly boundary layers combined

#### Mastercard Weekly YoY Tables
- [`econ_busyness_mcard_highstreets_yoy`](#econ_busyness_mcard_yoy) – Mastercard Highstreet YoY growth
- [`econ_busyness_mcard_towncentres_yoy`](#econ_busyness_mcard_yoy) – Mastercard Town Centre YoY growth
- [`econ_busyness_mcard_bids_yoy`](#econ_busyness_mcard_yoy) – Mastercard BID YoY growth
- [`econ_busyness_mcard_bespoke_yoy`](#econ_busyness_mcard_yoy) – Mastercard Bespoke YoY growth
- [`econ_busyness_mcard_boroughs_yoy`](#econ_busyness_mcard_yoy) – Mastercard Borough YoY growth
- [`econ_busyness_mcard_caz_yoy`](#econ_busyness_mcard_yoy) – Mastercard CAZ YoY growth
- [`econ_busyness_mcard_msoas_yoy`](#econ_busyness_mcard_yoy) – Mastercard MSOA YoY growth
- [`econ_busyness_mcard_inner_outer_yoy`](#econ_busyness_mcard_yoy) – Mastercard Inner/Outer London YoY growth
- [`econ_busyness_mcard_london_yoy`](#econ_busyness_mcard_yoy) – Mastercard London-wide YoY growth
- Consolidated: [`econ_busyness_mcard_yoy`](#econ_busyness_mcard_yoy) – All Mastercard weekly YoY layers combined

#### LDC Premises Tables
- [`ldc_premises_raw`](#ldc_premises_raw) – Accumulated LDC premises data (57 columns, all sources)
- [`ldc_premises_clean`](#ldc_premises_clean) – Transformed analyst-facing output (38 columns)
- [`ldc_premises_staging`](#ldc_premises_staging) – Temporary UNLOGGED table for upsert operations

#### Reference & Context Tables
- [`econ_busyness_mcard_adjustment_factors`](#econ_busyness_mcard_adjustment_factors) – Monthly cash-to-card and market share adjustment factors
- [`econ_busyness_mcard_cpi_data`](#econ_busyness_mcard_cpi_data) – CPIH inflation data from ONS API
- [`econ_busyness_borough_hs_lookup_3`](#econ_busyness_borough_hs_lookup_3) – Borough and Highstreet spatial relationships
- [`hsds_bid_hs_tc`](#hsds_bid_hs_tc) – Combined geometry reference for BIDs/Highstreets/TownCentres

### Key Design Principles
- **Temporal partitioning** on large date-series tables for performance
- **Multi-stage processing pipeline** for Mastercard weekly data (raw → staging → clean → aggregated)
- **Lookup-driven spatial aggregation** (hex/quad → boundary)
- **Consolidated union tables** for simplified analytics and cross-boundary analysis
- **Consistent identifiers and time formatting** across all data sources
- **CPI adjustment and market correction** for economic analysis

---

## 🏗️ Raw Data Tables

### bt_footfall_tfl_hex_3hourly
BT Footfall data at hex grid level with 3-hourly temporal resolution

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| hex_id | INTEGER | NOT NULL | TfL hex ID |
| count_date | DATE | NOT NULL | Measurement date |
| day | VARCHAR(10) | NOT NULL | Mon-Sun |
| time_indicator | VARCHAR(5) | NOT NULL | 3-hour slot (e.g. 09-12) |
| resident | INTEGER | NULL | Estimated residents |
| visitor | INTEGER | NULL | Estimated visitors |
| worker | INTEGER | NULL | Estimated workers |
| loyalty_percentage | FLOAT(10,2) | NULL | Avg loyalty (%) |
| dwell_time | FLOAT(10,2) | NULL | Avg dwell time (mins) |

Primary Key: (hex_id, count_date, time_indicator)

### bt_footfall_msoa_hourly
BT Footfall data at MSOA (Middle Super Output Area) level with hourly resolution

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| msoa_id | TEXT | NOT NULL | MSOA code |
| msoa_name | TEXT | NOT NULL | MSOA name |
| count_date | DATE | NOT NULL | Date |
| day | TEXT | NOT NULL | Mon-Sun |
| hour | BIGINT | NOT NULL | Hour of day (0–23) |
| resident | BIGINT | NULL | Residents |
| visitor | BIGINT | NULL | Visitors |
| worker | BIGINT | NULL | Workers |
| loyalty_percentage | FLOAT(10,2) | NULL | Loyalty |
| dwell_time | FLOAT(10,2) | NULL | Dwell time |

Primary Key: (msoa_id, count_date, hour)

### bt_footfall_lsoa_hourly
BT Footfall data at LSOA (Lower Super Output Area) level with hourly resolution

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| lsoa_id | TEXT | NOT NULL | LSOA code |
| lsoa_name | TEXT | NULL | LSOA name |
| day | TEXT | NULL | Mon-Sun |
| count_date | DATE | NOT NULL | Date |
| hour | INTEGER | NOT NULL | Hour of day (0–23) |
| resident | BIGINT | NULL | Residents |
| visitor | BIGINT | NULL | Visitors |
| worker | BIGINT | NULL | Workers |
| loyalty_percentage | FLOAT(10,2) | NULL | Loyalty |
| dwell_time | FLOAT(10,2) | NULL | Dwell time |

Primary Key: (lsoa_id, count_date, hour)

### econ_busyness_bt_daily_agg_cust_raw
BT Daily Aggregated Customer Shapes (API output post-lookup)

| Column | Type | Notes |
|--------|------|-------|
| poi_id | VARCHAR(9) | Mapped from `poi_uid` via UID lookup |
| poi_uid | VARCHAR(14) | Source UID from BT API |
| poi_name | TEXT | Name |
| poi_type | TEXT | bids/highstreet/towncentre/custom/major parks/gla boundary/borough |
| count_date | DATE | Date |
| time_indicator | TEXT | DAY/AM/PM |
| total_unique_volume | BIGINT | Total |
| total_unique_intl_only_visitors | BIGINT | Intl |
| total_unique_domestic_visitors | BIGINT | Domestic |
| total_unique_workers | BIGINT | Workers |
| total_unique_residents | BIGINT | Residents |
| avg_dwell_time | BIGINT | Minutes |

### econ_busyness_bt_outage_data
Outage history for data quality assurance

| Column | Type | Notes |
|--------|------|-------|
| count_date | DATE | Date |
| lad_name | VARCHAR(255) | borough name |
| region | VARCHAR(255) | Region (filtered to London) |
| outage_flag | VARCHAR(50) | Red/Green/Amber |

### econ_busyness_mrli_3hourly
Mastercard raw 3-hourly quad data (pre-adjustment)

| Column | Type | Notes |
|--------|------|-------|
| ldn_ref | BIGINT | refernce created for easy use of quads |
| quad_id | BIGINT | Quad ID |
| count_date | DATE | Date |
| hours | TEXT | 3-hour slot(00-03/03-06/..) |
| txn_amt | DECIMAL | Raw spend |
| txn_cnt | DECIMAL | Txn count |
| avg_spend_amt | DECIMAL | Avg ticket |

PK: (quad_id, count_date, hours)

### econ_busyness_mrli_3hourly_adj
CPI-adjusted 3-hourly Mastercard data

| Column | Type | Notes |
|--------|------|-------|
| ldn_ref | BIGINT | refernce created for easy use of quads |
| quad_id | BIGINT | Quad ID |
| count_date | DATE | Date |
| hours | TEXT | 3-hour slot(00-03/03-06/..) |
| txn_amt | DECIMAL | Raw spend |
| txn_amt_adj | DECIMAL | CPI-adjusted spend |
| txn_cnt | DECIMAL | Txn count |

PK: (quad_id, count_date, hours)

### econ_busyness_mcard_raw_18_zoom
Mastercard raw weekly data staging table (zoom level 18)

| Column | Type | Notes |
|--------|------|-------|
| yr | FLOAT | ISO calendar year |
| wk | FLOAT | ISO week number |
| industry | TEXT | Industry sector |
| segment | TEXT | Overall/International/.. |
| geo_type | TEXT | State/Country/Msa |
| geo_name | TEXT | Geographic area (London/United Kingdom/Outer London etc) |
| quad_id | TEXT | Quad identifier |
| txn_amt | FLOAT | Transaction amount |
| txn_cnt | FLOAT | Transaction count |
| acct_cnt | FLOAT | Account count |
| avg_ticket | FLOAT | Average ticket size |
| avg_freq | FLOAT | Average frequency |
| avg_spend_amt | FLOAT | Average spend amount |
| yoy_txn_amt | TEXT | YoY spend amount |
| yoy_txn_cnt | TEXT | YoY spend count |
| weekday_weekend | TEXT | weekdays/weekends |
| central_latitude | FLOAT | Quad center latitude |
| central_longitude | FLOAT | Quad center longitude |
| bounding_box | TEXT | location info |
| file_name | TEXT | Source file name |

---

## 🔄 Staging & Processing Tables

### econ_busyness_mcard_clean_18_zoom
Cleaned and complete Mastercard weekly data

| Column | Type | Notes |
|--------|------|-------|
| yr | FLOAT | ISO calendar year |
| wk | FLOAT | ISO week number |
| industry | TEXT | Total Retail/Total Apparel/Eating Places |
| quad_id | TEXT | Quad identifier |
| weekday_weekend | TEXT | weekdays/weekends |
| txn_amt | FLOAT | Transaction amount |
| txn_cnt | FLOAT | Transaction count |
| acct_cnt | FLOAT | account count |
| avg_ticket | FLOAT | average ticket size |
| avg_freq | FLOAT | average frequency|
| week_start | DATE | Monday of the ISO week |
| central_latitude | DATE | Monday of the ISO week |
| central_longitude | DATE | Monday of the ISO week |

### econ_busyness_mcard_stg_18_zoom
Staging table for incremental Mastercard weekly processing

| Column | Type | Notes |
|--------|------|-------|
| yr | FLOAT | ISO calendar year |
| wk | FLOAT | ISO week number |
| industry | TEXT | Industry sector |
| segment | TEXT | Overall/International |
| geo_name | TEXT | London |
| quad_id | TEXT | Quad identifier |
| txn_amt | FLOAT | Transaction amount |
| txn_cnt | FLOAT | Transaction count |
| weekday_weekend | TEXT | weekdays/weekends |

### econ_busyness_mcard_inner_outer_txn_pre_adj
Pre-adjustment inner/outer London weekly transaction aggregation

| Column | Type | Notes |
|--------|------|-------|
| week_start | TIMESTAMP | Monday of the ISO week |
| inner_outer | TEXT | Inner/Outer London designation |
| month | INTEGER | 1-12 |
| yr | INTEGER | ISO calendar year |
| txn_amt_wd_retail | DECIMAL | Weekday retail spend |
| txn_amt_we_retail | DECIMAL | Weekend retail spend |
| txn_amt_wd_eating | DECIMAL | Weekday eating spend |
| txn_amt_we_eating | DECIMAL | Weekend eating spend |
| txn_amt_wd_apparel | DECIMAL | Weekday apparel spend |
| txn_amt_we_apparel | DECIMAL | Weekend apparel spend |

---

## 🗺️ Geographic Lookup Tables

### econ_busyness_hex_highstreet_lookup
Maps BT hex grids to Highstreet boundaries
| hex_id | highstreet_id | highstreet_name |

### econ_busyness_hex_towncentre_lookup
Maps BT hex grids to Town Centre boundaries
| hex_id | tc_id | tc_name |

### econ_busyness_hex_bid_lookup
Maps BT hex grids to Business Improvement District boundaries
| hex_id | bid_id | bid_name |

### econ_busyness_hex_bespoke_lookup
Maps BT hex grids to custom geographic areas
| hex_id | bespoke_area_id | name |

### econ_busyness_mcard_Highstreets_quad_lookup
Maps Mastercard quads to Highstreet boundaries with coordinate information
| quad_id | highstreet_id | highstreet_name | x | y | borough |

### econ_busyness_mcard_TownCentres_quad_lookup
Maps Mastercard quads to Town Centre boundaries with coordinate information
| quad_id | tc_id | tc_name | x | y | borough |

### econ_busyness_mcard_BIDs_quad_lookup
Maps Mastercard quads to BID boundaries
| quad_id | bid_id | bid_name |

### econ_busyness_mcard_bespoke_quad_lookup
Maps Mastercard quads to custom geographic areas
| quad_id | bespoke_area_id | name | x | y | gss_code | borough |

### econ_busyness_mcard_inner_outer_quad_lookup
Classifies Mastercard quads as Inner or Outer London
| quad_id | inner_outer |

### econ_busyness_mcard_towncentre_caz_lookup
Maps Town Centres to Central Activities Zone designation
| tc_id | tc_name |

### econ_busyness_mcard_boroughs_quad_lookup
Maps Mastercard quads to London Borough boundaries
| quad_id | gss_code | name |

### econ_busyness_mcard_msoas_quad_lookup
Maps Mastercard quads to MSOA boundaries
| quad_id | msoa11cd | msoa11nm |

### econ_busyness_mcard_caz_quad_lookup
Maps Mastercard quads to Central Activities Zone
| quad_id | objectid | name |

### econ_busyness_bt_uid_jan25_lookup
POI UID→ID mapping used in BT daily aggregated customer shapes transform
| uid | id | layer | name | name_old | change_name | change_shape | new_name_shape | del_name_shape | bt_changed_2024

---

## 📈 BT Aggregated (3-hourly) Tables

### econ_busyness_bt_highstreets_3hourly_counts
Aggregated BT footfall by Highstreet boundaries
Columns: highstreet_id, highstreet_name, count_date, hours, x, y, borough, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_towncentres_3hourly_counts
Aggregated BT footfall by Town Centre boundaries
Columns: tc_id, tc_name, count_date, hours, x, y, borough, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_bids_3hourly_counts
Aggregated BT footfall by BID boundaries
Columns: bid_id, bid_name, count_date, hours, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_bespokes_3hourly_counts
Aggregated BT footfall by bespoke area boundaries
Columns: bespoke_area_id, name, count_date, hours, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_3hourly_counts
Consolidated union across all BT boundary layers
Columns: count_date, hours, id, name, layer, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

---

## 💳 Mastercard Aggregated Tables

### Mastercard 3-hourly Tables
**Tables**: econ_busyness_mcard_highstreets_3hourly_txn, econ_busyness_mcard_towncentres_3hourly_txn, econ_busyness_mcard_bids_3hourly_txn, econ_busyness_mcard_bespokes_3hourly_txn
Columns: boundary_id, boundary_name, count_date, hours, txn_amt, txn_cnt, [x, y, borough where applicable]

### econ_busyness_mcard_3hourly_txn
Consolidated union across all 3-hourly boundary layers
Columns: count_date, hours, id, name, layer, txn_amt, txn_cnt

### Mastercard Weekly Tables
**Individual Layer Tables**: econ_busyness_mcard_*_txn (highstreets, towncentres, bids, bespoke, boroughs, caz, msoas, inner_outer, london)
Typical columns by layer:
- boundary_id, boundary_name, week_start, yr, wk
- txn_amt_wd_retail, txn_amt_we_retail, txn_amt_wd_eating, txn_amt_we_eating, txn_amt_wd_apparel, txn_amt_we_apparel
- txn_cnt_wd_retail, txn_cnt_we_retail, txn_cnt_wd_eating, txn_cnt_we_eating, txn_cnt_wd_apparel, txn_cnt_we_apparel
- CPI-adjusted versions: *_adj columns
- Additional layer-specific fields (x, y, borough for spatial layers)

### econ_busyness_mcard_txn
Consolidated weekly transactions across all boundary layers
Columns: week_start, yr, wk, id, name, layer, [all txn_amt and txn_cnt columns by sector and weekend/weekday]

### Mastercard Weekly YoY Tables
**Individual Layer Tables**: econ_busyness_mcard_*_yoy (highstreets, towncentres, bids, bespoke, boroughs, caz, msoas, inner_outer, london)
Contains YoY growth metrics: boundary_id, boundary_name, week_start, yr, wk, and yoy_* metrics for all transaction columns

### econ_busyness_mcard_yoy
Consolidated YoY growth across all boundary layers
Columns: week_start, yr, wk, id, name, layer, [all yoy_* columns by sector and weekend/weekday]

---

## 📋 Reference & Context Tables

### econ_busyness_mcard_adjustment_factors
Monthly market-share and cash-to-card correction factors used for weekly processing

| Column | Type | Description |
|--------|------|-------------|
| inner_outer | TEXT | Inner/Outer London |
| date | Timestamp | First day of month |
| yr | INTEGER | Year |
| month | INTEGER | Month |
| adjustment_factor_retail | DECIMAL | Retail sector adjustment |
| adjustment_factor_apparel | DECIMAL | Apparel sector adjustment |
| adjustment_factor_eating | DECIMAL | Eating sector adjustment |

### econ_busyness_mcard_cpi_data
CPIH (Consumer Price Index including Housing) data from ONS API

| Column | Type | Description |
|--------|------|-------------|
| yr | INTEGER | Year |
| month | INTEGER | Month |
| aggregate | VARCHAR(150) | CPI category |
| cpi_index | DECIMAL(15,6) | CPI index value |

### econ_busyness_borough_hs_lookup_3
Borough and Highstreet spatial relationships for lookup refresh

### hsds_bid_hs_tc
Combined geometry reference of BIDs/Highstreets/TownCentres used by lookup refresh processes

---

## 🎯 Data Flow & Table Relationships

### BT (3-hourly) Flow
```
bt_footfall_tfl_hex_3hourly
├── JOIN econ_busyness_hex_highstreet_lookup → econ_busyness_bt_highstreets_3hourly_counts
├── JOIN econ_busyness_hex_towncentre_lookup → econ_busyness_bt_towncentres_3hourly_counts
├── JOIN econ_busyness_hex_bid_lookup → econ_busyness_bt_bids_3hourly_counts
└── JOIN econ_busyness_hex_bespoke_lookup → econ_busyness_bt_bespokes_3hourly_counts
    └── UNION ALL → econ_busyness_bt_3hourly_counts
```

### BT Daily Flow
```
BT Daily API → raw_bt_daily_preprocess_data() 
├── JOIN econ_busyness_bt_uid_jan25_lookup (poi_uid → poi_id)
└── econ_busyness_bt_daily_agg_cust_raw
```

### Mastercard 3-hourly Flow
```
econ_busyness_mrli_3hourly → CPI adjustment → econ_busyness_mrli_3hourly_adj
├── JOIN econ_busyness_mcard_Highstreets_quad_lookup → econ_busyness_mcard_highstreets_3hourly_txn
├── JOIN econ_busyness_mcard_TownCentres_quad_lookup → econ_busyness_mcard_towncentres_3hourly_txn
├── JOIN econ_busyness_mcard_BIDs_quad_lookup → econ_busyness_mcard_bids_3hourly_txn
└── JOIN econ_busyness_mcard_bespoke_quad_lookup → econ_busyness_mcard_bespokes_3hourly_txn
    └── UNION ALL → econ_busyness_mcard_3hourly_txn
```

### Mastercard Weekly Flow
```
Raw Weekly Files (*.csv) → econ_busyness_mcard_raw_18_zoom
└── Clean & Complete → econ_busyness_mcard_clean_18_zoom
    └── Incremental Staging → econ_busyness_mcard_stg_18_zoom
        └── Inner/Outer Aggregation → econ_busyness_mcard_inner_outer_txn_pre_adj
            ├── Generate Adjustment Factors → econ_busyness_mcard_adjustment_factors
            └── Weekly Boundary Aggregations:
                ├── econ_busyness_mcard_highstreets_txn
                ├── econ_busyness_mcard_towncentres_txn
                ├── econ_busyness_mcard_bids_txn
                ├── econ_busyness_mcard_bespoke_txn
                ├── econ_busyness_mcard_boroughs_txn
                ├── econ_busyness_mcard_caz_txn
                ├── econ_busyness_mcard_msoas_txn
                ├── econ_busyness_mcard_inner_outer_txn
                └── econ_busyness_mcard_london_txn
                    └── UNION ALL → econ_busyness_mcard_txn
                        └── YoY Growth Calculation → econ_busyness_mcard_*_yoy
                            └── UNION ALL → econ_busyness_mcard_yoy
```

### Key Processing Steps
1. **Raw Data Ingestion**: CSV files → raw tables with file metadata
2. **Data Cleaning**: Complete missing combinations, filter specific industries
3. **Incremental Processing**: Only process new data since last run
4. **Spatial Aggregation**: Quad/hex → boundary via lookup tables
5. **Market Adjustment**: Apply cash-to-card and market share corrections
6. **CPI Adjustment**: Apply sector-specific inflation adjustments
7. **Consolidation**: UNION boundary layers for cross-boundary analysis
8. **YoY Calculation**: Generate year-over-year growth metrics

---

---

## 🏪 LDC Premises Tables

### ldc_premises_raw
Accumulated LDC premises business history (all sources: live, historic, archived)

| Column Group | Key Columns | Description |
|-------------|-------------|-------------|
| **Primary Key** | `tenant_id`, `premises_id`, `date_create` | Composite PK for upsert |
| **Core Business** | `tenant`, `tenant_status`, `category`, `classification`, `subcategory` | Business identity and classification |
| **Location** | `address`, `street`, `city`, `zip`, `geography`, `latitude`, `longitude`, `uprn_id` | Physical location |
| **Company** | `company`, `company_id`, `company_holding`, `tenant_care_of`, `flag_independent` | Operator and ownership |
| **Dates** | `date_close`, `date_premises_create`, `timestamp_update` | Lifecycle dates |
| **Metadata** | `source`, `row_hash`, `ingested_at` | Pipeline tracking |

PK: (tenant_id, premises_id, date_create)
Full column reference: [LDC Data Dictionary](05.3-ldc-data-dictionary.md)

### ldc_premises_clean
Transformed 38-column analyst-facing output

| Column Group | Key Columns | Description |
|-------------|-------------|-------------|
| **Surrogate PK** | `id` (BIGSERIAL) | Auto-increment; accommodates rare SWS duplicates |
| **Identity** | `tenant_id`, `tenant`, `premises_id` | Business and location |
| **Location** | `address`, `geography`, `geography_large`, `latitude`, `longitude` | Spatial fields |
| **Classification** | `category`, `classification`, `subcategory` | Business type hierarchy |
| **Computed** | `latest_record_check`, `latest_premises_check` | Survey recency (computed during transform) |

PK: id (BIGSERIAL)
Full column reference: [LDC Data Dictionary](05.3-ldc-data-dictionary.md)

### ldc_premises_staging
Temporary UNLOGGED table for upsert operations. Schema mirrors `ldc_premises_raw`. Truncated before each use.

### LDC Data Flow
```
Snowflake → ldc_premises_staging (TRUNCATE + append)
         → ldc_premises_raw (INSERT ON CONFLICT with hash check)
         → Transform (7-step pipeline)
         → ldc_premises_clean (TRUNCATE + full reload)
         → S3 Parquet archives
```

---

## 🔗 Related Information

- [BT Footfall Data Flow](03.1-bt-data-flow.md)
- [Mastercard Transaction Data Flow](03.2-mastercard-data-flow.md)
- [LDC Premises Data Source](05-ldc-premises-data-source.md)
- [LDC Data Dictionary](05.3-ldc-data-dictionary.md)
- [Sublicensing & Data Distribution](07-sublicensing.md)

---