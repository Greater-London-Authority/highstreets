---
title: "Database Schema & Tables"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["database", "schema", "postgresql", "tables"]
---

# Database Schema & Tables

## 🗄️ Overview

The Highstreets Data Platform uses PostgreSQL with monthly partitioning on large time-series tables. Data is organized as raw, lookup, aggregated, consolidated, and reference tables to support BT footfall and Mastercard transaction workflows.

## 📊 Schema Architecture

### Database Structure

#### Raw Data Tables
- [`bt_footfall_tfl_hex_3hourly`](#bt_footfall_tfl_hex_3hourly) – BT hex 3-hourly
- [`bt_footfall_msoa_hourly`](#bt_footfall_msoa_hourly) – BT MSOA hourly
- [`bt_footfall_lsoa_hourly`](#bt_footfall_lsoa_hourly) – BT LSOA hourly
- [`econ_busyness_bt_daily_agg_cust_raw`](#econ_busyness_bt_daily_agg_cust_raw) – BT daily aggregated customer shapes
- [`econ_busyness_bt_outage_data`](#econ_busyness_bt_outage_data) – BT outage history
- [`econ_busyness_mrli_3hourly`](#econ_busyness_mrli_3hourly) – Mastercard raw 3-hourly quads
- [`econ_busyness_mrli_3hourly_adj`](#econ_busyness_mrli_3hourly_adj) – Mastercard adjusted 3-hourly quads

#### Geographic Lookup Tables
- [`econ_busyness_hex_highstreet_lookup`](#econ_busyness_hex_highstreet_lookup)
- [`econ_busyness_hex_towncentre_lookup`](#econ_busyness_hex_towncentre_lookup)
- [`econ_busyness_hex_bid_lookup`](#econ_busyness_hex_bid_lookup)
- [`econ_busyness_hex_bespoke_lookup`](#econ_busyness_hex_bespoke_lookup)
- Mastercard quad lookups:
  - [`econ_busyness_mcard_Highstreets_quad_lookup`](#econ_busyness_mcard_highstreets_quad_lookup)
  - [`econ_busyness_mcard_TownCentres_quad_lookup`](#econ_busyness_mcard_towncentres_quad_lookup)
  - [`econ_busyness_mcard_BIDs_quad_lookup`](#econ_busyness_mcard_bids_quad_lookup)
  - [`econ_busyness_mcard_bespoke_quad_lookup`](#econ_busyness_mcard_bespoke_quad_lookup)
  - [`econ_busyness_mcard_Inner_Outer_quad_lookup`](#econ_busyness_mcard_inner_outer_quad_lookup)
  - [`econ_busyness_mcard_towncentre_caz_lookup`](#econ_busyness_mcard_towncentre_caz_lookup)
- [`econ_busyness_bt_uid_jan25_lookup`](#econ_busyness_bt_uid_jan25_lookup) – POI UID→ID mapping for BT daily API

#### BT Aggregated (3-hourly) Tables
- [`econ_busyness_bt_highstreets_3hourly_counts`](#econ_busyness_bt_highstreets_3hourly_counts)
- [`econ_busyness_bt_towncentres_3hourly_counts`](#econ_busyness_bt_towncentres_3hourly_counts)
- [`econ_busyness_bt_bids_3hourly_counts`](#econ_busyness_bt_bids_3hourly_counts)
- [`econ_busyness_bt_bespokes_3hourly_counts`](#econ_busyness_bt_bespokes_3hourly_counts)
- Consolidated: [`econ_busyness_bt_3hourly_counts`](#econ_busyness_bt_3hourly_counts)

#### Mastercard Aggregated
- 3-hourly by boundary:
  - [`econ_busyness_mcard_highstreets_3hourly_txn`](#econ_busyness_mcard_3hourly_txn)
  - [`econ_busyness_mcard_towncentres_3hourly_txn`](#econ_busyness_mcard_3hourly_txn)
  - [`econ_busyness_mcard_bids_3hourly_txn`](#econ_busyness_mcard_3hourly_txn)
  - [`econ_busyness_mcard_bespokes_3hourly_txn`](#econ_busyness_mcard_3hourly_txn)
  - Consolidated: [`econ_busyness_mcard_3hourly_txn`](#econ_busyness_mcard_3hourly_txn)
- Weekly by boundary:
  - [`econ_busyness_mcard_highstreets_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_towncentres_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_bids_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_bespoke_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_boroughs_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_caz_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_msoas_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_inner_outer_txn`](#econ_busyness_mcard_txn-weekly)
  - [`econ_busyness_mcard_london_txn`](#econ_busyness_mcard_txn-weekly)
  - Consolidated: [`econ_busyness_mcard_txn`](#econ_busyness_mcard_txn-weekly)
- Weekly YoY (per boundary) + consolidated:
  - [`econ_busyness_mcard_*_yoy`](#econ_busyness_mcard_yoy)

#### Reference & Context
- [`econ_busyness_mcard_adjustment_factors`](#econ_busyness_mcard_adjustment_factors)
- [`econ_busyness_mcard_cpi_data`](#econ_busyness_mcard_cpi_data)
- [`hsds_bid_hs_tc`](#hsds_bid_hs_tc) – Combined geometry reference for BIDs/Highstreets/TownCentres

### Key Design Principles
- Temporal partitioning on large date-series tables
- Lookup-driven spatial aggregation (hex/quad→boundary)
- Consolidated union tables for simplified analytics
- Consistent identifiers and hours formatting

---

## 🏗️ Raw Data Tables

### bt_footfall_tfl_hex_3hourly
| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| hex_id | VARCHAR(20) | NOT NULL | TfL hex ID |
| count_date | DATE | NOT NULL | Measurement date |
| hours | VARCHAR(5) | NOT NULL | 3-hour slot (e.g. 09-12) |
| resident | INTEGER | NULL | Estimated residents |
| visitor | INTEGER | NULL | Estimated visitors |
| worker | INTEGER | NULL | Estimated workers |
| loyalty_percentage | FLOAT | NULL | Avg loyalty (%) |
| dwell_time | FLOAT | NULL | Avg dwell time (mins) |

Primary Key: (hex_id, count_date, hours)

### bt_footfall_msoa_hourly
| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| msoa_id | TEXT | NOT NULL | MSOA code |
| count_date | DATE | NOT NULL | Date |
| hour | INTEGER | NOT NULL | Hour of day (0–23) |
| resident | INTEGER | NULL | Residents |
| visitor | INTEGER | NULL | Visitors |
| worker | INTEGER | NULL | Workers |
| loyalty_percentage | FLOAT | NULL | Loyalty |
| dwell_time | FLOAT | NULL | Dwell time |

Primary Key: (msoa_id, count_date, hour)

### bt_footfall_lsoa_hourly
| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| lsoa_id | TEXT | NOT NULL | LSOA code |
| lsoa_name | TEXT | NULL | LSOA name |
| count_date | DATE | NOT NULL | Date |
| hour | INTEGER | NOT NULL | Hour of day (0–23) |
| resident | INTEGER | NULL | Residents |
| visitor | INTEGER | NULL | Visitors |
| worker | INTEGER | NULL | Workers |
| loyalty_percentage | FLOAT | NULL | Loyalty |
| dwell_time | FLOAT | NULL | Dwell time |

Primary Key: (lsoa_id, count_date, hour)

### econ_busyness_bt_daily_agg_cust_raw
BT Daily Aggregated Customer Shapes (API output post-lookup)

| Column | Type | Notes |
|--------|------|-------|
| poi_id | TEXT | Mapped from `poi_uid` via UID lookup |
| poi_uid | TEXT | Source UID from BT API |
| poi_name | TEXT | Name |
| poi_type | TEXT | bids/highstreet/towncentre/bespoke |
| count_date | DATE | Date |
| time_indicator | TEXT | 3-hour period |
| total_unique_volume | INT | Total |
| total_unique_intl_only_visitors | INT | Intl |
| total_unique_domestic_visitors | INT | Domestic |
| total_unique_workers | INT | Workers |
| total_unique_residents | INT | Residents |
| avg_dwell_time | INT | Minutes |

### econ_busyness_bt_outage_data
Outage history for QA

| Column | Type | Notes |
|--------|------|-------|
| count_date | DATE | Date |
| region | TEXT | Region (filtered to London) |
| ... | ... | API-sourced fields |

### econ_busyness_mrli_3hourly
Mastercard raw 3-hourly (pre-adjustment)

| Column | Type | Notes |
|--------|------|-------|
| quad_id | BIGINT | Quad ID |
| count_date | DATE | Date |
| hours | VARCHAR(5) | 3-hour slot |
| txn_amt | DECIMAL | Raw spend |
| txn_cnt | INTEGER | Txn count |
| avg_spend_amt | DECIMAL | Avg ticket |

PK: (quad_id, count_date, hours)

### econ_busyness_mrli_3hourly_adj
Adjusted 3-hourly (market share + CPI)

| Column | Type | Notes |
|--------|------|-------|
| quad_id | BIGINT | Quad ID |
| count_date | DATE | Date |
| hours | VARCHAR(5) | Slot |
| txn_amt | DECIMAL | Raw spend |
| txn_amt_adj | DECIMAL | Adjusted spend |
| txn_cnt | INTEGER | Txn count |

PK: (quad_id, count_date, hours)

---

## 🗺️ Geographic Lookup Tables

### econ_busyness_hex_highstreet_lookup
| hex_id | highstreet_id | highstreet_name |

### econ_busyness_hex_towncentre_lookup
| hex_id | tc_id | tc_name |

### econ_busyness_hex_bid_lookup
| hex_id | bid_id | bid_name |

### econ_busyness_hex_bespoke_lookup
| hex_id | bespoke_area_id | name |

### econ_busyness_mcard_Highstreets_quad_lookup
| quad_id | highstreet_id | highstreet_name | x | y | borough |

### econ_busyness_mcard_TownCentres_quad_lookup
| quad_id | tc_id | tc_name | x | y | borough |

### econ_busyness_mcard_BIDs_quad_lookup
| quad_id | bid_id | bid_name |

### econ_busyness_mcard_bespoke_quad_lookup
| quad_id | bespoke_area_id | name |

### econ_busyness_mcard_Inner_Outer_quad_lookup
| quad_id | inner_outer |

### econ_busyness_mcard_towncentre_caz_lookup
| tc_id | tc_name | (in CAZ) |

### econ_busyness_bt_uid_jan25_lookup
POI UID→ID mapping used in BT daily transform
| uid | id | layer | name |

---

## 📈 BT Aggregated (3-hourly)

### econ_busyness_bt_highstreets_3hourly_counts
Columns: highstreet_id, highstreet_name, count_date, hours, x, y, borough, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_towncentres_3hourly_counts
Columns: tc_id, tc_name, count_date, hours, x, y, borough, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_bids_3hourly_counts
Columns: bid_id, bid_name, count_date, hours, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_bespokes_3hourly_counts
Columns: bespoke_area_id, name, count_date, hours, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

### econ_busyness_bt_3hourly_counts
Consolidated union across all BT boundary layers
Columns: count_date, hours, id, name, layer, resident, visitor, worker, ave_loyalty_percentage, ave_dwell_time

---

## 💳 Mastercard Aggregated

### econ_busyness_mcard_*_3hourly_txn
Tables: highstreets, towncentres, bids, bespokes
Columns: boundary_id, boundary_name, count_date, hours, txn_amt, txn_cnt, [x, y, borough where applicable]

### econ_busyness_mcard_3hourly_txn
Consolidated union across all 3-hourly boundary layers
Columns: count_date, hours, id, name, layer, txn_amt, txn_cnt

### econ_busyness_mcard_*_txn (Weekly) and econ_busyness_mcard_txn
Weekly aggregated by layer (plus consolidated `econ_busyness_mcard_txn`). Typical columns by layer:
- boundary_id, boundary_name, week_start, yr, wk,
- txn_amt_wd_retail, txn_amt_we_retail, txn_amt_wd_eating, txn_amt_we_eating, txn_amt_wd_apparel, txn_amt_we_apparel
- Additional layer-specific fields as generated by weekly processor

### econ_busyness_mcard_*_yoy and econ_busyness_mcard_yoy
YoY growth per layer and consolidated view. Columns include: boundary_id, boundary_name, week_start, yr, wk, and yoy_* metrics.

---

## 📋 Reference & Context

### econ_busyness_mcard_adjustment_factors
Monthly market-share/cash-to-card correction factors used for weekly processing.

### econ_busyness_mcard_cpi_data
CPIH data pulled from ONS API and stored for reference.

### hsds_bid_hs_tc
Combined geometry reference of BIDs/Highstreets/TownCentres used by lookup refresh.

---

## 🎯 Table Relationships (textual)

BT (3-hourly):
- `bt_footfall_tfl_hex_3hourly` JOINs:
  - `econ_busyness_hex_highstreet_lookup` → `econ_busyness_bt_highstreets_3hourly_counts`
  - `econ_busyness_hex_towncentre_lookup` → `econ_busyness_bt_towncentres_3hourly_counts`
  - `econ_busyness_hex_bid_lookup` → `econ_busyness_bt_bids_3hourly_counts`
  - `econ_busyness_hex_bespoke_lookup` → `econ_busyness_bt_bespokes_3hourly_counts`
- All four aggregate tables UNION → `econ_busyness_bt_3hourly_counts`

BT (daily):
- `econ_busyness_bt_daily_agg_cust_raw` uses `econ_busyness_bt_uid_jan25_lookup` to map `poi_uid`→`poi_id`.

Mastercard (3-hourly):
- `econ_busyness_mrli_3hourly_adj` JOINs:
  - `econ_busyness_mcard_Highstreets_quad_lookup` → highstreets_3hourly_txn
  - `econ_busyness_mcard_TownCentres_quad_lookup` → towncentres_3hourly_txn
  - `econ_busyness_mcard_BIDs_quad_lookup` → bids_3hourly_txn
  - `econ_busyness_mcard_bespoke_quad_lookup` → bespokes_3hourly_txn
- All four UNION → `econ_busyness_mcard_3hourly_txn`

Mastercard (weekly):
- Aggregations from `econ_busyness_mrli_3hourly_adj` plus `econ_busyness_mcard_Inner_Outer_quad_lookup` and `econ_busyness_mcard_adjustment_factors` → weekly `*_txn` and consolidated `econ_busyness_mcard_txn`
- YoY derived from weekly `*_txn` → `econ_busyness_mcard_*_yoy` and consolidated `econ_busyness_mcard_yoy`
- CPI reference stored in `econ_busyness_mcard_cpi_data` (also applied inline in processing)

---

## 🔗 Related Information

- [BT Footfall Data Flow](03.1-bt-data-flow.md)
- [Mastercard Transaction Data Flow](03.2-mastercard-data-flow.md)
- [Sublicensing & Data Distribution](07-sublicensing.md)