-- BT performance indexes
-- Run once against the production database. Safe to re-run (IF NOT EXISTS).

-- ============================================================================
-- HEX RAW: bt_footfall_tfl_hex_3hourly
-- hex_hs/tc/bid/bespoke_transform_query.sql:
--   JOIN t.hex_id = h.hex_id  (or t.hex_id::bigint = h.hex_id::bigint)
--   GROUP BY count_date, time_indicator, ...
-- Also: MAX(count_date) watermark on every append
-- Also: year-partition export WHERE count_date >= ... AND count_date < ...
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_bt_hex_3h_hexid
    ON gisapdata.bt_footfall_tfl_hex_3hourly (hex_id);
CREATE INDEX IF NOT EXISTS idx_bt_hex_3h_date
    ON gisapdata.bt_footfall_tfl_hex_3hourly (count_date);

-- ============================================================================
-- HEX LOOKUP TABLES (4 tables)
-- Other side of every hex transform JOIN: h.hex_id
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_hex_hs_lookup
    ON gisapdata.econ_busyness_hex_highstreet_lookup (hex_id);
CREATE INDEX IF NOT EXISTS idx_hex_tc_lookup
    ON gisapdata.econ_busyness_hex_towncentre_lookup (hex_id);
CREATE INDEX IF NOT EXISTS idx_hex_bid_lookup
    ON gisapdata.econ_busyness_hex_bid_lookup (hex_id);
CREATE INDEX IF NOT EXISTS idx_hex_bespoke_lookup
    ON gisapdata.econ_busyness_hex_bespoke_lookup (hex_id);

-- ============================================================================
-- MSOA HOURLY: bt_footfall_msoa_hourly
-- MAX(count_date) watermark on append
-- Full export ORDER BY count_date; half-year range WHERE count_date >= ...
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_bt_msoa_hourly_date
    ON gisapdata.bt_footfall_msoa_hourly (count_date);

-- ============================================================================
-- LSOA HOURLY: bt_footfall_lsoa_hourly
-- MAX(count_date) watermark on append
-- Half-year export WHERE count_date >= ... AND count_date <= ...
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_bt_lsoa_hourly_date
    ON gisapdata.bt_footfall_lsoa_hourly (count_date);

-- ============================================================================
-- DAILY AGGREGATE: econ_busyness_bt_daily_agg_cust_raw
-- MAX(count_date) watermark on append
-- daily_agg_borough_enriched.sql: JOIN d.poi_id = bl.poi_id AND d.poi_type = bl.poi_type
-- Export: WHERE poi_type = :val ORDER BY count_date
-- Note: UNIQUE constraint uq_daily_agg_cust_raw on (poi_id, poi_type, count_date, time_indicator)
--   already provides an implicit index but leading with poi_id — not optimal for
--   MAX(count_date) alone or poi_type-first partition queries.
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_bt_daily_agg_date
    ON gisapdata.econ_busyness_bt_daily_agg_cust_raw (count_date);
CREATE INDEX IF NOT EXISTS idx_bt_daily_agg_poi_type_date
    ON gisapdata.econ_busyness_bt_daily_agg_cust_raw (poi_type, count_date);

-- ============================================================================
-- OUTAGE: econ_busyness_bt_outage_data
-- MAX(count_date) watermark on append
-- Note: UNIQUE constraint uq_outage_data on (count_date, lad_name) already provides
--   an implicit index starting with count_date — sufficient for MAX(count_date).
--   No additional index needed.
-- ============================================================================
