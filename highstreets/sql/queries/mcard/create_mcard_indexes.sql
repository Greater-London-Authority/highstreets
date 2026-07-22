-- Mastercard performance indexes
-- Run once against the production database. Safe to re-run (IF NOT EXISTS).

-- ============================================================================
-- STAGING TABLE: econ_busyness_mcard_stg_18_zoom
-- Scanned by all 9 weekly aggregation queries + inner_outer_weekly_summary
-- Pattern: JOIN main.quad_id::BIGINT = lookup.quad_id::BIGINT
--          WHERE industry IN (...) AND segment = 'Overall' AND geo_name = 'London'
--          GROUP BY yr, wk, ...
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mcard_stg_18_quad
    ON gisapdata.econ_busyness_mcard_stg_18_zoom (quad_id);
CREATE INDEX IF NOT EXISTS idx_mcard_stg_18_yr_wk
    ON gisapdata.econ_busyness_mcard_stg_18_zoom (yr, wk);

-- ============================================================================
-- CLEAN TABLE: econ_busyness_mcard_clean_18_zoom
-- Used in clean_and_process_data() for watermark reads (MAX yr, MAX wk)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mcard_clean_18_quad
    ON gisapdata.econ_busyness_mcard_clean_18_zoom (quad_id);
CREATE INDEX IF NOT EXISTS idx_mcard_clean_18_yr_wk
    ON gisapdata.econ_busyness_mcard_clean_18_zoom (yr, wk);

-- ============================================================================
-- RAW TABLE: econ_busyness_mcard_raw_18_zoom
-- Scanned by all 6 international queries:
--   JOIN main.quad_id::bigint = lookup.quad_id::bigint
--   WHERE industry = 'Total Retail' AND segment = 'International' AND geo_name = 'London'
-- Also: incremental_refresh inserts WHERE (yr > :max_yr OR (yr = :max_yr AND wk > :max_wk))
-- Also: get_partial_data WHERE ((yr >= 2024 AND wk > 47) OR (yr > 2024)) AND ...
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mcard_raw_18_quad
    ON gisapdata.econ_busyness_mcard_raw_18_zoom (quad_id);
CREATE INDEX IF NOT EXISTS idx_mcard_raw_18_yr_wk
    ON gisapdata.econ_busyness_mcard_raw_18_zoom (yr, wk);

-- ============================================================================
-- QUAD LOOKUP TABLES (8 tables)
-- Other side of every JOIN: lookup.quad_id::BIGINT
-- Used in: 9 weekly + 6 intl + 4 threehourly + adjustment script
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_hs_quad_lookup
    ON gisapdata.econ_busyness_mcard_highstreets_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_bid_quad_lookup
    ON gisapdata.econ_busyness_mcard_bids_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_tc_quad_lookup
    ON gisapdata.econ_busyness_mcard_towncentres_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_bespoke_quad_lookup
    ON gisapdata.econ_busyness_mcard_bespoke_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_borough_quad_lookup
    ON gisapdata.econ_busyness_mcard_boroughs_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_caz_quad_lookup
    ON gisapdata.econ_busyness_mcard_caz_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_msoa_quad_lookup
    ON gisapdata.econ_busyness_mcard_msoas_quad_lookup (quad_id);
CREATE INDEX IF NOT EXISTS idx_io_quad_lookup
    ON gisapdata.econ_busyness_mcard_inner_outer_quad_lookup (quad_id);

-- ============================================================================
-- 3-HOURLY RAW: econ_busyness_mrli_3hourly
-- update_mcard_adjustment_no_merge.sql:
--   WHERE EXTRACT(YEAR FROM count_date) = :yr AND EXTRACT(MONTH FROM count_date) = :mo
--   JOIN m.quad_id::bigint = i.quad_id::bigint
-- Also: MAX(count_date) watermark in append_data_to_postgres
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mrli_3hourly_date
    ON gisapdata.econ_busyness_mrli_3hourly (count_date);
CREATE INDEX IF NOT EXISTS idx_mrli_3hourly_quad
    ON gisapdata.econ_busyness_mrli_3hourly (quad_id);

-- ============================================================================
-- 3-HOURLY ADJUSTED: econ_busyness_mrli_3hourly_adj
-- All 4 POI transform queries (quad_hs, quad_tc, quad_bid, quad_bespoke):
--   JOIN d.quad_id::bigint = lookup.quad_id::bigint
--   GROUP BY d.count_date, d.hours, ...
-- Also merge key in adjustment script: (quad_id, count_date, hours)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_mrli_3hourly_adj_quad
    ON gisapdata.econ_busyness_mrli_3hourly_adj (quad_id);
CREATE INDEX IF NOT EXISTS idx_mrli_3hourly_adj_date_hours
    ON gisapdata.econ_busyness_mrli_3hourly_adj (count_date, hours);
