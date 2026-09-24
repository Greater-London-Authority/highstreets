-- ============================================================================
-- Strategic Boundaries Lookup Tables
-- Run this once to create the empty tables before the pipeline populates them.
-- ============================================================================

-- Hex → Strategic lookup (BT pipeline)
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_hex_strategic_lookup (
    hex_id          BIGINT,
    strategic_id    INTEGER,
    strategic_name  TEXT
);

CREATE INDEX IF NOT EXISTS idx_hex_strategic_lookup
    ON gisapdata.econ_busyness_hex_strategic_lookup (hex_id);

CREATE INDEX IF NOT EXISTS idx_hex_strategic_lookup_sid
    ON gisapdata.econ_busyness_hex_strategic_lookup (strategic_id);

-- Quad → Strategic lookup (Mastercard pipeline)
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_mcard_strategic_quad_lookup (
    quad_id         BIGINT,
    strategic_id    INTEGER,
    strategic_name  TEXT,
    x               DOUBLE PRECISION,
    y               DOUBLE PRECISION,
    gss_code        TEXT,
    borough         TEXT
);

CREATE INDEX IF NOT EXISTS idx_strategic_quad_lookup
    ON gisapdata.econ_busyness_mcard_strategic_quad_lookup (quad_id);

CREATE INDEX IF NOT EXISTS idx_strategic_quad_lookup_sid
    ON gisapdata.econ_busyness_mcard_strategic_quad_lookup (strategic_id);

-- BT 3-hourly aggregated output
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_bt_strategic_3hourly_counts (
    strategic_id            INTEGER,
    strategic_name          TEXT,
    count_date              DATE,
    hours                   TEXT,
    resident                BIGINT,
    visitor                 BIGINT,
    worker                  BIGINT,
    ave_loyalty_percentage  NUMERIC(10,2),
    ave_dwell_time          NUMERIC(10,2)
);

-- Mastercard 3-hourly aggregated output
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_mcard_strategic_3hourly_txn (
    strategic_id    INTEGER,
    strategic_name  TEXT,
    count_date      DATE,
    hours           TEXT,
    txn_amt         NUMERIC,
    txn_amt_adj     NUMERIC,
    txn_cnt         NUMERIC
);

-- Mastercard weekly txn
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_mcard_strategic_txn (
    strategic_id            INTEGER,
    strategic_name          TEXT,
    yr                      FLOAT,
    wk                      FLOAT,
    week_start              TIMESTAMP,
    txn_amt_wd_retail       NUMERIC,
    txn_amt_wd_eating       NUMERIC,
    txn_amt_wd_apparel      NUMERIC,
    txn_amt_we_retail       NUMERIC,
    txn_amt_we_eating       NUMERIC,
    txn_amt_we_apparel      NUMERIC,
    txn_cnt_wd_retail       NUMERIC,
    txn_cnt_wd_eating       NUMERIC,
    txn_cnt_wd_apparel      NUMERIC,
    txn_cnt_we_retail       NUMERIC,
    txn_cnt_we_eating       NUMERIC,
    txn_cnt_we_apparel      NUMERIC,
    txn_amt_wd_retail_adj   NUMERIC,
    txn_amt_we_retail_adj   NUMERIC
);

-- Mastercard weekly YoY
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_mcard_strategic_yoy (
    strategic_id                INTEGER,
    strategic_name              TEXT,
    yr                          FLOAT,
    wk                          FLOAT,
    week_start                  TIMESTAMP,
    yoy_txn_amt_wd_retail       NUMERIC,
    yoy_txn_amt_wd_eating       NUMERIC,
    yoy_txn_amt_wd_apparel      NUMERIC,
    yoy_txn_amt_we_retail       NUMERIC,
    yoy_txn_amt_we_eating       NUMERIC,
    yoy_txn_amt_we_apparel      NUMERIC,
    yoy_txn_cnt_wd_retail       NUMERIC,
    yoy_txn_cnt_wd_eating       NUMERIC,
    yoy_txn_cnt_wd_apparel      NUMERIC,
    yoy_txn_cnt_we_retail       NUMERIC,
    yoy_txn_cnt_we_eating       NUMERIC,
    yoy_txn_cnt_we_apparel      NUMERIC,
    yoy_txn_amt_wd_retail_adj   NUMERIC,
    yoy_txn_amt_we_retail_adj   NUMERIC
);

-- Mastercard weekly international
CREATE TABLE IF NOT EXISTS gisapdata.econ_busyness_mcard_strategic_intl_txn (
    strategic_id    INTEGER,
    strategic_name  TEXT,
    yr              FLOAT,
    wk              FLOAT,
    week_start      TIMESTAMP,
    segment         TEXT,
    txn_amt_wd_retail   NUMERIC,
    txn_amt_we_retail   NUMERIC,
    txn_amt_wd_retail_adj   NUMERIC,
    txn_amt_we_retail_adj   NUMERIC
);
