-- BT table safety constraints
-- Run once against the production database. Safe to re-run (IF NOT EXISTS equivalent via DO block).

-- Daily aggregate: prevent duplicate (poi_id, poi_type, count_date, time_indicator) rows
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_daily_agg_cust_raw'
    ) THEN
        ALTER TABLE gisapdata.econ_busyness_bt_daily_agg_cust_raw
            ADD CONSTRAINT uq_daily_agg_cust_raw
            UNIQUE (poi_id, poi_type, count_date, time_indicator);
    END IF;
END $$;

-- Outage data: prevent duplicate (count_date, lad_name) rows
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_outage_data'
    ) THEN
        ALTER TABLE gisapdata.econ_busyness_bt_outage_data
            ADD CONSTRAINT uq_outage_data
            UNIQUE (count_date, lad_name);
    END IF;
END $$;
