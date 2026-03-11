WITH hs_borough AS (
    SELECT DISTINCT ON (highstreet_id)
        highstreet_id::TEXT AS poi_id, borough
    FROM econ_busyness_mcard_Highstreets_quad_lookup
    WHERE highstreet_id IS NOT NULL
),
tc_borough AS (
    SELECT DISTINCT ON (tc_id)
        tc_id::TEXT AS poi_id, borough
    FROM econ_busyness_mcard_TownCentres_quad_lookup
    WHERE tc_id IS NOT NULL
),
bid_borough AS (
    SELECT DISTINCT ON (bid_id)
        bid_id::TEXT AS poi_id, borough_name AS borough
    FROM econ_busyness_borough_hs_lookup_3
    WHERE bid_id IS NOT NULL
),
borough_lookup AS (
    SELECT poi_id, 'highstreets' AS poi_type, borough FROM hs_borough
    UNION ALL
    SELECT poi_id, 'towncentres', borough FROM tc_borough
    UNION ALL
    SELECT poi_id, 'bids', borough FROM bid_borough
)
SELECT d.poi_id, d.poi_uid, d.poi_name, d.poi_type, d.count_date,
    d.time_indicator, d.total_unique_volume,
    d.total_unique_intl_only_visitors, d.total_unique_domestic_visitors,
    d.total_unique_workers, d.total_unique_residents, d.avg_dwell_time,
    COALESCE(bl.borough,
        CASE WHEN d.poi_type = 'borough' THEN d.poi_name ELSE NULL END
    ) AS borough
FROM econ_busyness_bt_daily_agg_cust_raw d
LEFT JOIN borough_lookup bl
    ON d.poi_id = bl.poi_id AND d.poi_type = bl.poi_type
