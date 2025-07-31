WITH combined_data AS (
    -- Highstreets data
    SELECT
        count_date,
        hours,
        highstreet_id AS id,
        highstreet_name AS name,
        'highstreets' AS layer,
        resident,
        visitor,
        worker,
        ave_loyalty_percentage,
        ave_dwell_time
    FROM econ_busyness_bt_highstreets_3hourly_counts

    UNION ALL

    -- Town centres data
    SELECT
        count_date,
        hours,
        tc_id AS id,
        tc_name AS name,
        'towncentres' AS layer,
        resident,
        visitor,
        worker,
        ave_loyalty_percentage,
        ave_dwell_time
    FROM econ_busyness_bt_towncentres_3hourly_counts

    UNION ALL

    -- BIDs data
    SELECT
        count_date,
        hours,
        bid_id AS id,
        bid_name AS name,
        'bids' AS layer,
        resident,
        visitor,
        worker,
        ave_loyalty_percentage,
        ave_dwell_time
    FROM econ_busyness_bt_bids_3hourly_counts

    UNION ALL

    -- Bespoke areas data
    SELECT
        count_date,
        hours,
        bespoke_area_id AS id,
        name,
        'bespoke' AS layer,
        resident,
        visitor,
        worker,
        ave_loyalty_percentage,
        ave_dwell_time
    FROM econ_busyness_bt_bespokes_3hourly_counts
)
SELECT 
    count_date,
    hours,
    id,
    name,
    layer,
    resident,
    visitor,
    worker,
    ave_loyalty_percentage,
    ave_dwell_time
FROM combined_data
ORDER BY count_date;