WITH combined_data AS (
    -- Highstreets data
    SELECT
        count_date,
        hours,
        highstreet_id AS id,
        highstreet_name AS name,
        'highstreets' AS layer,
        txn_amt as txn_amt_retail,
        txn_amt_adj as txn_amt_retail_adj
    FROM econ_busyness_mcard_highstreets_3hourly_txn

    UNION ALL

    -- Towncentres data
    SELECT
        count_date,
        hours,
        tc_id AS id,
        tc_name AS name,
        'towncentres' AS layer,
        txn_amt as txn_amt_retail,
        txn_amt_adj as txn_amt_retail_adj
    FROM econ_busyness_mcard_towncentres_3hourly_txn

    UNION ALL

    -- BIDs data
    SELECT
        count_date,
        hours,
        bid_id AS id,
        bid_name AS name,
        'bids' AS layer,
        txn_amt as txn_amt_retail,
        txn_amt_adj as txn_amt_retail_adj
    FROM econ_busyness_mcard_bids_3hourly_txn

    UNION ALL

    -- Bespoke areas data
    SELECT
        count_date,
        hours,
        bespoke_area_id AS id,
        name AS name,
        'bespoke' AS layer,
        txn_amt as txn_amt_retail,
        txn_amt_adj as txn_amt_retail_adj
    FROM econ_busyness_mcard_bespokes_3hourly_txn
)
SELECT 
    count_date,
    hours,
    id,
    name,
    layer,
    txn_amt_retail,
    txn_amt_retail_adj
FROM combined_data
ORDER BY count_date, layer, id;