WITH combined_data AS (
    -- BIDs data
    SELECT
        yr,
        wk,
        week_start::date,
        bid_id::bigint AS id,
        bid_name AS name,
        'bids' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_bids_txn

    UNION ALL

    -- Highstreets data
    SELECT
        yr,
        wk,
        week_start::date,
        highstreet_id::bigint AS id,
        highstreet_name AS name,
        'highstreets' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_highstreets_txn

    UNION ALL

    -- Towncentres data
    SELECT
        yr,
        wk,
        week_start::date,
        tc_id::bigint AS id,
        tc_name AS name,
        'towncentres' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_towncentres_txn

    UNION ALL

    -- Towncentres in CAZ
    SELECT
        yr,
        wk,
        week_start::date,
        tc_id::bigint AS id,
        tc_name AS name,
        'towncentres_in_caz' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_towncentres_txn
    WHERE tc_id::bigint IN (SELECT tc_id FROM econ_busyness_mcard_towncentre_caz_lookup)

    UNION ALL

    -- CAZ data
    SELECT
        yr,
        wk,
        week_start::date,
        1 AS id,  -- CAZ only has one area, so we use 1 as id
        LOWER(name) AS name,
        'caz' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_caz_txn

    UNION ALL

    -- Bespoke areas data
    SELECT
        yr,
        wk,
        week_start::date,
        bespoke_area_id::bigint AS id,
        name AS name,
        'bespoke' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_bespoke_txn

    UNION ALL

    -- Boroughs data
    SELECT
        yr,
        wk,
        week_start::date,
        CAST(regexp_replace(gss_code, '^.{3}0+', '', 'g') AS INTEGER) AS id,
        name AS name,
        'boroughs' AS layer,
        txn_amt_wd_eating,
        txn_amt_we_eating,
        txn_amt_wd_apparel,
        txn_amt_we_apparel,
        txn_amt_wd_retail,
        txn_amt_we_retail,
        txn_cnt_wd_eating,
        txn_cnt_we_eating,
        txn_cnt_wd_apparel,
        txn_cnt_we_apparel,
        txn_cnt_wd_retail,
        txn_cnt_we_retail,
        txn_amt_wd_retail_adj,
        txn_amt_we_retail_adj
    FROM econ_busyness_mcard_boroughs_txn
)
SELECT 
    yr,
    wk,
    week_start,
    id::bigint,
    name,
    layer,
    txn_amt_wd_eating,
    txn_amt_we_eating,
    txn_amt_wd_apparel,
    txn_amt_we_apparel,
    txn_amt_wd_retail,
    txn_amt_we_retail,
    txn_cnt_wd_eating,
    txn_cnt_we_eating,
    txn_cnt_wd_apparel,
    txn_cnt_we_apparel,
    txn_cnt_wd_retail,
    txn_cnt_we_retail,
    txn_amt_wd_retail_adj,
    txn_amt_we_retail_adj,
    ROW_NUMBER() OVER (ORDER BY yr, wk, layer, id) AS objectid
FROM combined_data
ORDER BY yr, wk, layer, id; 