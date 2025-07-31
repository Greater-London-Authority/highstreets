WITH combined_data AS (
    -- BIDs data
    SELECT
        yr,
        wk,
        week_start::date,
        bid_id::bigint AS id,
        bid_name AS name,
        'bids' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_bids_yoy

    UNION ALL

    -- Highstreets data
    SELECT
        yr,
        wk,
        week_start::date,
        highstreet_id::bigint AS id,
        highstreet_name AS name,
        'highstreets' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_highstreets_yoy

    UNION ALL

    -- Towncentres data
    SELECT
        yr,
        wk,
        week_start::date,
        tc_id::bigint AS id,
        tc_name AS name,
        'towncentres' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_towncentres_yoy

    UNION ALL

    -- Towncentres in CAZ
    SELECT
        yr,
        wk,
        week_start::date,
        tc_id::bigint AS id,
        tc_name AS name,
        'towncentres_in_caz' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_towncentres_yoy
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
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_caz_yoy

    UNION ALL

    -- Bespoke areas data
    SELECT
        yr,
        wk,
        week_start::date,
        bespoke_area_id::bigint AS id,
        name AS name,
        'bespoke' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_bespoke_yoy

    UNION ALL

    -- Boroughs data
    SELECT
        yr,
        wk,
        week_start::date,
        CAST(regexp_replace(gss_code, '^.{3}0+', '', 'g') AS INTEGER) AS id,
        name AS name,
        'boroughs' AS layer,
        yoy_txn_amt_wd_eating,
        yoy_txn_amt_we_eating,
        yoy_txn_amt_wd_apparel,
        yoy_txn_amt_we_apparel,
        yoy_txn_amt_wd_retail,
        yoy_txn_amt_we_retail,
        yoy_txn_cnt_wd_eating,
        yoy_txn_cnt_we_eating,
        yoy_txn_cnt_wd_apparel,
        yoy_txn_cnt_we_apparel,
        yoy_txn_cnt_wd_retail,
        yoy_txn_cnt_we_retail,
        yoy_txn_amt_wd_retail_adj,
        yoy_txn_amt_we_retail_adj
    FROM econ_busyness_mcard_boroughs_yoy
)
SELECT 
    yr,
    wk,
    week_start,
    id::bigint,
    name,
    layer,
    yoy_txn_amt_wd_eating,
    yoy_txn_amt_we_eating,
    yoy_txn_amt_wd_apparel,
    yoy_txn_amt_we_apparel,
    yoy_txn_amt_wd_retail,
    yoy_txn_amt_we_retail,
    yoy_txn_cnt_wd_eating,
    yoy_txn_cnt_we_eating,
    yoy_txn_cnt_wd_apparel,
    yoy_txn_cnt_we_apparel,
    yoy_txn_cnt_wd_retail,
    yoy_txn_cnt_we_retail,
    yoy_txn_amt_wd_retail_adj,
    yoy_txn_amt_we_retail_adj,
    ROW_NUMBER() OVER (ORDER BY yr, wk, layer, id) AS objectid
FROM combined_data
ORDER BY yr, wk, layer, id; 