WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.msoa11cd,
        lookup.msoa11nm,
        -- Calculate week_start based on yr and wk
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_msoas_quad_lookup AS lookup 
      ON main.quad_id = lookup.quad_id
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    msoa11cd,
    msoa11nm,
    yr,
    wk,
    week_start,
    segment,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY msoa11cd, msoa11nm, yr, wk, week_start, segment
ORDER BY yr, wk;