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
        lookup.strategic_id,
        lookup.strategic_name,
        TO_CHAR(
            (DATE_TRUNC('week', 
               TO_DATE(CONCAT(main.yr, '0104'), 'YYYYMMDD') 
               + INTERVAL '1 day' * (7 * (main.wk - 1))
            )), 'YYYY-MM-DD') AS week_start
    FROM econ_busyness_mcard_raw_18_zoom AS main
    JOIN econ_busyness_mcard_strategic_quad_lookup AS lookup 
      ON main.quad_id::bigint = lookup.quad_id::bigint
    WHERE main.industry = 'Total Retail'
      AND main.segment = 'International'
      AND main.geo_name = 'London'
)
SELECT
    strategic_id,
    strategic_name,
    yr,
    wk,
    week_start,
    segment,
    SUM(CASE WHEN weekday_weekend = 'weekdays' THEN txn_amt END) AS txn_amt_wd_retail,
    SUM(CASE WHEN weekday_weekend = 'weekends' THEN txn_amt END) AS txn_amt_we_retail
FROM week_data
GROUP BY strategic_id, strategic_name, yr, wk, week_start, segment
ORDER BY yr, wk;
