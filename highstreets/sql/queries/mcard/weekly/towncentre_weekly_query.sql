WITH week_data AS (
    SELECT 
        main.yr,
        main.wk,
        DATE_TRUNC('week', DATE(main.yr || '-01-04')) + INTERVAL '1 week' * (main.wk - 1) AS week_start, -- create week_start column
        main.industry,
        main.segment,
        main.geo_name,
        main.quad_id,
        main.txn_amt,
        main.txn_cnt,
        main.weekday_weekend,
        lookup.tc_id,
        lookup.tc_name
    FROM econ_busyness_mcard_stg_18_zoom AS main
    INNER JOIN econ_busyness_mcard_towncentres_quad_lookup AS lookup 
      ON main.quad_id::BIGINT = lookup.quad_id::BIGINT
    WHERE main.industry IN('Total Retail', 'Total Apparel', 'Eating Places')
      AND main.segment = 'Overall'
      AND main.geo_name = 'London'
)
SELECT
    tc_id,
    tc_name,
    yr,
    wk,
    week_start,
    -- Pivot the txn_amt values based on weekday_weekend and industry
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Retail' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_retail,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Eating Places' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_eating,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Apparel' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_apparel,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Retail' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_retail,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Eating Places' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_eating,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Apparel' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_apparel,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Retail' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_wd_retail,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Eating Places' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_wd_eating,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Apparel' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_wd_apparel,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Retail' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_we_retail,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Eating Places' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_we_eating,
    ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Apparel' THEN txn_cnt ELSE 0 END)::numeric, 3) AS txn_cnt_we_apparel

FROM week_data
GROUP BY tc_id, tc_name, yr, wk, week_start
ORDER BY yr, wk;