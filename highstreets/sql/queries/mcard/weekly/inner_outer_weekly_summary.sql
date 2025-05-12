WITH filtered_data AS (
  SELECT 
    c.week_start,
    c.wk,
    c.industry,
    c.weekday_weekend,
    c.txn_amt,
    l.inner_outer
  FROM 
    econ_busyness_mcard_clean_18_zoom c
  JOIN 
    econ_busyness_mcard_inner_outer_quad_lookup l ON c.quad_id = l.quad_id
  WHERE 
    c.industry IN ('Total Retail', 'Total Apparel', 'Eating Places')
)
SELECT
  week_start,
  inner_outer,
  EXTRACT(MONTH FROM week_start)::SMALLINT as month,
  EXTRACT(YEAR FROM week_start)::SMALLINT as yr,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Retail' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_retail,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Eating Places' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_eating,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekdays' AND industry = 'Total Apparel' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_wd_apparel,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Retail' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_retail,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Eating Places' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_eating,
  ROUND(SUM(CASE WHEN weekday_weekend = 'weekends' AND industry = 'Total Apparel' THEN txn_amt ELSE 0 END)::numeric, 3) AS txn_amt_we_apparel
FROM 
  filtered_data
GROUP BY 
  week_start, inner_outer
ORDER BY 
  week_start, inner_outer;