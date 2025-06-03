-- Transform Mastercard data to aggregate by town centers
SELECT 
    t.tc_id::INTEGER as tc_id,
    t.tc_name,
    d.count_date,
    d.hours,
    t.borough,
    t.x,
    t.y,
    ROUND(SUM(d.txn_amt)::NUMERIC, 2) as txn_amt,
    ROUND(SUM(d.txn_amt_adj)::NUMERIC, 2) as txn_amt_adj,
    ROUND(SUM(d.txn_cnt)::NUMERIC, 2) as txn_cnt
FROM 
    econ_busyness_mrli_3hourly_adj d
JOIN 
    econ_busyness_mcard_TownCentres_quad_lookup t
    ON d.quad_id::bigint = t.quad_id::bigint
WHERE 
    t.tc_id IS NOT NULL
GROUP BY 
    t.tc_id,
    t.tc_name,
    d.count_date,
    d.hours,
    t.borough,
    t.x,
    t.y
ORDER BY 
    t.tc_id, 
    d.count_date, 
    d.hours;