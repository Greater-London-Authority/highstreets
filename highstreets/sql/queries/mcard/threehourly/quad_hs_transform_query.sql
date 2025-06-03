-- Transform Mastercard data to aggregate by highstreets
SELECT 
    h.highstreet_id::INTEGER as highstreet_id,
    h.highstreet_name,
    d.count_date,
    d.hours,
    h.borough,
    h.x,
    h.y,
    ROUND(SUM(d.txn_amt)::NUMERIC, 2) as txn_amt,
    ROUND(SUM(d.txn_amt_adj)::NUMERIC, 2) as txn_amt_adj,
    ROUND(SUM(d.txn_cnt)::NUMERIC, 2) as txn_cnt
FROM 
    econ_busyness_mrli_3hourly_adj d
JOIN 
    econ_busyness_mcard_Highstreets_quad_lookup h
    ON d.quad_id::bigint = h.quad_id::bigint
WHERE 
    h.highstreet_id IS NOT NULL
GROUP BY 
    h.highstreet_id,
    h.highstreet_name,
    d.count_date,
    d.hours,
    h.borough,
    h.x,
    h.y
ORDER BY 
    h.highstreet_id, 
    d.count_date, 
    d.hours;