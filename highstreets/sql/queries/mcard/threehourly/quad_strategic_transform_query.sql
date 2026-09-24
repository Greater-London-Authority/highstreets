SELECT 
    s.strategic_id::INTEGER as strategic_id,
    s.strategic_name,
    d.count_date,
    d.hours,
    ROUND(SUM(d.txn_amt)::NUMERIC, 2) as txn_amt,
    ROUND(SUM(d.txn_amt_adj)::NUMERIC, 2) as txn_amt_adj,
    ROUND(SUM(d.txn_cnt)::NUMERIC, 2) as txn_cnt
FROM 
    econ_busyness_mrli_3hourly_adj d
JOIN 
    econ_busyness_mcard_strategic_quad_lookup s
    ON d.quad_id::bigint = s.quad_id::bigint
WHERE 
    s.strategic_id IS NOT NULL
GROUP BY 
    s.strategic_id,
    s.strategic_name,
    d.count_date,
    d.hours
ORDER BY 
    s.strategic_id, 
    s.strategic_name,
    d.count_date, 
    d.hours;
