-- Transform Mastercard data to aggregate by bespoke areas
SELECT 
    b.bespoke_area_id::INTEGER as bespoke_area_id,
    b.name,
    d.count_date,
    d.hours,
    ROUND(SUM(d.txn_amt)::NUMERIC, 2) as txn_amt,
    ROUND(SUM(d.txn_amt_adj)::NUMERIC, 2) as txn_amt_adj,
    ROUND(SUM(d.txn_cnt)::NUMERIC, 2) as txn_cnt
FROM 
    econ_busyness_mrli_3hourly_adj d
JOIN 
    econ_busyness_mcard_bespoke_quad_lookup b
    ON d.quad_id::bigint = b.quad_id::bigint
WHERE 
    b.bespoke_area_id IS NOT NULL
GROUP BY 
    b.bespoke_area_id,
    b.name,
    d.count_date,
    d.hours
ORDER BY 
    b.bespoke_area_id, 
    d.count_date, 
    d.hours;