WITH
merged_data AS (
    SELECT 
        h.strategic_id,
        h.strategic_name,
        t.count_date,
        t.time_indicator,
        t.loyalty_percentage,
        t.dwell_time,
        t.resident,
        t.visitor,
        t.worker
    FROM bt_footfall_tfl_hex_3hourly t
    INNER JOIN econ_busyness_hex_strategic_lookup h 
        ON t.hex_id::bigint = h.hex_id::bigint
        WHERE h.strategic_id IS NOT NULL
),
aggregated AS (
    SELECT
        strategic_id,
        strategic_name,
        count_date,
        time_indicator,
        ROUND(SUM(resident)::NUMERIC, 2) AS resident,
        ROUND(SUM(visitor)::NUMERIC, 2) AS visitor,
        ROUND(SUM(worker)::NUMERIC, 2) AS worker,
        ROUND(AVG(loyalty_percentage)::NUMERIC, 2) AS ave_loyalty_percentage,
        ROUND(AVG(dwell_time)::NUMERIC, 2) AS ave_dwell_time
    FROM merged_data
    GROUP BY 1,2,3,4
)
SELECT
    strategic_id::INTEGER,
    strategic_name,
    count_date::DATE,
    time_indicator::TEXT AS hours,
    resident::bigint,
    visitor::bigint,
    worker::bigint,
    ave_loyalty_percentage::NUMERIC(10,2),
    ave_dwell_time::NUMERIC(10,2)
FROM aggregated
WHERE 
    NOT (resident IS NULL AND 
         visitor IS NULL AND 
         worker IS NULL AND 
         ave_loyalty_percentage IS NULL AND 
         ave_dwell_time IS NULL)
ORDER BY strategic_id, strategic_name, count_date, time_indicator;
