WITH
merged_data AS (
    SELECT 
        h.bespoke_area_id,
        h.name,
        t.count_date,
        t.time_indicator,
        t.loyalty_percentage,
        t.dwell_time,
        t.resident,
        t.visitor,
        t.worker
    FROM bt_footfall_tfl_hex_3hourly t
    INNER JOIN econ_busyness_hex_bespoke_lookup h 
        ON t.hex_id::bigint = h.hex_id::bigint
        WHERE h.bespoke_area_id IS NOT NULL
),
aggregated AS (
    SELECT
        bespoke_area_id,
        name,
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
    bespoke_area_id::INTEGER,
    name,
    count_date::DATE,
    time_indicator::TEXT AS hours,
    resident::bigint,
    visitor::bigint,
    worker::bigint,
    ave_loyalty_percentage::NUMERIC(10,2),
    ave_dwell_time::NUMERIC(10,2)
FROM aggregated
WHERE 
    -- Keep row if ANY of these columns has a value (at least one is not null)
    NOT (resident IS NULL AND 
         visitor IS NULL AND 
         worker IS NULL AND 
         ave_loyalty_percentage IS NULL AND 
         ave_dwell_time IS NULL)
ORDER BY bespoke_area_id, count_date, time_indicator;