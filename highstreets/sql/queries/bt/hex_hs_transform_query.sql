WITH
merged_data AS (
    SELECT 
        h.highstreet_id,
        h.highstreet_name,
        t.count_date,
        t.time_indicator,
        t.loyalty_percentage,
        t.dwell_time,
        t.resident,
        t.visitor,
        t.worker
    FROM bt_footfall_tfl_hex_3hourly t
    LEFT JOIN econ_busyness_hex_highstreet_lookup h 
        ON t.hex_id = h.hex_id
    WHERE h.highstreet_id IS NOT NULL
),
unpivoted AS (
    SELECT
        m.highstreet_id,
        m.highstreet_name,
        m.count_date,
        m.time_indicator::TEXT,
        m.loyalty_percentage,
        m.dwell_time,
        u.count_type,
        u.volume
    FROM merged_data m
    CROSS JOIN LATERAL (
        VALUES
            ('resident', resident),
            ('visitor', visitor),
            ('worker', worker)
    ) u(count_type, volume)
),
aggregated AS (
    SELECT
        highstreet_id,
        highstreet_name,
        count_date,
        time_indicator,
        count_type,
        ROUND(SUM(volume)::NUMERIC, 2) AS volume,
        ROUND(AVG(loyalty_percentage)::NUMERIC, 2) AS ave_loyalty_percentage,
        ROUND(AVG(dwell_time)::NUMERIC, 2) AS ave_dwell_time
    FROM unpivoted
    GROUP BY 1,2,3,4,5
),
pivoted AS (
    SELECT
        highstreet_id,
        highstreet_name,
        count_date,
        time_indicator,
        ave_loyalty_percentage,
        ave_dwell_time,
        SUM(volume) FILTER (WHERE count_type = 'resident')::INTEGER AS resident,
        SUM(volume) FILTER (WHERE count_type = 'visitor')::INTEGER AS visitor,
        SUM(volume) FILTER (WHERE count_type = 'worker')::INTEGER AS worker
    FROM aggregated
    GROUP BY 1,2,3,4,5,6
)
SELECT
    p.highstreet_id::INTEGER,
    p.highstreet_name,
    p.count_date::DATE,
    p.time_indicator::TEXT AS hours,
    q.x,
    q.y,
    q.borough,
    p.resident::bigint,
    p.visitor::bigint,
    p.worker::bigint,
    p.ave_loyalty_percentage::NUMERIC(10,2),
    p.ave_dwell_time::NUMERIC(10,2)
FROM pivoted p
LEFT JOIN (
    SELECT DISTINCT ON (highstreet_id)
        highstreet_id::bigint,
        x,
        y,
        borough
    FROM econ_busyness_mcard_Highstreets_quad_lookup
) q USING (highstreet_id)
WHERE
    -- Keep row if ANY of these columns has a value (at least one is not null)
    NOT (resident IS NULL AND 
         visitor IS NULL AND 
         worker IS NULL AND 
         ave_loyalty_percentage IS NULL AND 
         ave_dwell_time IS NULL)
ORDER BY p.highstreet_id, p.count_date, hours;