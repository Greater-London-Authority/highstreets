-- Start transaction for atomicity
BEGIN;

-- Set performance parameters
SET LOCAL work_mem = '1GB';
SET LOCAL maintenance_work_mem = '2GB';

-- Materialize lookup tables with proper indexing for better join performance
CREATE TEMPORARY TABLE temp_cpi AS 
SELECT 
    yr::INTEGER, 
    month::INTEGER,
    CASE 
        WHEN cpi_index > 0 THEN 
            (cpi_index / NULLIF((
                SELECT AVG(cpi_index) 
                FROM econ_busyness_mcard_cpi_data 
                WHERE yr = 2018 AND aggregate = 'Overall Index'
            ), 0)) * 100
        ELSE 100
    END::NUMERIC(12,4) AS cpi_index
FROM econ_busyness_mcard_cpi_data
WHERE aggregate = 'Overall Index';

-- Use BIGINT for quad_id since values exceed INTEGER range
CREATE TEMPORARY TABLE temp_inner_outer AS 
SELECT 
    DISTINCT ON (quad_id)
    quad_id::BIGINT,
    inner_outer 
FROM econ_busyness_mcard_Inner_Outer_quad_lookup
ORDER BY quad_id, inner_outer DESC;

CREATE TEMPORARY TABLE temp_adj_factors AS
SELECT 
    yr::INTEGER, 
    month::INTEGER, 
    inner_outer,
    COALESCE(NULLIF(adjustment_factor_retail, 0), 1.0)::NUMERIC(12,4) AS factor
FROM econ_busyness_mcard_adjustment_factors;

-- Create indexes on temporary tables
CREATE INDEX idx_temp_inner_outer ON temp_inner_outer(quad_id);
CREATE INDEX idx_temp_cpi ON temp_cpi(yr, month);
CREATE INDEX idx_temp_adj_factors ON temp_adj_factors(yr, month, inner_outer);

-- Create a staging table for calculations
CREATE TABLE IF NOT EXISTS econ_busyness_mrli_3hourly_adj_new (
    ldn_ref BIGINT,
    quad_id BIGINT,
    count_date DATE,
    hours VARCHAR(10),
    txn_amt NUMERIC,
    txn_cnt INTEGER,
    txn_amt_adj NUMERIC(12,4)
);

-- Clear the staging table if it exists
TRUNCATE econ_busyness_mrli_3hourly_adj_new;

-- Create index on staging table for merge operation
CREATE INDEX IF NOT EXISTS idx_mrli_3hourly_adj_new ON econ_busyness_mrli_3hourly_adj_new(quad_id, count_date, hours);

-- Populate the staging table in smaller batches by month/year for better memory management
DO $$
DECLARE
    current_yr INTEGER;
    current_mo INTEGER;
    rec RECORD;
    default_factor NUMERIC(12,4) := 1.0;
    default_cpi NUMERIC(12,4) := 100.0;
BEGIN
    -- Get unique year/month combinations
    FOR rec IN 
        SELECT DISTINCT 
            EXTRACT(YEAR FROM count_date)::INTEGER AS year, 
            EXTRACT(MONTH FROM count_date)::INTEGER AS month
        FROM econ_busyness_mrli_3hourly
        ORDER BY 1, 2
    LOOP
        current_yr := rec.year;
        current_mo := rec.month;
        
        RAISE NOTICE 'Processing year % month %', current_yr, current_mo;
        
        -- Insert data for the current year/month with calculated adjustment
        -- Use explicit numeric casts to fix ROUND function
        EXECUTE format('
        INSERT INTO econ_busyness_mrli_3hourly_adj_new (
            ldn_ref, quad_id, count_date, hours, txn_amt, txn_cnt, txn_amt_adj
        )
        SELECT 
            m.ldn_ref,
            m.quad_id,
            m.count_date,
            m.hours,
            m.txn_amt,
            m.txn_cnt,
            -- Safe calculation with fallbacks and explicit NUMERIC cast
            CASE
                WHEN m.txn_amt = 0 THEN 0
                ELSE ((m.txn_amt::NUMERIC / 
                     COALESCE(a.factor, %s)::NUMERIC) / 
                    COALESCE(c.cpi_index, %s)::NUMERIC * 100)::NUMERIC(12,4)
            END AS txn_amt_adj
        FROM 
            econ_busyness_mrli_3hourly m
        LEFT JOIN 
            temp_inner_outer i ON m.quad_id::bigint = i.quad_id::bigint
        LEFT JOIN 
            temp_adj_factors a ON 
                %s = a.yr AND 
                %s = a.month AND 
                COALESCE(i.inner_outer, ''Outer'') = a.inner_outer
        LEFT JOIN 
            temp_cpi c ON 
                %s = c.yr AND 
                %s = c.month
        WHERE 
            EXTRACT(YEAR FROM m.count_date)::INTEGER = %s AND
            EXTRACT(MONTH FROM m.count_date)::INTEGER = %s',
            default_factor, default_cpi, 
            current_yr, current_mo, 
            current_yr, current_mo,
            current_yr, current_mo);
    END LOOP;
END $$;

-- For initial setup, check if the target table exists
DO $$
BEGIN
    -- Check if the table exists and create if needed
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                   WHERE table_name = 'econ_busyness_mrli_3hourly_adj') THEN
        -- Create it if it doesn't exist
        EXECUTE 'CREATE TABLE econ_busyness_mrli_3hourly_adj (
            ldn_ref BIGINT,
            quad_id BIGINT,
            count_date DATE,
            hours VARCHAR(10),
            txn_amt NUMERIC,
            txn_cnt INTEGER,
            txn_amt_adj NUMERIC(12,4)
        )';
        
        EXECUTE 'CREATE INDEX idx_mrli_3hourly_adj_quadid 
                 ON econ_busyness_mrli_3hourly_adj(quad_id, count_date, hours)';
    END IF;
END $$;

-- Perform efficient merge - only update existing records - using test table
UPDATE econ_busyness_mrli_3hourly_adj a
SET txn_amt_adj = n.txn_amt_adj
FROM econ_busyness_mrli_3hourly_adj_new n
WHERE 
    a.quad_id = n.quad_id AND
    a.count_date = n.count_date AND
    a.hours = n.hours;

-- Insert records that don't exist in the target test table
INSERT INTO econ_busyness_mrli_3hourly_adj (
    ldn_ref, quad_id, count_date, hours, txn_amt, txn_cnt, txn_amt_adj
)
SELECT 
    n.ldn_ref, n.quad_id, n.count_date, n.hours, n.txn_amt, n.txn_cnt, n.txn_amt_adj
FROM 
    econ_busyness_mrli_3hourly_adj_new n
LEFT JOIN 
    econ_busyness_mrli_3hourly_adj a ON 
        n.quad_id = a.quad_id AND
        n.count_date = a.count_date AND
        n.hours = a.hours
WHERE 
    a.quad_id IS NULL;

-- Clean up
DROP TABLE econ_busyness_mrli_3hourly_adj_new;
DROP TABLE temp_cpi;
DROP TABLE temp_inner_outer;
DROP TABLE temp_adj_factors;

-- Commit all changes
COMMIT;