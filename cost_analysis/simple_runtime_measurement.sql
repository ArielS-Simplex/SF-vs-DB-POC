-- ==============================================================================
-- SIMPLE RUNTIME MEASUREMENT FOR ENHANCED WORKING ETL
-- Quick execution to measure actual performance and costs
-- ==============================================================================

-- Record start time
SET START_TIME = CURRENT_TIMESTAMP();

-- Run a simplified version of enhanced_working_etl.sql to measure performance
-- This processes actual 12.6M records to get real runtime data

SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TEST_TABLE = 'POC.PUBLIC.RUNTIME_TEST_RESULTS';

-- Drop test table if exists
DROP TABLE IF EXISTS IDENTIFIER($TEST_TABLE);

-- Simple count query to test performance
CREATE TABLE IDENTIFIER($TEST_TABLE) AS
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    MIN(transaction_date) AS min_date,
    MAX(transaction_date) AS max_date,
    AVG(CASE WHEN result = 'SUCCESS' THEN 1 ELSE 0 END) AS success_rate
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) >= $DATE_RANGE_START
  AND DATE(transaction_date) <= $DATE_RANGE_END
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );

-- Record end time
SET END_TIME = CURRENT_TIMESTAMP();

-- Calculate runtime metrics
SELECT 
    'Runtime Performance Test' AS test_type,
    $START_TIME AS start_time,
    $END_TIME AS end_time,
    DATEDIFF('second', $START_TIME, $END_TIME) AS runtime_seconds,
    ROUND(DATEDIFF('second', $START_TIME, $END_TIME) / 60.0, 2) AS runtime_minutes,
    total_rows,
    ROUND(total_rows / DATEDIFF('second', $START_TIME, $END_TIME), 0) AS rows_per_second
FROM IDENTIFIER($TEST_TABLE);

-- Get cost data for this specific test
SELECT 
    'Cost Analysis for Runtime Test' AS analysis_type,
    query_type,
    warehouse_name,
    execution_time / 1000 AS execution_seconds,
    credits_used_cloud_services,
    credits_used_compute,
    (credits_used_cloud_services + COALESCE(credits_used_compute, 0)) AS total_credits,
    rows_produced,
    bytes_scanned / POWER(1024, 3) AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= $START_TIME
  AND query_text ILIKE '%RUNTIME_TEST_RESULTS%'
  AND execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 5;

-- Cleanup
DROP TABLE IF EXISTS IDENTIFIER($TEST_TABLE);