-- ==============================================================================
-- TIME BOUNDARY FIX - EXACT DATABRICKS HOUR 23 HANDLING
-- Based on Databricks comparison showing +4,479 in hour 23
-- ==============================================================================

-- DATABRICKS FINDINGS:
-- Hour 23: Databricks = 287,895, Snowflake = 283,416, Difference = +4,479
-- This suggests our time boundary logic is excluding records that Databricks includes

SET VALIDATION_DATE = '2025-09-06';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';

-- ==============================================================================
-- INVESTIGATION: WHAT TIME RECORDS ARE WE EXCLUDING?
-- ==============================================================================

-- Check records in hour 23 that might be getting filtered
SELECT 
    'HOUR_23_DETAILED_ANALYSIS' AS analysis_type,
    HOUR(transaction_date) AS hour_of_day,
    MINUTE(transaction_date) AS minute_of_hour,
    COUNT(*) AS transaction_count,
    MIN(transaction_date) AS earliest_in_minute,
    MAX(transaction_date) AS latest_in_minute
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND HOUR(transaction_date) = 23
  AND MINUTE(transaction_date) >= 30  -- Focus on 23:30-23:59
GROUP BY HOUR(transaction_date), MINUTE(transaction_date)
ORDER BY minute_of_hour;

-- ==============================================================================
-- CHECK OUR CURRENT TIME FILTERING LOGIC
-- ==============================================================================

-- Current logic: transaction_date < CURRENT_DATE() (excludes 2025-09-07 00:00:00)
-- But what about records very close to midnight?

SELECT 
    'TIME_BOUNDARY_CHECK' AS analysis_type,
    'Records in last minute of Sept 6' AS description,
    COUNT(*) AS records_in_last_minute
FROM IDENTIFIER($SOURCE_TABLE)
WHERE transaction_date >= '2025-09-06 23:59:00'
  AND transaction_date < '2025-09-07 00:00:00';

-- Records that might be getting excluded by our boundary logic
SELECT 
    'POTENTIAL_EXCLUDED_RECORDS' AS analysis_type,
    'Records that might be filtered by our time logic' AS description,
    COUNT(*) AS potentially_excluded_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE transaction_date >= '2025-09-06 23:30:00'
  AND transaction_date < '2025-09-07 00:00:00'
  AND (
    -- Check if any of our other filters are excluding these
    transaction_main_id IS NULL OR 
    transaction_date IS NULL OR
    LOWER(TRIM(multi_client_name)) IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
  );

-- ==============================================================================
-- DATABRICKS COMPARISON: EXACT HOUR 23 MINUTE BREAKDOWN
-- ==============================================================================

-- Let's see exactly what we have vs what Databricks has in hour 23
SELECT 
    'SNOWFLAKE_HOUR_23_BREAKDOWN' AS platform,
    HOUR(transaction_date) AS hour_of_day,
    MINUTE(transaction_date) AS minute_of_hour,
    COUNT(*) AS transaction_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND HOUR(transaction_date) = 23
GROUP BY HOUR(transaction_date), MINUTE(transaction_date)
ORDER BY minute_of_hour;

-- ==============================================================================
-- CORRECTED TIME BOUNDARY LOGIC TEST
-- ==============================================================================

-- Test what happens if we use EXACT Databricks time boundary logic
WITH corrected_time_boundary AS (
    SELECT *
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE transaction_date >= '2025-09-06 00:00:00'
      AND transaction_date <= '2025-09-06 23:59:59.999'  -- Include ALL of Sept 6
),

applied_filters AS (
    SELECT *
    FROM corrected_time_boundary
    WHERE transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
)

SELECT 
    'CORRECTED_TIME_BOUNDARY_TEST' AS test_type,
    COUNT(*) AS new_record_count,
    COUNT(*) - 10584798 AS difference_from_current,
    10589277 - COUNT(*) AS difference_from_databricks,
    CASE 
        WHEN COUNT(*) = 10589277 THEN '🎯 PERFECT MATCH!'
        WHEN ABS(COUNT(*) - 10589277) < 100 THEN '✅ VERY CLOSE'
        ELSE '❌ STILL INVESTIGATING'
    END AS result_status
FROM applied_filters
WHERE DATE(transaction_date) = $VALIDATION_DATE;
