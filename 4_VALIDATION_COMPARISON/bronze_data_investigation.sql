-- ==============================================================================
-- BRONZE DATA INVESTIGATION - ROOT CAUSE ANALYSIS
-- Investigating why we're missing hour 23 minutes 30-59
-- ==============================================================================

-- FINDINGS:
-- 1. Hour 23 only goes to minute 29 (missing 30-59)
-- 2. Databricks has 287,895 in hour 23, we have 283,416 in bronze
-- 3. Time boundary test shows we have too much data when boundary is corrected

SET VALIDATION_DATE = '2025-09-06';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';

-- ==============================================================================
-- 1. COMPREHENSIVE HOUR 23 ANALYSIS
-- ==============================================================================

-- Check EXACT hour 23 distribution vs Databricks
SELECT 
    'HOUR_23_COMPLETE_BREAKDOWN' AS analysis_type,
    HOUR(transaction_date) AS hour_of_day,
    MINUTE(transaction_date) AS minute_of_hour,
    COUNT(*) AS snowflake_count,
    287895 / 60 AS expected_avg_per_minute,  -- If evenly distributed
    COUNT(*) - (287895 / 60) AS difference_from_expected
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND HOUR(transaction_date) = 23
GROUP BY HOUR(transaction_date), MINUTE(transaction_date)
ORDER BY minute_of_hour;

-- ==============================================================================
-- 2. MISSING MINUTES INVESTIGATION
-- ==============================================================================

-- Check what minutes we're completely missing in hour 23
WITH all_minutes AS (
    SELECT 
        23 AS hour_val,
        seq4() AS minute_val
    FROM TABLE(GENERATOR(ROWCOUNT => 60))
    WHERE seq4() < 60
),
actual_data AS (
    SELECT 
        HOUR(transaction_date) AS hour_of_day,
        MINUTE(transaction_date) AS minute_of_hour,
        COUNT(*) AS transaction_count
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
      AND HOUR(transaction_date) = 23
    GROUP BY HOUR(transaction_date), MINUTE(transaction_date)
)

SELECT 
    'MISSING_MINUTES_ANALYSIS' AS analysis_type,
    all_minutes.minute_val AS minute_of_hour,
    COALESCE(actual_data.transaction_count, 0) AS actual_count,
    CASE 
        WHEN actual_data.transaction_count IS NULL THEN 'MISSING'
        ELSE 'PRESENT'
    END AS status
FROM all_minutes
LEFT JOIN actual_data ON all_minutes.minute_val = actual_data.minute_of_hour
ORDER BY all_minutes.minute_val;

-- ==============================================================================
-- 3. BRONZE TABLE DATA COMPLETENESS CHECK
-- ==============================================================================

-- Check if we have complete data for the full day
SELECT 
    'DAILY_COMPLETENESS_CHECK' AS analysis_type,
    DATE(transaction_date) AS transaction_date,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT HOUR(transaction_date)) AS unique_hours_present,
    MAX(HOUR(transaction_date)) AS latest_hour,
    MAX(MINUTE(transaction_date)) AS latest_minute_overall
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY DATE(transaction_date);

-- ==============================================================================
-- 4. COMPARE WITH DATABRICKS HOUR DISTRIBUTION
-- ==============================================================================

-- Databricks reported: Hour 0 = 578,470, Hour 23 = 287,895
-- Let's see our full hour breakdown
SELECT 
    'HOUR_DISTRIBUTION_COMPARISON' AS analysis_type,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS snowflake_count,
    CASE 
        WHEN HOUR(transaction_date) = 0 THEN 578470
        WHEN HOUR(transaction_date) = 23 THEN 287895
        ELSE NULL
    END AS databricks_count,
    CASE 
        WHEN HOUR(transaction_date) = 0 THEN COUNT(*) - 578470
        WHEN HOUR(transaction_date) = 23 THEN COUNT(*) - 287895
        ELSE NULL
    END AS difference
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;

-- ==============================================================================
-- 5. BRONZE DATA SOURCE INVESTIGATION
-- ==============================================================================

-- Check when our bronze data was loaded and if it's complete
SELECT 
    'BRONZE_DATA_SOURCE_CHECK' AS analysis_type,
    'Check bronze table metadata' AS description,
    COUNT(*) AS total_bronze_records,
    MIN(inserted_at) AS earliest_insert,
    MAX(inserted_at) AS latest_insert,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- 6. ETL FILTERING IMPACT ANALYSIS
-- ==============================================================================

-- Check exactly what our ETL filters are doing
WITH bronze_sept6 AS (
    SELECT COUNT(*) AS bronze_total
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
),
after_basic_filters AS (
    SELECT COUNT(*) AS after_basic
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
),
after_test_client_filter AS (
    SELECT COUNT(*) AS after_test_filter
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
after_deduplication AS (
    SELECT COUNT(*) AS after_dedup
    FROM (
        SELECT *
        FROM IDENTIFIER($SOURCE_TABLE)
        WHERE DATE(transaction_date) = $VALIDATION_DATE
          AND transaction_main_id IS NOT NULL 
          AND transaction_date IS NOT NULL
          AND LOWER(TRIM(multi_client_name)) NOT IN (
            'test multi', 
            'davidh test2 multi', 
            'ice demo multi', 
            'monitoring client pod2 multi'
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) = 1
    )
)

SELECT 
    'ETL_FILTERING_IMPACT' AS analysis_type,
    bronze_sept6.bronze_total,
    after_basic_filters.after_basic,
    bronze_sept6.bronze_total - after_basic_filters.after_basic AS basic_filter_removed,
    after_test_client_filter.after_test_filter,
    after_basic_filters.after_basic - after_test_client_filter.after_test_filter AS test_client_removed,
    after_deduplication.after_dedup AS final_count,
    after_test_client_filter.after_test_filter - after_deduplication.after_dedup AS dedup_removed,
    10589277 - after_deduplication.after_dedup AS still_missing
FROM bronze_sept6, after_basic_filters, after_test_client_filter, after_deduplication;
