-- ==============================================================================
-- REMAINING 4,479 ROW DIFFERENCE INVESTIGATION
-- Deep dive into the final discrepancy between Snowflake and Databricks
-- ==============================================================================

-- CURRENT STATE:
-- Snowflake: 10,584,798 rows (Sep 6, 2025)
-- Databricks: 10,589,277 rows (Sep 6, 2025) 
-- Difference: 4,479 rows (Databricks has MORE)

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. DEDUPLICATION INVESTIGATION
-- Check if our deduplication logic matches Databricks exactly
SELECT 
    'DEDUPLICATION_ANALYSIS' AS check_type,
    COUNT(*) AS total_records_after_dedup,
    COUNT(DISTINCT CONCAT(transaction_main_id, '|', DATE(transaction_date))) AS unique_id_date_combinations,
    COUNT(*) - COUNT(DISTINCT CONCAT(transaction_main_id, '|', DATE(transaction_date))) AS potential_duplicates_kept,
    COUNT(DISTINCT transaction_main_id) AS unique_transaction_ids,
    COUNT(DISTINCT DATE(transaction_date)) AS unique_dates_processed
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- 2. TIME BOUNDARY ANALYSIS
-- Check if there are records at the very edge of the day that might be handled differently
SELECT 
    'TIME_BOUNDARY_ANALYSIS' AS check_type,
    DATE(transaction_date) AS transaction_date,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    MIN(transaction_date) AS earliest_in_hour,
    MAX(transaction_date) AS latest_in_hour
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (HOUR(transaction_date) = 0 OR HOUR(transaction_date) = 23)  -- Focus on day boundaries
GROUP BY DATE(transaction_date), HOUR(transaction_date)
ORDER BY hour_of_day;

-- 3. TRANSACTION TYPE DETAILED BREAKDOWN
-- Compare exact transaction type counts that Databricks should match
SELECT 
    'TRANSACTION_TYPE_DETAILED' AS check_type,
    transaction_type,
    transaction_result_id,
    final_transaction_status,
    COUNT(*) AS count_in_snowflake,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 4) AS percentage
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY transaction_type, transaction_result_id, final_transaction_status
ORDER BY count_in_snowflake DESC;

-- 4. DATA QUALITY FLAGS ANALYSIS
-- Check if any records have unexpected data quality issues
SELECT 
    'DATA_QUALITY_ANALYSIS' AS check_type,
    data_quality_flag,
    date_quality_flag,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 4) AS percentage
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY data_quality_flag, date_quality_flag
ORDER BY record_count DESC;

-- 5. CLIENT FILTERING EDGE CASES
-- Check for any clients that might be filtered differently
SELECT 
    'CLIENT_EDGE_CASES' AS check_type,
    multi_client_name,
    COUNT(*) AS transaction_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (LOWER(multi_client_name) LIKE '%test%' 
       OR LOWER(multi_client_name) LIKE '%demo%'
       OR LOWER(multi_client_name) LIKE '%monitor%'
       OR LOWER(multi_client_name) LIKE '%staging%'
       OR LOWER(multi_client_name) LIKE '%dev%')
GROUP BY multi_client_name
ORDER BY transaction_count DESC;

-- 6. INSERTED_AT TIMESTAMP ANALYSIS
-- Check if there are timing differences in when records were inserted
SELECT 
    'INSERTION_TIMING_ANALYSIS' AS check_type,
    DATE(inserted_at) AS insertion_date,
    HOUR(inserted_at) AS insertion_hour,
    COUNT(*) AS records_inserted,
    MIN(inserted_at) AS earliest_insertion,
    MAX(inserted_at) AS latest_insertion
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY DATE(inserted_at), HOUR(inserted_at)
ORDER BY insertion_date, insertion_hour;

-- 7. POTENTIAL MISSING RECORDS PATTERN
-- Look for patterns in what might be missing
SELECT 
    'MISSING_RECORDS_PATTERN' AS check_type,
    processor_name,
    currency_code,
    bin_country,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY processor_name, currency_code, bin_country
HAVING COUNT(*) < 10  -- Focus on low-volume combinations that might be edge cases
ORDER BY count_in_snowflake ASC
LIMIT 20;
