-- ==============================================================================
-- DATABRICKS INVESTIGATION QUERIES - September 6, 2025
-- Run these in Databricks to compare with Snowflake findings
-- ==============================================================================

-- STEP 1: Check test client filtering in Databricks
SELECT 
    'STEP1_TEST_CLIENT_FILTER_DB' AS check_type,
    'Databricks Silver' AS filter_stage,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_client_rows,
    COUNT(CASE WHEN multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS non_test_client_rows
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';

-- STEP 2: Check deduplication in Databricks  
SELECT 
    'STEP2_DEDUPLICATION_CHECK_DB' AS check_type,
    COUNT(*) AS total_records_silver,
    COUNT(DISTINCT CONCAT(transaction_main_id, '|', CAST(transaction_date AS STRING))) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT CONCAT(transaction_main_id, '|', CAST(transaction_date AS STRING))) AS duplicate_count
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';

-- STEP 3: Check data quality in Databricks
SELECT 
    'STEP3_DATA_QUALITY_FILTER_DB' AS check_type,
    data_quality_flag,
    date_quality_flag,
    COUNT(*) AS record_count
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY data_quality_flag, date_quality_flag
ORDER BY record_count DESC;

-- STEP 4: Check time range in Databricks
SELECT 
    'STEP5_TIME_RANGE_CHECK_DB' AS check_type,
    MIN(transaction_date) AS earliest_time,
    MAX(transaction_date) AS latest_time,
    COUNT(CASE WHEN DATE_FORMAT(transaction_date, 'HH:mm:ss') < '00:01:00' THEN 1 END) AS very_early_records,
    COUNT(CASE WHEN DATE_FORMAT(transaction_date, 'HH:mm:ss') > '23:58:00' THEN 1 END) AS very_late_records
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';

-- STEP 5: Check transaction types in Databricks
SELECT 
    'STEP6_TRANSACTION_TYPE_CHECK_DB' AS check_type,
    transaction_type,
    COUNT(*) AS count_in_silver
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY transaction_type
ORDER BY count_in_silver DESC;

-- STEP 6: Check if Databricks has any additional filtering
-- Look for any WHERE conditions that might filter out records
SELECT 
    'STEP7_MISSING_RECORDS_CHECK_DB' AS check_type,
    'Check for specific patterns' AS description,
    COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_main_id,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_date,
    COUNT(CASE WHEN transaction_result_id NOT IN ('1006', '1008') THEN 1 END) AS unusual_result_ids
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';
