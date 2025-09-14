-- ==============================================================================
-- STEP-BY-STEP INVESTIGATION: Why Row Count Difference?
-- Snowflake: 10,611,400 vs Databricks: 10,589,277 (Difference: 22,123 rows)
-- ==============================================================================

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- STEP 1: Check if test client filtering is applied consistently
SELECT 
    'STEP1_TEST_CLIENT_FILTER' AS check_type,
    'Before Filter' AS filter_stage,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_client_rows,
    COUNT(CASE WHEN multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS non_test_client_rows
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE

UNION ALL

-- Check what's in the bronze source for the same date
SELECT 
    'STEP1_SOURCE_BRONZE' AS check_type,
    'Source Data' AS filter_stage,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_client_rows,
    COUNT(CASE WHEN multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS non_test_client_rows
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- STEP 2: Check for deduplication differences
SELECT 
    'STEP2_DEDUPLICATION_CHECK' AS check_type,
    COUNT(*) AS total_records_silver,
    COUNT(DISTINCT CONCAT(transaction_main_id, '|', transaction_date)) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT CONCAT(transaction_main_id, '|', transaction_date)) AS duplicate_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- STEP 3: Check data quality filtering
SELECT 
    'STEP3_DATA_QUALITY_FILTER' AS check_type,
    data_quality_flag,
    date_quality_flag,
    COUNT(*) AS record_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY data_quality_flag, date_quality_flag
ORDER BY record_count DESC;

-- STEP 4: Check if there are any NULL transaction_main_id or transaction_date in source
SELECT 
    'STEP4_NULL_CHECK_BRONZE' AS check_type,
    COUNT(*) AS total_bronze_records,
    COUNT(CASE WHEN transaction_main_id IS NULL OR transaction_main_id = '' THEN 1 END) AS null_transaction_id,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_transaction_date,
    COUNT(CASE WHEN (transaction_main_id IS NULL OR transaction_main_id = '') OR transaction_date IS NULL THEN 1 END) AS would_be_filtered_out
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = $VALIDATION_DATE
   OR (transaction_date IS NULL AND DATE(inserted_at) = $VALIDATION_DATE);

-- STEP 5: Check time range differences (maybe timezone issues?)
SELECT 
    'STEP5_TIME_RANGE_CHECK' AS check_type,
    MIN(transaction_date) AS earliest_time,
    MAX(transaction_date) AS latest_time,
    COUNT(CASE WHEN TIME(transaction_date) < '00:01:00' THEN 1 END) AS very_early_records,
    COUNT(CASE WHEN TIME(transaction_date) > '23:58:00' THEN 1 END) AS very_late_records
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- STEP 6: Check for any specific transaction types that might be filtered differently
SELECT 
    'STEP6_TRANSACTION_TYPE_CHECK' AS check_type,
    transaction_type,
    COUNT(*) AS count_in_silver
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY transaction_type
ORDER BY count_in_silver DESC;
