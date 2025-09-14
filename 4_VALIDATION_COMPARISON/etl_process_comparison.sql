-- ==============================================================================
-- ETL PROCESS COMPARISON: What does Databricks have that Snowflake doesn't?
-- Analyzing potential differences in filtering, functions, and business rules
-- ==============================================================================

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. CHECK: Does Databricks filter out specific transaction_result_ids?
SELECT 
    'SNOWFLAKE_RESULT_ID_ANALYSIS' AS check_type,
    transaction_result_id,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY transaction_result_id
ORDER BY count_in_snowflake DESC;

-- 2. CHECK: Does Databricks filter out specific final_transaction_status values?
SELECT 
    'SNOWFLAKE_FINAL_STATUS_ANALYSIS' AS check_type,
    final_transaction_status,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY final_transaction_status
ORDER BY count_in_snowflake DESC;

-- 3. CHECK: Does Databricks have additional client filtering beyond test clients?
SELECT 
    'SNOWFLAKE_CLIENT_ANALYSIS' AS check_type,
    multi_client_name,
    COUNT(*) AS count_in_snowflake,
    CASE 
        WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') 
        THEN 'TEST_CLIENT' 
        ELSE 'PRODUCTION_CLIENT' 
    END AS client_type
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY multi_client_name
ORDER BY count_in_snowflake DESC;

-- 4. CHECK: Does Databricks filter based on amount ranges?
SELECT 
    'SNOWFLAKE_AMOUNT_ANALYSIS' AS check_type,
    CASE 
        WHEN amount_in_usd IS NULL THEN 'NULL_AMOUNT'
        WHEN amount_in_usd <= 0 THEN 'ZERO_OR_NEGATIVE'
        WHEN amount_in_usd > 0 AND amount_in_usd <= 1 THEN 'MICRO_AMOUNT'
        WHEN amount_in_usd > 10000 THEN 'LARGE_AMOUNT'
        ELSE 'NORMAL_AMOUNT'
    END AS amount_category,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY amount_category
ORDER BY count_in_snowflake DESC;

-- 5. CHECK: Does Databricks filter based on specific processor/gateway combinations?
SELECT 
    'SNOWFLAKE_PROCESSOR_ANALYSIS' AS check_type,
    processor_name,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND processor_name IS NOT NULL
GROUP BY processor_name
ORDER BY count_in_snowflake DESC
LIMIT 20;

-- 6. CHECK: Does Databricks filter based on country/region restrictions?
SELECT 
    'SNOWFLAKE_COUNTRY_ANALYSIS' AS check_type,
    bin_country,
    region,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND bin_country IS NOT NULL
GROUP BY bin_country, region
ORDER BY count_in_snowflake DESC
LIMIT 20;

-- 7. CHECK: Does Databricks have stricter data validation rules?
SELECT 
    'SNOWFLAKE_DATA_VALIDATION' AS check_type,
    CASE 
        WHEN transaction_main_id IS NULL OR transaction_main_id = '' THEN 'INVALID_ID'
        WHEN transaction_date IS NULL THEN 'INVALID_DATE'
        WHEN transaction_type IS NULL OR transaction_type = '' THEN 'INVALID_TYPE'
        WHEN transaction_result_id IS NULL OR transaction_result_id = '' THEN 'INVALID_RESULT'
        WHEN multi_client_id IS NULL OR multi_client_id = '' THEN 'INVALID_CLIENT'
        ELSE 'VALID_RECORD'
    END AS validation_status,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY validation_status
ORDER BY count_in_snowflake DESC;

-- 8. CHECK: Does Databricks filter based on specific time windows within the day?
SELECT 
    'SNOWFLAKE_TIME_ANALYSIS' AS check_type,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS count_in_snowflake
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;

-- 9. CRITICAL CHECK: What's the actual count from bronze source for Sep 6?
SELECT 
    'SNOWFLAKE_BRONZE_SOURCE_CHECK' AS check_type,
    'Raw Bronze Count' AS description,
    COUNT(*) AS total_bronze_sep6,
    COUNT(CASE WHEN transaction_main_id IS NOT NULL AND transaction_date IS NOT NULL THEN 1 END) AS valid_records,
    COUNT(CASE WHEN multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS non_test_clients
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = $VALIDATION_DATE;
