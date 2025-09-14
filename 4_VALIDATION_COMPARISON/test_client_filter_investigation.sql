-- ==============================================================================
-- FOCUSED INVESTIGATION: Test Client Filter Issue
-- Check if the $TARGET_TABLE condition is working properly
-- ==============================================================================

SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET VALIDATION_DATE = '2025-09-06';

-- 1. CHECK: What is the actual value of $TARGET_TABLE and does the LIKE work?
SELECT 
    'TARGET_TABLE_CHECK' AS check_type,
    $TARGET_TABLE AS target_table_value,
    CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN 'MATCHES' ELSE 'NO_MATCH' END AS like_result;

-- 2. CHECK: Count with and without test client filter
SELECT 
    'TEST_CLIENT_FILTER_IMPACT' AS check_type,
    COUNT(*) AS total_with_test_clients,
    COUNT(CASE WHEN multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS total_without_test_clients,
    COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_client_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- 3. CHECK: What test clients are actually in the data?
SELECT 
    'ACTUAL_TEST_CLIENTS' AS check_type,
    multi_client_name,
    COUNT(*) AS count_in_silver
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
GROUP BY multi_client_name
ORDER BY count_in_silver DESC;

-- 4. CRITICAL: Check if the filter condition is actually being applied
-- This simulates what should happen vs what might be happening
SELECT 
    'FILTER_LOGIC_TEST' AS check_type,
    'Should Filter Test Clients' AS expected_behavior,
    CASE 
        WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN 'YES - Filter Applied'
        ELSE 'NO - Filter Skipped'
    END AS actual_behavior,
    COUNT(*) AS current_silver_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- 5. HYPOTHESIS: Maybe Databricks doesn't include test clients at all
-- Let's see what clients Snowflake has that might not be in Databricks
SELECT 
    'CLIENT_COMPARISON' AS check_type,
    multi_client_name,
    COUNT(*) AS count_in_snowflake_silver,
    CASE 
        WHEN multi_client_name LIKE '%test%' THEN 'TEST_LIKE'
        WHEN multi_client_name LIKE '%demo%' THEN 'DEMO_LIKE' 
        WHEN multi_client_name LIKE '%monitoring%' THEN 'MONITORING_LIKE'
        WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 'EXACT_TEST_MATCH'
        ELSE 'PRODUCTION_CLIENT'
    END AS client_category
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY multi_client_name
HAVING COUNT(*) > 100  -- Only show clients with significant volume
ORDER BY count_in_snowflake_silver DESC;
