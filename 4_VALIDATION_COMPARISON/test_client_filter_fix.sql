-- ==============================================================================
-- TEST CLIENT FILTER FIX INVESTIGATION
-- Check exact test client names and fix the filter
-- ==============================================================================

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. Find all clients that look like test clients
SELECT 
    'POTENTIAL_TEST_CLIENTS' AS check_type,
    multi_client_name,
    COUNT(*) AS transaction_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (LOWER(multi_client_name) LIKE '%test%' 
       OR LOWER(multi_client_name) LIKE '%monitoring%'
       OR LOWER(multi_client_name) LIKE '%demo%'
       OR LOWER(multi_client_name) LIKE '%david%')
GROUP BY multi_client_name
ORDER BY transaction_count DESC;

-- 2. Check exact counts for suspected test clients
SELECT 
    'SUSPECTED_TEST_CLIENT_COUNTS' AS check_type,
    multi_client_name,
    COUNT(*) AS transaction_count,
    CASE 
        WHEN LOWER(multi_client_name) LIKE '%monitoring client pod2%' THEN 'MONITORING_POD2'
        WHEN LOWER(multi_client_name) LIKE '%test multi%' THEN 'TEST_MULTI'
        WHEN LOWER(multi_client_name) LIKE '%davidh%' THEN 'DAVIDH_TEST'
        WHEN LOWER(multi_client_name) LIKE '%ice demo%' THEN 'ICE_DEMO'
        ELSE 'OTHER'
    END AS test_client_type
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND multi_client_name IN (
    'Monitoring Client POD2 Multi',
    'monitoring client pod2 multi', 
    'test multi',
    'davidh test2 multi',
    'ice demo multi'
  )
GROUP BY multi_client_name
ORDER BY transaction_count DESC;

-- 3. Calculate what the row count would be if we properly filter test clients
SELECT 
    'CORRECTED_ROW_COUNT_ESTIMATE' AS check_type,
    COUNT(*) AS current_total,
    COUNT(*) - COUNT(CASE WHEN LOWER(multi_client_name) LIKE '%monitoring client pod2%' THEN 1 END) AS after_removing_monitoring,
    COUNT(CASE WHEN LOWER(multi_client_name) LIKE '%monitoring client pod2%' THEN 1 END) AS monitoring_client_count,
    (10611400 - COUNT(CASE WHEN LOWER(multi_client_name) LIKE '%monitoring client pod2%' THEN 1 END)) AS estimated_corrected_total
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;
