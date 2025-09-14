-- ==============================================================================
-- DEBUG BOOLEAN LOGIC - Simple Raw Data Analysis
-- Testing what's actually in the bronze data for Sept 5, 2025
-- ==============================================================================

-- First: Check raw transaction_type and transaction_result_id distribution
SELECT 
    'RAW_TRANSACTION_DATA' AS test_type,
    transaction_type,
    transaction_result_id,
    COUNT(*) AS record_count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
GROUP BY transaction_type, transaction_result_id
ORDER BY record_count DESC
LIMIT 20;

-- Second: Check raw 3DS flow status distribution
SELECT 
    'RAW_3DS_DATA' AS test_type,
    threed_flow_status,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
GROUP BY threed_flow_status
ORDER BY record_count DESC;

-- Third: Test the exact CASE logic on raw data
SELECT 
    'CASE_LOGIC_TEST' AS test_type,
    -- Test is_approved logic directly on raw data (FIXED CASE SENSITIVITY)
    CASE 
        WHEN (UPPER(transaction_type) = 'AUTH' AND transaction_result_id = '1006') 
          OR (UPPER(transaction_type) = 'SALE' AND transaction_result_id = '1006') THEN 'TRUE'
        WHEN (UPPER(transaction_type) = 'AUTH' AND transaction_result_id != '1006') 
          OR (UPPER(transaction_type) = 'SALE' AND transaction_result_id != '1006') THEN 'FALSE'
        ELSE 'NULL'
    END AS is_approved_test,
    -- Test is_successful_challenge logic directly on raw data (FIXED CASE SENSITIVITY)
    CASE 
        WHEN UPPER(threed_flow_status) = '3D_SUCCESS' THEN 'TRUE'
        WHEN UPPER(threed_flow_status) IN ('3D_FAILURE', '3D_WASNT_COMPLETED') THEN 'FALSE'
        ELSE 'NULL'
    END AS is_successful_challenge_test,
    COUNT(*) AS record_count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
GROUP BY 
    CASE 
        WHEN (UPPER(transaction_type) = 'AUTH' AND transaction_result_id = '1006') 
          OR (UPPER(transaction_type) = 'SALE' AND transaction_result_id = '1006') THEN 'TRUE'
        WHEN (UPPER(transaction_type) = 'AUTH' AND transaction_result_id != '1006') 
          OR (UPPER(transaction_type) = 'SALE' AND transaction_result_id != '1006') THEN 'FALSE'
        ELSE 'NULL'
    END,
    CASE 
        WHEN UPPER(threed_flow_status) = '3D_SUCCESS' THEN 'TRUE'
        WHEN UPPER(threed_flow_status) IN ('3D_FAILURE', '3D_WASNT_COMPLETED') THEN 'FALSE'
        ELSE 'NULL'
    END
ORDER BY record_count DESC;

-- Fourth: Sample actual raw values to see what we're working with
SELECT 
    'SAMPLE_RAW_DATA' AS test_type,
    transaction_main_id,
    transaction_type,
    transaction_result_id,
    threed_flow_status,
    -- Show what the CASE logic would produce
    CASE 
        WHEN (transaction_type = 'auth' AND transaction_result_id = '1006') 
          OR (transaction_type = 'sale' AND transaction_result_id = '1006') THEN 'APPROVED'
        ELSE 'NOT_APPROVED'
    END AS approval_test
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
ORDER BY RANDOM()
LIMIT 10;