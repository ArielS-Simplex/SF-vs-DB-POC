-- ==============================================================================
-- BUSINESS LOGIC DIAGNOSTIC QUERIES
-- Debug why approval/decline logic is showing 0 results
-- ==============================================================================

SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. CHECK TRANSACTION_RESULT_ID VALUES (key for approval logic)
SELECT 
    'TRANSACTION_RESULT_ID ANALYSIS' AS analysis_type,
    transaction_result_id,
    COUNT(*) AS count,
    COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) AS success_1006_count,
    COUNT(CASE WHEN transaction_result_id = '1008' THEN 1 END) AS decline_1008_count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY transaction_result_id
ORDER BY count DESC
LIMIT 10;

-- 2. CHECK STATUS FLAGS (should be true/false, not NULL)
SELECT 
    'STATUS FLAGS ANALYSIS' AS analysis_type,
    transaction_type,
    COUNT(*) AS total,
    COUNT(auth_status) AS auth_status_not_null,
    COUNT(sale_status) AS sale_status_not_null,
    COUNT(CASE WHEN auth_status = true THEN 1 END) AS auth_true,
    COUNT(CASE WHEN sale_status = true THEN 1 END) AS sale_true
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_type IN ('auth', 'sale')
GROUP BY transaction_type;

-- 3. CHECK AUTHENTICATION FLOW VALUES
SELECT 
    'AUTHENTICATION FLOW ANALYSIS' AS analysis_type,
    authentication_flow,
    COUNT(*) AS count,
    COUNT(CASE WHEN status = '40' THEN 1 END) AS status_40_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE authentication_flow IS NOT NULL
GROUP BY authentication_flow
ORDER BY count DESC;

-- 4. CHECK 3D FLOW STATUS VALUES
SELECT 
    '3D FLOW STATUS ANALYSIS' AS analysis_type,
    threed_flow_status,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
WHERE threed_flow_status IS NOT NULL
GROUP BY threed_flow_status
ORDER BY count DESC;

-- 5. SAMPLE RECORDS WITH KEY FIELDS
SELECT 
    'SAMPLE RECORDS FOR DEBUGGING' AS analysis_type,
    transaction_main_id,
    transaction_type,
    transaction_result_id,
    authentication_flow,
    threed_flow_status,
    status,
    auth_status,
    sale_status,
    is_approved,
    is_successful_authentication
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_type IN ('auth', 'sale')
LIMIT 5;

-- 6. CHECK IF TARGET_TABLE VARIABLE IS WORKING
SELECT 
    'TARGET_TABLE CHECK' AS analysis_type,
    $TARGET_TABLE AS target_table_var,
    CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN 'MATCH' ELSE 'NO_MATCH' END AS table_name_check;

-- 7. RAW BUSINESS LOGIC TEST
SELECT 
    'RAW BUSINESS LOGIC TEST' AS analysis_type,
    transaction_type,
    transaction_result_id,
    CASE WHEN transaction_result_id = '1006' THEN true ELSE false END AS should_be_success,
    auth_status,
    sale_status,
    is_approved
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_type IN ('auth', 'sale') AND transaction_result_id IN ('1006', '1008')
LIMIT 10;
