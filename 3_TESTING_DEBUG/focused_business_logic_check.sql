-- ==============================================================================
-- FOCUSED BUSINESS LOGIC CHECK
-- Quick verification that the fixed business logic is working
-- ==============================================================================

SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. BUSINESS LOGIC SUMMARY - Should show non-zero values now
SELECT 
    'BUSINESS LOGIC RESULTS' AS check_type,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_count,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_count,
    COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) AS successful_auth_count,
    COUNT(CASE WHEN is_successful_frictionless = true THEN 1 END) AS frictionless_success_count,
    COUNT(CASE WHEN is_successful_challenge = true THEN 1 END) AS challenge_success_count
FROM IDENTIFIER($TARGET_TABLE);

-- 2. STATUS FLAGS CHECK - Should show true/false values, not all NULL
SELECT 
    'STATUS FLAGS CHECK' AS check_type,
    transaction_type,
    COUNT(*) AS total,
    COUNT(CASE WHEN auth_status = true THEN 1 END) AS auth_success,
    COUNT(CASE WHEN auth_status = false THEN 1 END) AS auth_fail,
    COUNT(CASE WHEN sale_status = true THEN 1 END) AS sale_success,
    COUNT(CASE WHEN sale_status = false THEN 1 END) AS sale_fail
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_type IN ('Auth', 'Sale')
GROUP BY transaction_type;

-- 3. SAMPLE WORKING RECORDS - Show actual business logic in action
SELECT 
    'SAMPLE WORKING RECORDS' AS check_type,
    transaction_main_id,
    transaction_type,
    transaction_result_id,
    auth_status,
    sale_status,
    is_approved,
    is_declined,
    authentication_flow,
    is_successful_authentication
FROM IDENTIFIER($TARGET_TABLE)
WHERE (auth_status IS NOT NULL OR sale_status IS NOT NULL)
   OR is_successful_authentication IS NOT NULL
LIMIT 10;

-- 4. QUICK SUCCESS/FAILURE BREAKDOWN
SELECT 
    'TRANSACTION OUTCOMES' AS check_type,
    CASE 
        WHEN is_approved = true THEN 'APPROVED'
        WHEN is_declined = true THEN 'DECLINED'
        WHEN is_successful_authentication = true THEN 'AUTH_SUCCESS'
        WHEN is_successful_authentication = false THEN 'AUTH_FAILED'
        ELSE 'OTHER'
    END AS outcome,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY outcome
ORDER BY count DESC;
