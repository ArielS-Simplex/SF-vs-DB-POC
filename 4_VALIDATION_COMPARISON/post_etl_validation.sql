-- ==============================================================================
-- POST-ETL VALIDATION - CHECK IF DATABRICKS PARITY ACHIEVED
-- Run this after executing exact_databricks_parity_etl.sql
-- ==============================================================================

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- ==============================================================================
-- 1. FINAL ROW COUNT CHECK
-- Compare with Databricks target of 10,589,277 rows
-- ==============================================================================

SELECT 
    'FINAL_ROW_COUNT_COMPARISON' AS check_type,
    COUNT(*) AS snowflake_final_count,
    10589277 AS databricks_target_count,
    COUNT(*) - 10589277 AS difference,
    CASE 
        WHEN COUNT(*) = 10589277 THEN '✅ PERFECT MATCH!'
        WHEN ABS(COUNT(*) - 10589277) < 100 THEN '✅ VERY CLOSE (< 100 difference)'
        WHEN ABS(COUNT(*) - 10589277) < 1000 THEN '⚠️ CLOSE (< 1000 difference)'
        ELSE '❌ NEEDS INVESTIGATION'
    END AS result_status
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- 2. VALIDATION BY DATE RANGE
-- Check all dates to ensure we have the right data
-- ==============================================================================

SELECT 
    'DATE_RANGE_VALIDATION' AS check_type,
    DATE(transaction_date) AS transaction_date,
    COUNT(*) AS daily_count,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY DATE(transaction_date)
ORDER BY transaction_date;

-- ==============================================================================
-- 3. TEST CLIENT FILTER VALIDATION
-- Ensure no test clients remain in the data
-- ==============================================================================

SELECT 
    'TEST_CLIENT_VALIDATION' AS check_type,
    multi_client_name,
    COUNT(*) AS remaining_count,
    'These should be 0 if filtering worked correctly' AS note
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND LOWER(TRIM(multi_client_name)) IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  )
GROUP BY multi_client_name;

-- ==============================================================================
-- 4. DATABRICKS PARITY CHECK - BOOLEAN FIELDS
-- Verify boolean normalization worked correctly
-- ==============================================================================

SELECT 
    'BOOLEAN_NORMALIZATION_CHECK' AS check_type,
    'All boolean fields should now be true/false/null only' AS description,
    SUM(CASE WHEN is_void NOT IN (true, false) AND is_void IS NOT NULL THEN 1 ELSE 0 END) AS invalid_is_void,
    SUM(CASE WHEN liability_shift NOT IN (true, false) AND liability_shift IS NOT NULL THEN 1 ELSE 0 END) AS invalid_liability_shift,
    SUM(CASE WHEN is_sale_3d NOT IN (true, false) AND is_sale_3d IS NOT NULL THEN 1 ELSE 0 END) AS invalid_is_sale_3d,
    SUM(CASE WHEN manage_3d_decision NOT IN (true, false) AND manage_3d_decision IS NOT NULL THEN 1 ELSE 0 END) AS invalid_manage_3d_decision
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- 5. FORCED NULL COLUMNS CHECK
-- Verify Databricks forced-null columns are actually NULL
-- ==============================================================================

SELECT 
    'FORCED_NULL_COLUMNS_CHECK' AS check_type,
    'These should all be 0 if Databricks parity is achieved' AS description,
    SUM(CASE WHEN user_agent_3d IS NOT NULL THEN 1 ELSE 0 END) AS non_null_user_agent_3d,
    SUM(CASE WHEN authentication_request IS NOT NULL THEN 1 ELSE 0 END) AS non_null_auth_request,
    SUM(CASE WHEN authentication_response IS NOT NULL THEN 1 ELSE 0 END) AS non_null_auth_response,
    SUM(CASE WHEN authorization_req_duration IS NOT NULL THEN 1 ELSE 0 END) AS non_null_auth_duration
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- 6. ADDITIONAL DATABRICKS COLUMNS CHECK
-- Verify we created the additional columns that Databricks has
-- ==============================================================================

SELECT 
    'ADDITIONAL_COLUMNS_CHECK' AS check_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_sale_3d_auth_3d IS NOT NULL THEN 1 ELSE 0 END) AS records_with_is_sale_3d_auth_3d,
    SUM(CASE WHEN manage_3d_decision_auth_3d IS NOT NULL THEN 1 ELSE 0 END) AS records_with_manage_3d_decision_auth_3d,
    SUM(CASE WHEN transaction_type = 'Auth3D' THEN 1 ELSE 0 END) AS auth3d_transactions,
    'is_sale_3d_auth_3d and manage_3d_decision_auth_3d should equal auth3d_transactions' AS validation_note
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- 7. BUSINESS LOGIC VALIDATION
-- Check transaction status flags are working correctly
-- ==============================================================================

SELECT 
    'BUSINESS_LOGIC_VALIDATION' AS check_type,
    transaction_type,
    transaction_result_id,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN transaction_result_id = '1006' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN transaction_result_id = '1008' THEN 1 ELSE 0 END) AS declined_count,
    SUM(CASE WHEN 
        (transaction_type = 'Sale' AND sale_status = true) OR
        (transaction_type = 'Auth' AND auth_status = true) OR
        (transaction_type = 'Auth3D' AND auth_3d_status = true) OR
        (transaction_type = 'InitAuth3D' AND init_status = true) OR
        (transaction_type = 'Settle' AND settle_status = true) OR
        (transaction_type = 'verify_auth_3d' AND verify_auth_3d_status = true)
    THEN 1 ELSE 0 END) AS status_flag_true_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
GROUP BY transaction_type, transaction_result_id
ORDER BY transaction_type, transaction_result_id;

-- ==============================================================================
-- 8. FINAL SUMMARY COMPARISON
-- Compare key metrics with our previous results
-- ==============================================================================

SELECT 
    'FINAL_SUMMARY_COMPARISON' AS check_type,
    'Previous Snowflake count: 10,584,798' AS previous_result,
    'Databricks target: 10,589,277' AS databricks_target,
    COUNT(*) AS new_snowflake_count,
    COUNT(*) - 10589277 AS difference_from_databricks,
    COUNT(*) - 10584798 AS improvement_from_previous,
    CASE 
        WHEN COUNT(*) = 10589277 THEN '🎯 PERFECT DATABRICKS PARITY ACHIEVED!'
        WHEN ABS(COUNT(*) - 10589277) < ABS(10584798 - 10589277) THEN '📈 IMPROVEMENT - CLOSER TO DATABRICKS'
        ELSE '📉 CHECK REQUIRED - MAY NEED FURTHER ADJUSTMENTS'
    END AS final_assessment
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;
