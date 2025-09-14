-- ================================================
-- Validation Script: 143-Column Parity Test
-- Tests Snowflake vs Databricks ETL Output
-- Date: 2025-09-14
-- ================================================

-- Step 1: Execute the complete Snowflake ETL
-- Run: snowflake/refactored_scripts/complete_bronze_to_silver_etl.sql first

-- Step 2: Schema Validation - Count columns
SELECT 'Column Count Check' AS test_type,
       COUNT(*) AS snowflake_columns
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_SILVER_V3'
  AND TABLE_CATALOG = 'POC';
-- Expected: 143 columns

-- Step 3: Row Count Validation 
SELECT 'Row Count Check' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT transaction_main_id) AS unique_transactions,
       MIN(transaction_date) AS min_date,
       MAX(transaction_date) AS max_date
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 4: Derived Column Validation - Status Flags
SELECT 'Status Flags Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_status_populated,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth_3d_status_populated,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_status_populated,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_status_populated,
       COUNT(CASE WHEN settle_status IS NOT NULL THEN 1 END) AS settle_status_populated,
       COUNT(CASE WHEN verify_auth_3d_status IS NOT NULL THEN 1 END) AS verify_auth_3d_status_populated
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 5: Conditional Copy Validation
SELECT 'Conditional Copies Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS auth3d_conditional_copies,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND manage_3d_decision_auth_3d IS NOT NULL THEN 1 END) AS auth3d_decision_copies,
       COUNT(CASE WHEN transaction_type != 'auth3d' AND is_sale_3d_auth_3d IS NULL THEN 1 END) AS non_auth3d_null_copies
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 6: 3D Secure Success Analysis
SELECT '3D Secure Analysis Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_successful_challenge IS NOT NULL THEN 1 END) AS challenge_populated,
       COUNT(CASE WHEN is_successful_exemption IS NOT NULL THEN 1 END) AS exemption_populated,
       COUNT(CASE WHEN is_successful_frictionless IS NOT NULL THEN 1 END) AS frictionless_populated,
       COUNT(CASE WHEN is_successful_authentication IS NOT NULL THEN 1 END) AS authentication_populated
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 7: High-Level Logic Validation
SELECT 'Approval/Decline Logic Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_approved = 'true' THEN 1 END) AS approved_count,
       COUNT(CASE WHEN is_approved = 'false' THEN 1 END) AS not_approved_count,
       COUNT(CASE WHEN is_declined = 'true' THEN 1 END) AS declined_count,
       COUNT(CASE WHEN is_declined = 'false' THEN 1 END) AS not_declined_count
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 8: Boolean Conversion Validation
SELECT 'Boolean Conversions Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_currency_converted = TRUE THEN 1 END) AS currency_converted_true,
       COUNT(CASE WHEN is_currency_converted = FALSE THEN 1 END) AS currency_converted_false,
       COUNT(CASE WHEN is_eea = TRUE THEN 1 END) AS eea_true,
       COUNT(CASE WHEN is_eea = FALSE THEN 1 END) AS eea_false,
       COUNT(CASE WHEN is_3d = TRUE THEN 1 END) AS is_3d_true,
       COUNT(CASE WHEN is_3d = FALSE THEN 1 END) AS is_3d_false
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 9: NULL Placeholder Column Validation  
SELECT 'NULL Placeholders Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN IsOnlineRefund IS NULL THEN 1 END) AS is_online_refund_null,
       COUNT(CASE WHEN IsNoCVV IS NULL THEN 1 END) AS is_no_cvv_null,
       COUNT(CASE WHEN IsSupportedOCT IS NULL THEN 1 END) AS is_supported_oct_null,
       COUNT(CASE WHEN ExternalTokenTrasactionType IS NULL THEN 1 END) AS external_token_null,
       COUNT(CASE WHEN user_agent_3d IS NULL THEN 1 END) AS user_agent_3d_null,
       COUNT(CASE WHEN authentication_request IS NULL THEN 1 END) AS auth_request_null
FROM POC.PUBLIC.NCP_SILVER_V3;

-- Step 10: Sample Data Inspection (first 5 records)
SELECT 'Sample Data' AS test_type,
       transaction_main_id,
       transaction_type,
       transaction_result_id,
       init_status,
       auth_3d_status,
       sale_status,
       auth_status,
       is_approved,
       is_declined,
       is_successful_authentication,
       amount_in_usd,
       inserted_at
FROM NUVEI_DWH.NCP.TRANSACTIONS_SILVER
ORDER BY transaction_main_id
LIMIT 5;

-- Step 11: Data Quality Check - Test client filtering
SELECT 'Test Client Filtering Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_clients_found
FROM POC.PUBLIC.NCP_SILVER_V3;
-- Expected: test_clients_found = 0

-- Step 12: Column List for Manual Verification
SELECT 'All Columns List' AS test_type,
       LISTAGG(COLUMN_NAME, ', ') AS all_columns
FROM (
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'PUBLIC' 
      AND TABLE_NAME = 'NCP_SILVER_V3'
      AND TABLE_CATALOG = 'POC'
    ORDER BY ORDINAL_POSITION
);

-- Expected Results Summary:
-- - Column count: 143
-- - Row count: Should match bronze data for 2025-09-05 minus test clients
-- - Status flags populated based on transaction types
-- - Boolean conversions working correctly  
-- - NULL placeholders in place
-- - Test clients filtered out (test_clients_found = 0)