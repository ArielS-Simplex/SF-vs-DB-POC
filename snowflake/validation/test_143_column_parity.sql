-- ================================================
-- Progressive Validation: Snowflake vs Databricks ETL Parity
-- Each level gets progressively deeper into validation
-- Date: 2025-09-14
-- ================================================

-- Prerequisites:
-- 1. Execute: snowflake/refactored_scripts/complete_bronze_to_silver_etl.sql
-- 2. Replace 'ncp.silver' with actual Databricks table name

-- ========================================
-- LEVEL 1: BASIC SCHEMA VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Column Count Check
SELECT 'SF - Column Count Check' AS test_type,
       COUNT(*) AS snowflake_columns
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_SILVER_V2'
  AND TABLE_CATALOG = 'POC';
-- Expected: 143 columns

-- 🟠 DATABRICKS: Column Count Check
%sql
DESCRIBE ncp.silver

-- ========================================
-- LEVEL 2: DATA VOLUME VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Row Count and Date Range
SELECT 'SF - Row Count Check' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT transaction_main_id) AS unique_transactions,
       MIN(transaction_date) AS min_date,
       MAX(transaction_date) AS max_date
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: Row Count and Date Range
%sql
SELECT 'DB - Row Count Check' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT transaction_main_id) AS unique_transactions,
       MIN(transaction_date) AS min_date,
       MAX(transaction_date) AS max_date
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 3: DERIVED COLUMNS VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Status Flags Population
SELECT 'SF - Status Flags Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_status_populated,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth_3d_status_populated,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_status_populated,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_status_populated,
       COUNT(CASE WHEN settle_status IS NOT NULL THEN 1 END) AS settle_status_populated,
       COUNT(CASE WHEN verify_auth_3d_status IS NOT NULL THEN 1 END) AS verify_auth_3d_status_populated
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: Status Flags Population
%sql
SELECT 'DB - Status Flags Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_status_populated,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth_3d_status_populated,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_status_populated,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_status_populated,
       COUNT(CASE WHEN settle_status IS NOT NULL THEN 1 END) AS settle_status_populated,
       COUNT(CASE WHEN verify_auth_3d_status IS NOT NULL THEN 1 END) AS verify_auth_3d_status_populated
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 4: BUSINESS LOGIC VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Conditional Copies Logic (FIXED CASE SENSITIVITY)
SELECT 'SF - Conditional Copies Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS auth3d_conditional_copies,
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND manage_3d_decision_auth_3d IS NOT NULL THEN 1 END) AS auth3d_decision_copies,
       COUNT(CASE WHEN UPPER(transaction_type) != 'AUTH3D' AND is_sale_3d_auth_3d IS NULL THEN 1 END) AS non_auth3d_null_copies
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: Conditional Copies Logic
%sql
SELECT 'DB - Conditional Copies Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS auth3d_conditional_copies,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND manage_3d_decision_auth_3d IS NOT NULL THEN 1 END) AS auth3d_decision_copies,
       COUNT(CASE WHEN transaction_type != 'auth3d' AND is_sale_3d_auth_3d IS NULL THEN 1 END) AS non_auth3d_null_copies
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 5: COMPLEX 3D SECURE LOGIC
-- ========================================

-- 🔵 SNOWFLAKE: 3D Secure Success Analysis
SELECT 'SF - 3D Secure Analysis Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_successful_challenge IS NOT NULL THEN 1 END) AS challenge_populated,
       COUNT(CASE WHEN is_successful_exemption IS NOT NULL THEN 1 END) AS exemption_populated,
       COUNT(CASE WHEN is_successful_frictionless IS NOT NULL THEN 1 END) AS frictionless_populated,
       COUNT(CASE WHEN is_successful_authentication IS NOT NULL THEN 1 END) AS authentication_populated,
       COUNT(CASE WHEN is_successful_challenge = 'true' THEN 1 END) AS challenge_success_count,
       COUNT(CASE WHEN is_successful_authentication = 'true' THEN 1 END) AS auth_success_count
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: 3D Secure Success Analysis
%sql
SELECT 'DB - 3D Secure Analysis Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_successful_challenge IS NOT NULL THEN 1 END) AS challenge_populated,
       COUNT(CASE WHEN is_successful_exemption IS NOT NULL THEN 1 END) AS exemption_populated,
       COUNT(CASE WHEN is_successful_frictionless IS NOT NULL THEN 1 END) AS frictionless_populated,
       COUNT(CASE WHEN is_successful_authentication IS NOT NULL THEN 1 END) AS authentication_populated,
       COUNT(CASE WHEN is_successful_challenge = 'true' THEN 1 END) AS challenge_success_count,
       COUNT(CASE WHEN is_successful_authentication = 'true' THEN 1 END) AS auth_success_count
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 6: HIGH-LEVEL APPROVAL/DECLINE LOGIC
-- ========================================

-- 🔵 SNOWFLAKE: Approval/Decline Logic Validation
SELECT 'SF - Approval/Decline Logic Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_approved = 'true' THEN 1 END) AS approved_count,
       COUNT(CASE WHEN is_approved = 'false' THEN 1 END) AS not_approved_count,
       COUNT(CASE WHEN is_declined = 'true' THEN 1 END) AS declined_count,
       COUNT(CASE WHEN is_declined = 'false' THEN 1 END) AS not_declined_count,
       -- Cross-validation: approved and declined should be mutually exclusive
       COUNT(CASE WHEN is_approved = 'true' AND is_declined = 'true' THEN 1 END) AS logic_conflict_count
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: Approval/Decline Logic Validation
%sql
SELECT 'DB - Approval/Decline Logic Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_approved = 'true' THEN 1 END) AS approved_count,
       COUNT(CASE WHEN is_approved = 'false' THEN 1 END) AS not_approved_count,
       COUNT(CASE WHEN is_declined = 'true' THEN 1 END) AS declined_count,
       COUNT(CASE WHEN is_declined = 'false' THEN 1 END) AS not_declined_count,
       COUNT(CASE WHEN is_approved = 'true' AND is_declined = 'true' THEN 1 END) AS logic_conflict_count
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 7: BOOLEAN CONVERSION VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Boolean Conversions
SELECT 'SF - Boolean Conversions Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_currency_converted = TRUE THEN 1 END) AS currency_converted_true,
       COUNT(CASE WHEN is_currency_converted = FALSE THEN 1 END) AS currency_converted_false,
       COUNT(CASE WHEN is_eea = TRUE THEN 1 END) AS eea_true,
       COUNT(CASE WHEN is_eea = FALSE THEN 1 END) AS eea_false,
       COUNT(CASE WHEN is_3d = TRUE THEN 1 END) AS is_3d_true,
       COUNT(CASE WHEN is_3d = FALSE THEN 1 END) AS is_3d_false,
       COUNT(CASE WHEN is_void = TRUE THEN 1 END) AS is_void_true,
       COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) AS liability_shift_true
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 🟠 DATABRICKS: Boolean Conversions
%sql
SELECT 'DB - Boolean Conversions Validation' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN is_currency_converted = true THEN 1 END) AS currency_converted_true,
       COUNT(CASE WHEN is_currency_converted = false THEN 1 END) AS currency_converted_false,
       COUNT(CASE WHEN is_eea = true THEN 1 END) AS eea_true,
       COUNT(CASE WHEN is_eea = false THEN 1 END) AS eea_false,
       COUNT(CASE WHEN is_3d = true THEN 1 END) AS is_3d_true,
       COUNT(CASE WHEN is_3d = false THEN 1 END) AS is_3d_false,
       COUNT(CASE WHEN is_void = true THEN 1 END) AS is_void_true,
       COUNT(CASE WHEN liability_shift = true THEN 1 END) AS liability_shift_true
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 8: DATA QUALITY VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Test Client Filtering and Data Quality
SELECT 'SF - Data Quality Check' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_clients_found,
       COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_transaction_ids,
       COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_dates,
       COUNT(CASE WHEN amount_in_usd IS NULL OR amount_in_usd = 0 THEN 1 END) AS null_or_zero_amounts
FROM POC.PUBLIC.NCP_SILVER_V2;
-- Expected: test_clients_found = 0, null counts should be minimal

-- 🟠 DATABRICKS: Test Client Filtering and Data Quality
%sql
SELECT 'DB - Data Quality Check' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(CASE WHEN multi_client_name IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_clients_found,
       COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_transaction_ids,
       COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_dates,
       COUNT(CASE WHEN amount_in_usd IS NULL OR amount_in_usd = 0 THEN 1 END) AS null_or_zero_amounts
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- LEVEL 9: SAMPLE DATA COMPARISON
-- ========================================

-- 🔵 SNOWFLAKE: Sample Data Inspection (5 records across transaction types)
SELECT 'SF - Sample Data' AS test_type,
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
       ROUND(amount_in_usd, 2) AS amount_in_usd
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE UPPER(transaction_type) IN ('AUTH', 'SALE', 'AUTH3D', 'INITAUTH3D', 'SETTLE')
ORDER BY transaction_type, transaction_main_id
LIMIT 5;

-- 🟠 DATABRICKS: Sample Data Inspection (5 records across transaction types)
%sql
SELECT 'DB - Sample Data' AS test_type,
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
       ROUND(amount_in_usd, 2) AS amount_in_usd
FROM ncp.silver
-- WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_type IN ('auth', 'sale', 'auth3d', 'initauth3d', 'settle')
ORDER BY transaction_type, transaction_main_id
LIMIT 5;

-- ========================================
-- LEVEL 10: DEEP DIVE SPECIFIC TRANSACTION VALIDATION
-- ========================================

-- 🔵 SNOWFLAKE: Specific Transaction Logic Deep Dive
SELECT 'SF - Transaction Logic Deep Dive' AS test_type,
       transaction_type,
       COUNT(*) AS count,
       COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) AS success_1006_count,
       COUNT(CASE WHEN transaction_result_id = '1008' THEN 1 END) AS decline_1008_count,
       -- Status flag validation per transaction type (FIXED CASE SENSITIVITY)
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH' AND auth_status IS NOT NULL THEN 1 END) AS auth_status_populated,
       COUNT(CASE WHEN UPPER(transaction_type) = 'SALE' AND sale_status IS NOT NULL THEN 1 END) AS sale_status_populated,
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND auth_3d_status IS NOT NULL THEN 1 END) AS auth3d_status_populated,
       -- Conditional logic validation (FIXED CASE SENSITIVITY)
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS conditional_copies_populated
FROM POC.PUBLIC.NCP_SILVER_V2
GROUP BY transaction_type
ORDER BY transaction_type;

-- 🟠 DATABRICKS: Specific Transaction Logic Deep Dive
%sql
SELECT 'DB - Transaction Logic Deep Dive' AS test_type,
       transaction_type,
       COUNT(*) AS count,
       COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) AS success_1006_count,
       COUNT(CASE WHEN transaction_result_id = '1008' THEN 1 END) AS decline_1008_count,
       COUNT(CASE WHEN transaction_type = 'auth' AND auth_status IS NOT NULL THEN 1 END) AS auth_status_populated,
       COUNT(CASE WHEN transaction_type = 'sale' AND sale_status IS NOT NULL THEN 1 END) AS sale_status_populated,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND auth_3d_status IS NOT NULL THEN 1 END) AS auth3d_status_populated,
       COUNT(CASE WHEN transaction_type = 'auth3d' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS conditional_copies_populated
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY transaction_type
ORDER BY transaction_type;

-- ========================================
-- VALIDATION SUMMARY & COMPARISON GUIDE
-- ========================================
-- 
-- VALIDATION LEVELS EXPLAINED:
-- Level 1: Basic schema - Column count should be exactly 143
-- Level 2: Data volume - Row counts and date ranges should match
-- Level 3: Derived columns - Status flags populated correctly per transaction type
-- Level 4: Business logic - Conditional copies only for auth3d transactions
-- Level 5: Complex 3D - 3D Secure success analysis logic working correctly
-- Level 6: High-level - Approval/decline logic with no conflicts
-- Level 7: Boolean conversion - String-to-boolean mapping working identically
-- Level 8: Data quality - Test clients filtered, no unexpected nulls
-- Level 9: Sample data - Specific transaction examples match exactly
-- Level 10: Deep dive - Transaction-type-specific logic validation
--
-- COMPARISON PROCESS:
-- 1. Run each Snowflake query, save results
-- 2. Run corresponding Databricks query, save results
-- 3. Compare results level by level
-- 4. Any differences at any level indicate parity issues to investigate
-- 5. All levels must pass for complete parity validation