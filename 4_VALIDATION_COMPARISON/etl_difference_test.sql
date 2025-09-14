-- ==============================================================================
-- DATABRICKS ETL LOGIC TEST SCRIPT
-- Test specific Databricks transformations that we might be missing
-- ==============================================================================

-- This script tests the exact differences we identified between Databricks and Snowflake ETL logic
-- Run this BEFORE updating the main ETL to understand what's causing the 4,479 row difference

SET VALIDATION_DATE = '2025-09-06';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';

-- ==============================================================================
-- TEST 1: NULL BYTE CHARACTER DETECTION
-- Databricks filters records with \x00 characters, we don't
-- ==============================================================================

SELECT 
    'NULL_BYTE_TEST' AS test_name,
    'Records with null byte characters that Databricks filters but we keep' AS description,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (
    status LIKE '%' || CHR(0) || '%' OR 
    acs_url LIKE '%' || CHR(0) || '%' OR 
    user_agent_3d LIKE '%' || CHR(0) || '%' OR
    authentication_request LIKE '%' || CHR(0) || '%' OR
    authentication_response LIKE '%' || CHR(0) || '%'
  );

-- ==============================================================================
-- TEST 2: YES/NO BOOLEAN VALUES
-- Databricks handles "yes"/"no" in boolean fields, check if we do
-- ==============================================================================

SELECT 
    'YES_NO_BOOLEAN_TEST' AS test_name,
    'Records with yes/no boolean values that might be handled differently' AS description,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (
    LOWER(TRIM(is_void)) IN ('yes', 'no') OR
    LOWER(TRIM(liability_shift)) IN ('yes', 'no') OR
    LOWER(TRIM(is_sale_3d)) IN ('yes', 'no') OR
    LOWER(TRIM(manage_3d_decision)) IN ('yes', 'no') OR
    LOWER(TRIM(is_external_mpi)) IN ('yes', 'no') OR
    LOWER(TRIM(rebill)) IN ('yes', 'no') OR
    LOWER(TRIM(is_prepaid)) IN ('yes', 'no') OR
    LOWER(TRIM(is_eea)) IN ('yes', 'no') OR
    LOWER(TRIM(is_currency_converted)) IN ('yes', 'no') OR
    LOWER(TRIM(mc_scheme_token_used)) IN ('yes', 'no') OR
    LOWER(TRIM(is_3d)) IN ('yes', 'no')
  );

-- ==============================================================================
-- TEST 3: NUMERIC STRING EXTRACTION DIFFERENCES
-- Check if our regex pattern matches Databricks exactly
-- ==============================================================================

SELECT 
    'NUMERIC_STRING_TEST' AS test_name,
    'Records where numeric string extraction might differ' AS description,
    status AS original_status,
    CASE 
        WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1)
        ELSE status
    END AS snowflake_extraction,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND REGEXP_LIKE(status, '^\\d+\\.?\\d*$')
GROUP BY status, snowflake_extraction
ORDER BY record_count DESC
LIMIT 10;

-- ==============================================================================
-- TEST 4: DEPRECATED VALUE HANDLING
-- Check records with "deprecated" values that Databricks sets to NULL
-- ==============================================================================

SELECT 
    'DEPRECATED_VALUES_TEST' AS test_name,
    'Records with deprecated values that Databricks nullifies' AS description,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (
    LOWER(TRIM(user_agent_3d)) = 'deprecated' OR
    LOWER(TRIM(authentication_request)) = 'deprecated' OR
    LOWER(TRIM(authentication_response)) = 'deprecated' OR
    LOWER(TRIM(acs_url)) = 'deprecated'
  );

-- ==============================================================================
-- TEST 5: FORCED NULL COLUMNS
-- Databricks forces these columns to NULL regardless of content
-- ==============================================================================

SELECT 
    'FORCED_NULL_COLUMNS_TEST' AS test_name,
    'Records where Databricks forces specific columns to NULL' AS description,
    COUNT(*) AS total_records,
    SUM(CASE WHEN user_agent_3d IS NOT NULL THEN 1 ELSE 0 END) AS user_agent_3d_not_null,
    SUM(CASE WHEN authentication_request IS NOT NULL THEN 1 ELSE 0 END) AS auth_request_not_null,
    SUM(CASE WHEN authentication_response IS NOT NULL THEN 1 ELSE 0 END) AS auth_response_not_null,
    SUM(CASE WHEN authorization_req_duration IS NOT NULL THEN 1 ELSE 0 END) AS auth_duration_not_null
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE;

-- ==============================================================================
-- TEST 6: AUTH3D TRANSACTION ANALYSIS
-- Databricks creates additional columns for Auth3D transactions
-- ==============================================================================

SELECT 
    'AUTH3D_TRANSACTIONS_TEST' AS test_name,
    'Auth3D transactions where Databricks creates additional columns' AS description,
    transaction_type,
    COUNT(*) AS record_count,
    SUM(CASE WHEN is_sale_3d IS NOT NULL THEN 1 ELSE 0 END) AS has_is_sale_3d,
    SUM(CASE WHEN manage_3d_decision IS NOT NULL THEN 1 ELSE 0 END) AS has_manage_3d_decision
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND transaction_type = 'Auth3D'
GROUP BY transaction_type;

-- ==============================================================================
-- TEST 7: EMPTY STRING VS NULL HANDLING
-- Check records with empty strings vs NULL that might be handled differently
-- ==============================================================================

SELECT 
    'EMPTY_STRING_TEST' AS test_name,
    'Records with empty strings that might be converted to NULL differently' AS description,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (
    status = '' OR status = ' ' OR
    acs_url = '' OR acs_url = ' ' OR
    user_agent_3d = '' OR user_agent_3d = ' ' OR
    processor_name = '' OR processor_name = ' ' OR
    currency_code = '' OR currency_code = ' '
  );

-- ==============================================================================
-- TEST 8: BOOLEAN FIELD DETAILED ANALYSIS
-- Analyze each boolean field to see exact value distributions
-- ==============================================================================

SELECT 
    'BOOLEAN_FIELD_ANALYSIS' AS test_name,
    'is_void' AS field_name,
    is_void AS field_value,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND is_void IS NOT NULL
GROUP BY is_void
ORDER BY record_count DESC

UNION ALL

SELECT 
    'BOOLEAN_FIELD_ANALYSIS' AS test_name,
    'liability_shift' AS field_name,
    liability_shift AS field_value,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND liability_shift IS NOT NULL
GROUP BY liability_shift
ORDER BY record_count DESC

UNION ALL

SELECT 
    'BOOLEAN_FIELD_ANALYSIS' AS test_name,
    'is_sale_3d' AS field_name,
    is_sale_3d AS field_value,
    COUNT(*) AS record_count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND is_sale_3d IS NOT NULL
GROUP BY is_sale_3d
ORDER BY record_count DESC;

-- ==============================================================================
-- TEST 9: COMPREHENSIVE DIFFERENCE SIMULATOR
-- Simulate what Databricks would do vs what we currently do
-- ==============================================================================

WITH databricks_simulation AS (
    SELECT 
        transaction_main_id,
        transaction_date,
        
        -- Simulate Databricks boolean normalization
        CASE 
            WHEN LOWER(TRIM(is_void)) IN ('true', '1', 'yes', '1.0') THEN 'TRUE_DB'
            WHEN LOWER(TRIM(is_void)) IN ('false', '0', 'no', '0.0') THEN 'FALSE_DB'
            ELSE 'NULL_DB'
        END AS is_void_databricks,
        
        -- Our current Snowflake logic
        CASE 
            WHEN LOWER(TRIM(is_void)) IN ('true', '1', 'yes', '1.0') THEN 'TRUE_SF'
            WHEN LOWER(TRIM(is_void)) IN ('false', '0', 'no', '0.0') THEN 'FALSE_SF'
            ELSE 'NULL_SF'
        END AS is_void_snowflake,
        
        -- Simulate Databricks string cleaning
        CASE 
            WHEN LOWER(TRIM(status)) IN ('<na>', 'na', 'nan', 'none', '', ' ') THEN NULL
            WHEN status LIKE '%' || CHR(0) || '%' THEN NULL  -- null byte handling
            WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1)
            ELSE TRIM(LOWER(status))
        END AS status_databricks,
        
        -- Our current logic
        CASE 
            WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1)
            ELSE TRIM(LOWER(status))
        END AS status_snowflake,
        
        -- Test client filtering
        CASE 
            WHEN LOWER(multi_client_name) IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') 
            THEN 'FILTERED'
            ELSE 'KEPT'
        END AS client_filter_result
        
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
)

SELECT 
    'DIFFERENCE_SIMULATION' AS test_name,
    'Summary of potential differences between Databricks and Snowflake processing' AS description,
    SUM(CASE WHEN is_void_databricks != is_void_snowflake THEN 1 ELSE 0 END) AS boolean_differences,
    SUM(CASE WHEN status_databricks != status_snowflake THEN 1 ELSE 0 END) AS string_differences,
    SUM(CASE WHEN client_filter_result = 'FILTERED' THEN 1 ELSE 0 END) AS filtered_records,
    COUNT(*) AS total_records
FROM databricks_simulation;

-- ==============================================================================
-- TEST 10: MATHEMATICAL VALIDATION
-- Sum up all potential differences to see if they add up to 4,479
-- ==============================================================================

SELECT 
    'MATHEMATICAL_VALIDATION' AS test_name,
    'Sum of all potential differences' AS description,
    (
        -- Records with null bytes (Databricks filters, we don't)
        (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE) 
         WHERE DATE(transaction_date) = $VALIDATION_DATE 
           AND (status LIKE '%' || CHR(0) || '%' OR acs_url LIKE '%' || CHR(0) || '%')) +
        
        -- Records with yes/no booleans (might be handled differently)
        (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE)
         WHERE DATE(transaction_date) = $VALIDATION_DATE
           AND (LOWER(TRIM(is_void)) IN ('yes', 'no') OR LOWER(TRIM(liability_shift)) IN ('yes', 'no'))) +
           
        -- Records with deprecated values (Databricks nullifies, we might not)
        (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE)
         WHERE DATE(transaction_date) = $VALIDATION_DATE
           AND LOWER(TRIM(user_agent_3d)) = 'deprecated')
    ) AS estimated_difference_total;
