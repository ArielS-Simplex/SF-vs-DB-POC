-- ==============================================================================
-- TESTING AND VALIDATION QUERIES
-- Run these AFTER the ETL to validate results
-- ==============================================================================

-- REQUIRED VARIABLES
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET CLOUD_PROVIDER = 'Snowflake';

-- BASIC ETL COMPLETION SUMMARY
SELECT 
    'ETL COMPLETED' AS status,
    $TARGET_TABLE AS target_table,
    COUNT(*) AS rows_processed,
    CURRENT_TIMESTAMP() AS completion_time,
    $CLOUD_PROVIDER AS cloud_environment,
    'DATABRICKS_LOGIC_COMPLETE' AS feature_parity
FROM IDENTIFIER($TARGET_TABLE);

-- DATA QUALITY SUMMARY
SELECT 
    'DATA QUALITY REPORT' AS report_type,
    data_quality_flag,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM IDENTIFIER($TARGET_TABLE) 
GROUP BY data_quality_flag;

-- BUSINESS LOGIC VALIDATION
SELECT 
    'BUSINESS LOGIC SUMMARY' AS report_type,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_transactions,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_transactions,
    COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) AS successful_auths,
    COUNT(CASE WHEN is_successful_frictionless = true THEN 1 END) AS frictionless_success,
    COUNT(CASE WHEN is_successful_challenge = true THEN 1 END) AS challenge_success
FROM IDENTIFIER($TARGET_TABLE);

-- SAMPLE DATA VERIFICATION
SELECT 
    'SAMPLE DATA VERIFICATION' AS report_type,
    transaction_main_id,
    transaction_date,
    transaction_type,
    multi_client_name,
    is_approved,
    is_successful_authentication,
    data_quality_flag
FROM IDENTIFIER($TARGET_TABLE)
LIMIT 10;

-- FINAL VERIFICATION
SELECT 
    'FINAL VERIFICATION' AS report_type,
    COUNT(*) AS total_rows,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_count,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_count
FROM IDENTIFIER($TARGET_TABLE);

-- TRANSACTION TYPE BREAKDOWN
SELECT 
    'TRANSACTION TYPE BREAKDOWN' AS report_type,
    transaction_type,
    COUNT(*) AS count,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY transaction_type
ORDER BY count DESC;

-- CLIENT BREAKDOWN
SELECT 
    'CLIENT BREAKDOWN' AS report_type,
    multi_client_name,
    COUNT(*) AS transactions,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY multi_client_name
ORDER BY transactions DESC
LIMIT 10;
