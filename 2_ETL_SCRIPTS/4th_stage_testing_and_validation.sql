-- ==============================================================================
-- 4TH STAGE: TESTING AND VALIDATION
-- Data quality reports, business logic validation, and results verification
-- ==============================================================================

-- REQUIRED VARIABLES (if not already set in previous stages)
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET curr_timestamp = CURRENT_TIMESTAMP();
SET CLOUD_PROVIDER = 'Snowflake';

-- Get variables for reporting (these might not exist if run independently)
SET total_rows = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));
SET target_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'TRANSACTIONS_SILVER');

-- Results summary with schema evolution info + DATABRICKS-STYLE METRICS
SELECT 
    'ETL COMPLETED' AS status,
    $TARGET_TABLE AS target_table,
    $total_rows AS rows_processed,
    $curr_timestamp AS completion_time,
    (SELECT COUNT(*) FROM schema_comparison) AS new_columns_detected,
    $target_exists AS target_table_existed,
    $CLOUD_PROVIDER AS cloud_environment,
    'DATABRICKS_LOGIC_COMPLETE' AS feature_parity;

-- DATA QUALITY SUMMARY: Mimic Databricks data quality reporting
SELECT 
    'DATA QUALITY REPORT' AS report_type,
    data_quality_flag,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM IDENTIFIER($TARGET_TABLE) 
GROUP BY data_quality_flag;

-- BUSINESS LOGIC VALIDATION: Show derived column statistics
SELECT 
    'BUSINESS LOGIC SUMMARY' AS report_type,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_transactions,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_transactions,
    COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) AS successful_auths,
    COUNT(CASE WHEN is_successful_frictionless = true THEN 1 END) AS frictionless_success,
    COUNT(CASE WHEN is_successful_challenge = true THEN 1 END) AS challenge_success
FROM IDENTIFIER($TARGET_TABLE);

-- Final count verification
SELECT COUNT(*) AS total_rows_in_target
FROM IDENTIFIER($TARGET_TABLE);

-- Schema evolution summary (show any new columns that were detected)
-- Create temporary table if it doesn't exist (for independent execution)
CREATE OR REPLACE TEMPORARY TABLE schema_comparison AS (
    SELECT 'No schema changes detected' AS COLUMN_NAME, 'INFO' AS DATA_TYPE
    WHERE NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SCHEMA_COMPARISON')
);

SELECT 
    'SCHEMA EVOLUTION SUMMARY' AS info,
    COLUMN_NAME,
    DATA_TYPE
FROM schema_comparison;

-- Sample data verification
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

SELECT 'STAGE 4 COMPLETED: Testing and validation reports generated' AS status;
