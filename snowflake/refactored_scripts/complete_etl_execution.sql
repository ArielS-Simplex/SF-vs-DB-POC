-- ================================================
-- Snowflake Bronze-to-Silver ETL - Complete Execution Script
-- This script executes the complete ETL pipeline equivalent to your Databricks notebooks
-- ================================================

-- ================================================
-- Snowflake Connection and Session Setup
-- ================================================

-- STEP 1: CONNECTION READY - Just add username/password to ~/.snowflake/connections.toml

-- Set context for your POC
USE DATABASE POC;
USE SCHEMA PUBLIC;
USE WAREHOUSE X_SMALL_2_GEN;

-- ================================================
-- SETUP AND INITIALIZATION
-- ================================================

-- Set session parameters
ALTER SESSION SET MULTI_STATEMENT_COUNT = 200;
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- Initialize variables (equivalent to Databricks widgets)
SET TARGET_TABLE = 'POC.PUBLIC.silver';  -- Updated for your POC database
SET SOURCE_TABLE = 'POC.PUBLIC.bronze';  -- Updated for your POC database
SET TABLE_KEYS = 'transaction_main_id,transaction_id_life_cycle';
SET CURRENT_TIMESTAMP = CURRENT_TIMESTAMP();

-- Log start of ETL process
SELECT 'Starting Bronze-to-Silver ETL at: ' || $CURRENT_TIMESTAMP AS etl_start_log;

-- ================================================
-- STEP 1: Load Utility Functions
-- ================================================

-- Execute utility functions setup (equivalent to %run ./data_utility_modules)
@snowflake/refactored_scripts/data_utility_functions.sql;

-- Execute custom ETL functions setup (equivalent to %run ./custom_etl_functions)  
@snowflake/refactored_scripts/custom_etl_functions.sql;

SELECT 'Utility functions loaded successfully' AS setup_log;

-- ================================================
-- STEP 2: Initialize Metadata (equivalent to SchemaManager initialization)
-- ================================================

-- Ensure metadata table exists
CREATE TABLE IF NOT EXISTS main.ncp.metadata_table (
    table_name STRING,
    schema_json STRING,
    checkpoint TIMESTAMP_NTZ,
    source_table STRING,
    table_keys STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Set up metadata for this table if it doesn't exist
MERGE INTO main.ncp.metadata_table AS target
USING (
    SELECT 
        $TARGET_TABLE AS table_name,
        $SOURCE_TABLE AS source_table,
        $TABLE_KEYS AS table_keys,
        '1900-01-01 00:00:00'::TIMESTAMP_NTZ AS checkpoint  -- Default old checkpoint
) AS source
ON target.table_name = source.table_name
WHEN NOT MATCHED THEN 
    INSERT (table_name, source_table, table_keys, checkpoint)
    VALUES (source.table_name, source.source_table, source.table_keys, source.checkpoint);

SELECT 'Metadata initialized' AS metadata_log;

-- ================================================
-- STEP 3: Get Current Metadata Values
-- ================================================

-- Get checkpoint time (equivalent to schema_mgr.get_metadata calls)
SET CHECKPOINT_TIME = (
    SELECT COALESCE(checkpoint, '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
    FROM main.ncp.metadata_table 
    WHERE table_name = $TARGET_TABLE
);

-- Verify metadata
SELECT 
    'Checkpoint Time: ' || $CHECKPOINT_TIME AS checkpoint_log,
    'Source Table: ' || $SOURCE_TABLE AS source_log,
    'Target Table: ' || $TARGET_TABLE AS target_log,
    'Table Keys: ' || $TABLE_KEYS AS keys_log;

-- ================================================
-- STEP 4: Extract and Transform Data
-- ================================================

-- Create view with new data since last checkpoint
CREATE OR REPLACE TEMPORARY VIEW new_source_data AS
SELECT *,
       CONVERT_TIMEZONE('GMT', CURRENT_TIMESTAMP()) AS inserted_at
FROM IDENTIFIER($SOURCE_TABLE)
WHERE inserted_at > $CHECKPOINT_TIME;

-- Get row count for logging
SET TOTAL_ROWS = (SELECT COUNT(*) FROM new_source_data);

SELECT 'Total rows to process: ' || $TOTAL_ROWS AS row_count_log;

-- Only proceed if we have data to process
-- ================================================
-- STEP 5: Apply Transformations (equivalent to your notebooks)
-- ================================================

-- Apply complete transformation pipeline
CREATE OR REPLACE TEMPORARY VIEW transformed_transactions AS
WITH base_filtered AS (
    -- Filter out test clients (equivalent to filter in custom_etl_functions)
    SELECT *
    FROM new_source_data
    WHERE multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
),

-- Apply deduplication using table keys
deduplicated AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY transaction_main_id, transaction_id_life_cycle
                   ORDER BY inserted_at DESC
               ) as rn
        FROM base_filtered
    )
    WHERE rn = 1
),

-- Add conversion columns (equivalent to create_conversions_columns)
with_conversions AS (
    SELECT *,
           -- Conditional copies
           CASE WHEN transaction_type = 'auth3d' THEN is_sale_3d END AS is_sale_3d_auth_3d,
           CASE WHEN transaction_type = 'auth3d' THEN manage_3d_decision END AS manage_3d_decision_auth_3d,
           
           -- Transaction status flags  
           CASE WHEN transaction_type = 'initauth3d' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'initauth3d' THEN 'false' END AS init_status,
           CASE WHEN transaction_type = 'auth3d' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'auth3d' THEN 'false' END AS auth_3d_status,
           CASE WHEN transaction_type = 'sale' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'sale' THEN 'false' END AS sale_status,
           CASE WHEN transaction_type = 'auth' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'auth' THEN 'false' END AS auth_status,
           CASE WHEN transaction_type = 'settle' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'settle' THEN 'false' END AS settle_status,
           CASE WHEN transaction_type = 'verify_auth_3d' AND transaction_result_id = '1006' THEN 'true'
                WHEN transaction_type = 'verify_auth_3d' THEN 'false' END AS verify_auth_3d_status,
           
           -- Business logic columns
           CASE WHEN "3d_flow_status" = '3d_success' THEN 'true'
                WHEN "3d_flow_status" IN ('3d_failure', '3d_wasnt_completed') THEN 'false' END AS is_successful_challenge,
           CASE WHEN authentication_flow = 'exemption' THEN 'true'
                WHEN challenge_preference = 'y_requested_by_acquirer' THEN 'false' END AS is_successful_exemption,
           CASE WHEN authentication_flow = 'frictionless' AND status = '40' THEN 'true'
                WHEN authentication_flow = 'frictionless' THEN 'false' END AS is_successful_frictionless,
           CASE WHEN "3d_flow_status" = '3d_success' OR (authentication_flow = 'frictionless' AND status = '40') THEN 'true'
                WHEN (acs_url IS NOT NULL AND authentication_flow != 'exemption') OR (authentication_flow = 'frictionless' AND status != '40') THEN 'false' END AS is_successful_authentication,
           CASE WHEN auth_status = 'true' OR sale_status = 'true' THEN 'true'
                WHEN auth_status = 'false' OR sale_status = 'false' THEN 'false' END AS is_approved,
           CASE WHEN transaction_type IN ('sale', 'auth') AND transaction_result_id = '1008' THEN 'true'
                WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN 'false' END AS is_declined
           
    FROM deduplicated
),

-- Apply data type fixes (equivalent to fixing_dtypes)
final_transformed AS (
    SELECT 
        -- Handle forced null columns
        NULL::STRING AS user_agent_3d,
        NULL::STRING AS authentication_request, 
        NULL::STRING AS authentication_response,
        NULL::NUMBER AS authorization_req_duration,
        
        -- Boolean field conversions
        CASE WHEN LOWER(TRIM(is_currency_converted)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_currency_converted)) IN ('false','0','no','0.0') THEN FALSE END AS is_currency_converted,
        CASE WHEN LOWER(TRIM(is_eea)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_eea)) IN ('false','0','no','0.0') THEN FALSE END AS is_eea,
        CASE WHEN LOWER(TRIM(is_external_mpi)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_external_mpi)) IN ('false','0','no','0.0') THEN FALSE END AS is_external_mpi,
        CASE WHEN LOWER(TRIM(is_partial_amount)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_partial_amount)) IN ('false','0','no','0.0') THEN FALSE END AS is_partial_amount,
        CASE WHEN LOWER(TRIM(is_prepaid)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_prepaid)) IN ('false','0','no','0.0') THEN FALSE END AS is_prepaid,
        CASE WHEN LOWER(TRIM(is_sale_3d)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_sale_3d)) IN ('false','0','no','0.0') THEN FALSE END AS is_sale_3d,
        CASE WHEN LOWER(TRIM(is_void)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_void)) IN ('false','0','no','0.0') THEN FALSE END AS is_void,
        CASE WHEN LOWER(TRIM(liability_shift)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(liability_shift)) IN ('false','0','no','0.0') THEN FALSE END AS liability_shift,
        CASE WHEN LOWER(TRIM(manage_3d_decision)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(manage_3d_decision)) IN ('false','0','no','0.0') THEN FALSE END AS manage_3d_decision,
        CASE WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('false','0','no','0.0') THEN FALSE END AS mc_scheme_token_used,
        CASE WHEN LOWER(TRIM(partial_approval_is_void)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(partial_approval_is_void)) IN ('false','0','no','0.0') THEN FALSE END AS partial_approval_is_void,
        CASE WHEN LOWER(TRIM(rebill)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(rebill)) IN ('false','0','no','0.0') THEN FALSE END AS rebill,
        CASE WHEN LOWER(TRIM(is_3d)) IN ('true','1','yes','1.0') THEN TRUE
             WHEN LOWER(TRIM(is_3d)) IN ('false','0','no','0.0') THEN FALSE END AS is_3d,
        
        -- String cleaning
        CASE WHEN LOWER(TRIM(transaction_main_id)) IN ('<na>','na','nan','none','','\\x00','deprecated') THEN NULL
             WHEN REGEXP_LIKE(transaction_main_id, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(transaction_main_id, '(\\d+)')
             ELSE LOWER(TRIM(transaction_main_id)) END AS transaction_main_id,
        
        -- Keep all other columns unchanged
        * EXCLUDE (
            user_agent_3d, authentication_request, authentication_response, authorization_req_duration,
            is_currency_converted, is_eea, is_external_mpi, is_partial_amount, is_prepaid,
            is_sale_3d, is_void, liability_shift, manage_3d_decision, mc_scheme_token_used,
            partial_approval_is_void, rebill, is_3d, transaction_main_id, rn
        )
        
    FROM with_conversions
)

SELECT * FROM final_transformed;

SELECT 'Data transformation completed' AS transform_log;

-- ================================================
-- STEP 6: Handle Schema Evolution and Merge
-- ================================================

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS IDENTIFIER($TARGET_TABLE) LIKE transformed_transactions;

-- Perform merge operation (equivalent to Delta merge)
MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING transformed_transactions AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_id_life_cycle = source.transaction_id_life_cycle
WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT *;

LET rows_merged := SQLROWCOUNT;
SELECT 'Rows merged: ' || $rows_merged AS merge_log;

-- ================================================
-- STEP 7: Update Checkpoint (equivalent to schema_mgr.update_metadata)
-- ================================================

-- Update checkpoint only if we processed rows
MERGE INTO main.ncp.metadata_table AS target
USING (SELECT $TARGET_TABLE AS table_name, $CURRENT_TIMESTAMP AS checkpoint, $TOTAL_ROWS AS row_count) AS source
ON target.table_name = source.table_name
WHEN MATCHED AND source.row_count > 0 THEN 
    UPDATE SET 
        target.checkpoint = source.checkpoint,
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND source.row_count > 0 THEN 
    INSERT (table_name, checkpoint, updated_at) 
    VALUES (source.table_name, source.checkpoint, CURRENT_TIMESTAMP());

-- ================================================
-- STEP 8: Final Validation and Logging
-- ================================================

-- Get final statistics
SET FINAL_ROW_COUNT = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));

-- Log completion
SELECT 
    'ETL Process Completed at: ' || CURRENT_TIMESTAMP() AS completion_time,
    'Source Table: ' || $SOURCE_TABLE AS source_info,
    'Target Table: ' || $TARGET_TABLE AS target_info,
    'Rows Processed: ' || $TOTAL_ROWS AS processed_count,
    'Total Rows in Target: ' || $FINAL_ROW_COUNT AS total_count,
    'Checkpoint Updated to: ' || $CURRENT_TIMESTAMP AS checkpoint_info;

-- Optional: Basic validation queries
SELECT 'Validation: Transaction types distribution' AS validation_header;
SELECT 
    transaction_type,
    COUNT(*) AS count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
FROM IDENTIFIER($TARGET_TABLE)
WHERE inserted_at >= $CHECKPOINT_TIME  -- Only validate newly processed data
GROUP BY transaction_type
ORDER BY count DESC;

SELECT 'Validation: Boolean field distribution' AS validation_header2;
SELECT 
    is_sale_3d,
    is_void,
    liability_shift,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
WHERE inserted_at >= $CHECKPOINT_TIME
GROUP BY is_sale_3d, is_void, liability_shift
ORDER BY count DESC
LIMIT 10;

-- ================================================
-- SUCCESS - ETL PROCESS COMPLETE
-- ================================================

SELECT '🎉 SUCCESS: Bronze-to-Silver ETL completed successfully!' AS final_status;
