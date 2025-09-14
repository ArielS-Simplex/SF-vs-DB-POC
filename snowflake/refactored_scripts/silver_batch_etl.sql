-- ================================================
-- Snowflake Bronze-to-Silver ETL - Main Script
-- Equivalent to: silver_batch_etl.ipynb
-- ================================================

-- Set session parameters for optimal performance
ALTER SESSION SET MULTI_STATEMENT_COUNT = 100;
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- Variables (equivalent to Databricks widgets)
SET TARGET_TABLE = 'NUVEI_DWH.NCP.TRANSACTIONS_SILVER';  -- Replace with actual table name
SET CURRENT_TIMESTAMP = CURRENT_TIMESTAMP();

-- ================================================
-- STEP 1: Get Metadata (equivalent to SchemaManager)
-- ================================================

-- Create metadata table if not exists (equivalent to SchemaManager._create_metadata_table_if_not_exists)
CREATE TABLE IF NOT EXISTS NUVEI_DWH.NCP.METADATA_TABLE (
    table_name STRING,
    schema_json STRING,
    checkpoint TIMESTAMP_NTZ,
    source_table STRING,
    table_keys STRING
);

-- Get metadata values (equivalent to schema_mgr.get_metadata calls)
SET CHECKPOINT_TIME = (
    SELECT checkpoint 
    FROM NUVEI_DWH.NCP.METADATA_TABLE 
    WHERE table_name = $TARGET_TABLE
);

SET SOURCE_TABLE = (
    SELECT source_table 
    FROM NUVEI_DWH.NCP.METADATA_TABLE 
    WHERE table_name = $TARGET_TABLE
);

SET TABLE_KEYS = (
    SELECT table_keys 
    FROM NUVEI_DWH.NCP.METADATA_TABLE 
    WHERE table_name = $TARGET_TABLE
);

-- ================================================
-- STEP 2: Extract New Data (equivalent to source_df creation)
-- ================================================

-- Create temporary view for new data (equivalent to Spark DataFrame)
CREATE OR REPLACE TEMPORARY VIEW NEW_SOURCE_DATA AS
SELECT *,
       CONVERT_TIMEZONE('GMT', CURRENT_TIMESTAMP()) AS inserted_at
FROM IDENTIFIER($SOURCE_TABLE)
WHERE inserted_at > $CHECKPOINT_TIME
;

-- Remove unnecessary columns (equivalent to .drop() operations)
CREATE OR REPLACE TEMPORARY VIEW CLEANED_SOURCE_DATA AS
SELECT * EXCLUDE (inserted_at_original, source_file_path, source_file_name),
       inserted_at
FROM NEW_SOURCE_DATA
;

-- Apply deduplication (equivalent to .dropDuplicates())
-- Note: This creates a row-numbered version to handle duplicates
CREATE OR REPLACE TEMPORARY VIEW DEDUPLICATED_DATA AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
                   -- Dynamic key construction based on $TABLE_KEYS
                   -- For now using common keys - this would be dynamic in real implementation
                   transaction_main_id, transaction_id_life_cycle
               ), ',')
               ORDER BY inserted_at DESC
           ) as rn
    FROM CLEANED_SOURCE_DATA
)
WHERE rn = 1
;

-- Get row count for logging (equivalent to source_df.count())
SET TOTAL_ROWS = (SELECT COUNT(*) FROM DEDUPLICATED_DATA);

-- Log the run information
SELECT 'Run timestamp: ' || $CURRENT_TIMESTAMP || ' - Total rows expected: ' || $TOTAL_ROWS AS log_message;

-- ================================================
-- STEP 3: Apply Transformations (equivalent to filter_and_transform_transactions)
-- ================================================

-- Create view with all transformations applied
CREATE OR REPLACE TEMPORARY VIEW TRANSFORMED_DATA AS
WITH base_data AS (
    SELECT *
    FROM DEDUPLICATED_DATA
    WHERE multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
),

-- Add conversion columns (equivalent to create_conversions_columns function)
with_conversions AS (
    SELECT *,
           -- Conditional copies
           CASE WHEN transaction_type = 'auth3d' THEN is_sale_3d END AS is_sale_3d_auth_3d,
           CASE WHEN transaction_type = 'auth3d' THEN manage_3d_decision END AS manage_3d_decision_auth_3d,
           
           -- Transaction result status flags
           CASE WHEN transaction_type = 'initauth3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS init_status,
           
           CASE WHEN transaction_type = 'auth3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS auth_3d_status,
           
           CASE WHEN transaction_type = 'sale' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS sale_status,
           
           CASE WHEN transaction_type = 'auth' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS auth_status,
           
           CASE WHEN transaction_type = 'settle' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS settle_status,
           
           CASE WHEN transaction_type = 'verify_auth_3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS verify_auth_3d_status,
           
           -- Challenge success
           CASE 
               WHEN "3d_flow_status" = '3d_success' THEN 'true'
               WHEN "3d_flow_status" IN ('3d_failure', '3d_wasnt_completed') THEN 'false'
           END AS is_successful_challenge,
           
           -- Exemption logic
           CASE 
               WHEN authentication_flow = 'exemption' THEN 'true'
               WHEN challenge_preference = 'y_requested_by_acquirer' THEN 'false'
           END AS is_successful_exemption,
           
           -- Frictionless logic
           CASE 
               WHEN authentication_flow = 'frictionless' AND status = '40' THEN 'true'
               WHEN authentication_flow = 'frictionless' THEN 'false'
           END AS is_successful_frictionless,
           
           -- Successful authentication
           CASE 
               WHEN "3d_flow_status" = '3d_success' 
                    OR (authentication_flow = 'frictionless' AND status = '40') THEN 'true'
               WHEN (acs_url IS NOT NULL AND authentication_flow != 'exemption')
                    OR (authentication_flow = 'frictionless' AND status != '40') THEN 'false'
           END AS is_successful_authentication,
           
           -- Approval logic
           CASE 
               WHEN auth_status = 'true' OR sale_status = 'true' THEN 'true'
               WHEN auth_status = 'false' OR sale_status = 'false' THEN 'false'
           END AS is_approved,
           
           -- Decline logic
           CASE 
               WHEN transaction_type IN ('sale', 'auth') AND transaction_result_id = '1008' THEN 'true'
               WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN 'false'
           END AS is_declined
           
    FROM base_data
),

-- Apply data type fixes (equivalent to fixing_dtypes function)
with_fixed_types AS (
    SELECT 
        -- Force null for specific columns
        NULL::STRING AS user_agent_3d,
        NULL::STRING AS authentication_request,
        NULL::STRING AS authentication_response,
        NULL::NUMBER AS authorization_req_duration,
        
        -- Boolean conversions
        CASE 
            WHEN LOWER(TRIM(is_currency_converted)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_currency_converted)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_currency_converted,
        
        CASE 
            WHEN LOWER(TRIM(is_eea)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_eea)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_eea,
        
        CASE 
            WHEN LOWER(TRIM(is_external_mpi)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_external_mpi)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_external_mpi,
        
        CASE 
            WHEN LOWER(TRIM(is_partial_amount)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_partial_amount)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_partial_amount,
        
        CASE 
            WHEN LOWER(TRIM(is_prepaid)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_prepaid)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_prepaid,
        
        CASE 
            WHEN LOWER(TRIM(is_sale_3d)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_sale_3d)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_sale_3d,
        
        CASE 
            WHEN LOWER(TRIM(is_void)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_void)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_void,
        
        CASE 
            WHEN LOWER(TRIM(liability_shift)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(liability_shift)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS liability_shift,
        
        CASE 
            WHEN LOWER(TRIM(manage_3d_decision)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(manage_3d_decision)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS manage_3d_decision,
        
        CASE 
            WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS mc_scheme_token_used,
        
        CASE 
            WHEN LOWER(TRIM(partial_approval_is_void)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(partial_approval_is_void)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS partial_approval_is_void,
        
        CASE 
            WHEN LOWER(TRIM(rebill)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(rebill)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS rebill,
        
        CASE 
            WHEN LOWER(TRIM(is_3d)) IN ('true', '1', 'yes', '1.0') THEN TRUE
            WHEN LOWER(TRIM(is_3d)) IN ('false', '0', 'no', '0.0') THEN FALSE
            ELSE NULL
        END AS is_3d,
        
        -- String cleaning
        CASE 
            WHEN LOWER(TRIM(transaction_main_id)) IN ('<na>', 'na', 'nan', 'none', '', ' ', '\\x00', 'deprecated') THEN NULL
            WHEN REGEXP_LIKE(transaction_main_id, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(transaction_main_id, '(\\d+)', 1, 1)
            ELSE LOWER(TRIM(transaction_main_id))
        END AS transaction_main_id,
        
        -- Add all other columns (this would be dynamic in real implementation)
        * EXCLUDE (
            user_agent_3d, authentication_request, authentication_response, authorization_req_duration,
            is_currency_converted, is_eea, is_external_mpi, is_partial_amount, is_prepaid,
            is_sale_3d, is_void, liability_shift, manage_3d_decision, mc_scheme_token_used,
            partial_approval_is_void, rebill, is_3d, transaction_main_id
        )
        
    FROM with_conversions
)

SELECT * FROM with_fixed_types;

-- ================================================
-- STEP 4: Handle Table Schema Evolution
-- ================================================

-- Check if target table exists and handle schema evolution
CREATE OR REPLACE TEMPORARY VIEW SCHEMA_CHECK AS
SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'NCP' 
  AND TABLE_NAME = 'TRANSACTIONS_SILVER'
  AND TABLE_CATALOG = 'NUVEI_DWH'
;

-- Note: In a real implementation, we would dynamically detect new columns
-- and add them using ALTER TABLE ADD COLUMN statements
-- For now, assuming schema compatibility

-- ================================================
-- STEP 5: Merge Data into Target Table
-- ================================================

-- Perform MERGE operation (equivalent to Delta merge)
MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING TRANSFORMED_DATA AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_id_life_cycle = source.transaction_id_life_cycle
WHEN MATCHED THEN 
    UPDATE SET 
        target.transaction_date = source.transaction_date,
        target.transaction_type_id = source.transaction_type_id,
        target.transaction_type = source.transaction_type,
        target.transaction_result_id = source.transaction_result_id,
        target.final_transaction_status = source.final_transaction_status,
        target."3d_flow_status" = source."3d_flow_status",
        target.challenge_preference = source.challenge_preference,
        target.preference_reason = source.preference_reason,
        target.authentication_flow = source.authentication_flow,
        target."3d_flow" = source."3d_flow",
        target.is_void = source.is_void,
        target.liability_shift = source.liability_shift,
        target.status = source.status,
        target.acs_url = source.acs_url,
        target.acs_res_authentication_status = source.acs_res_authentication_status,
        target.r_req_authentication_status = source.r_req_authentication_status,
        target.transaction_status_reason = source.transaction_status_reason,
        target.interaction_counter = source.interaction_counter,
        target.challenge_cancel = source.challenge_cancel,
        target.three_ds_method_indication = source.three_ds_method_indication,
        target.is_sale_3d = source.is_sale_3d,
        target.manage_3d_decision = source.manage_3d_decision,
        target.decline_reason = source.decline_reason,
        target.amount_in_usd = source.amount_in_usd,
        target.approved_amount_in_usd = source.approved_amount_in_usd,
        target.original_currency_amount = source.original_currency_amount,
        target.rate_usd = source.rate_usd,
        target.currency_code = source.currency_code,
        target.three_ds_protocol_version = source.three_ds_protocol_version,
        target.is_external_mpi = source.is_external_mpi,
        target.rebill = source.rebill,
        target.device_channel = source.device_channel,
        target.user_agent_3d = source.user_agent_3d,
        target.device_type = source.device_type,
        target.device_name = source.device_name,
        target.device_os = source.device_os,
        target.challenge_window_size = source.challenge_window_size,
        target.type_of_authentication_method = source.type_of_authentication_method,
        target.multi_client_id = source.multi_client_id,
        target.client_id = source.client_id,
        target.multi_client_name = source.multi_client_name,
        target.client_name = source.client_name,
        target.industry_code = source.industry_code,
        target.inserted_at = source.inserted_at
WHEN NOT MATCHED THEN 
    INSERT (
        transaction_main_id, transaction_date, transaction_id_life_cycle, transaction_date_life_cycle,
        transaction_type_id, transaction_type, transaction_result_id, final_transaction_status,
        "3d_flow_status", challenge_preference, preference_reason, authentication_flow,
        "3d_flow", is_void, liability_shift, status, acs_url, acs_res_authentication_status,
        r_req_authentication_status, transaction_status_reason, interaction_counter,
        challenge_cancel, three_ds_method_indication, is_sale_3d, manage_3d_decision,
        decline_reason, amount_in_usd, approved_amount_in_usd, original_currency_amount,
        rate_usd, currency_code, three_ds_protocol_version, is_external_mpi, rebill,
        device_channel, user_agent_3d, device_type, device_name, device_os,
        challenge_window_size, type_of_authentication_method, multi_client_id, client_id,
        multi_client_name, client_name, industry_code, inserted_at
    )
    VALUES (
        source.transaction_main_id, source.transaction_date, source.transaction_id_life_cycle, 
        source.transaction_date_life_cycle, source.transaction_type_id, source.transaction_type,
        source.transaction_result_id, source.final_transaction_status, source."3d_flow_status",
        source.challenge_preference, source.preference_reason, source.authentication_flow,
        source."3d_flow", source.is_void, source.liability_shift, source.status,
        source.acs_url, source.acs_res_authentication_status, source.r_req_authentication_status,
        source.transaction_status_reason, source.interaction_counter, source.challenge_cancel,
        source.three_ds_method_indication, source.is_sale_3d, source.manage_3d_decision,
        source.decline_reason, source.amount_in_usd, source.approved_amount_in_usd,
        source.original_currency_amount, source.rate_usd, source.currency_code,
        source.three_ds_protocol_version, source.is_external_mpi, source.rebill,
        source.device_channel, source.user_agent_3d, source.device_type, source.device_name,
        source.device_os, source.challenge_window_size, source.type_of_authentication_method,
        source.multi_client_id, source.client_id, source.multi_client_name, source.client_name,
        source.industry_code, source.inserted_at
    )
;

-- ================================================
-- STEP 6: Update Checkpoint (equivalent to schema_mgr.update_metadata)
-- ================================================

-- Update checkpoint only if we processed rows
MERGE INTO NUVEI_DWH.NCP.METADATA_TABLE AS target
USING (SELECT $TARGET_TABLE AS table_name, $CURRENT_TIMESTAMP AS checkpoint, $TOTAL_ROWS AS row_count) AS source
ON target.table_name = source.table_name
WHEN MATCHED AND source.row_count > 0 THEN 
    UPDATE SET target.checkpoint = source.checkpoint
WHEN NOT MATCHED AND source.row_count > 0 THEN 
    INSERT (table_name, checkpoint) VALUES (source.table_name, source.checkpoint)
;

-- ================================================
-- STEP 7: Optimize Table (equivalent to OPTIMIZE command)
-- ================================================

-- Snowflake equivalent of Delta OPTIMIZE
-- Note: Snowflake automatically manages optimization, but we can cluster if needed
-- ALTER TABLE IDENTIFIER($TARGET_TABLE) CLUSTER BY (transaction_date, transaction_main_id);

-- Final logging
SELECT 
    'ETL Complete: ' || $CURRENT_TIMESTAMP AS timestamp,
    'Processed Rows: ' || $TOTAL_ROWS AS summary,
    'Target Table: ' || $TARGET_TABLE AS target_info;

-- ================================================
-- End of Bronze-to-Silver ETL Script
-- ================================================
