-- ==============================================================================
-- COMPLETE ETL PIPELINE - PRODUCTION READY
-- Setup + Data Transformation + Table Creation in one script
-- ==============================================================================

-- ENVIRONMENT SETUP
SET CLOUD_PROVIDER = 'Snowflake';
SET TEST_CLIENTS = 'test multi,davidh test2 multi,ice demo multi,monitoring client pod2 multi';
SET FORCE_NULL_COLUMNS = 'user_agent_3d,authentication_request,authentication_response,authorization_req_duration';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET curr_timestamp = CURRENT_TIMESTAMP();

-- METADATA SETUP
CREATE TABLE IF NOT EXISTS POC.PUBLIC.metadata_table (
    table_name STRING,
    schema_json STRING,
    checkpoint TIMESTAMP_TZ,
    source_table STRING,
    table_keys STRING
);

MERGE INTO POC.PUBLIC.metadata_table AS target
USING (SELECT $TARGET_TABLE AS table_name) AS source
ON target.table_name = source.table_name
WHEN NOT MATCHED THEN 
    INSERT (table_name, source_table, table_keys) 
    VALUES ($TARGET_TABLE, $SOURCE_TABLE, 'TRANSACTION_MAIN_ID,TRANSACTION_DATE');

SET checkpoint_time = (SELECT checkpoint FROM POC.PUBLIC.metadata_table WHERE table_name = $TARGET_TABLE);
SET target_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'TRANSACTIONS_SILVER');

-- SCHEMA EVOLUTION SETUP
CREATE OR REPLACE TEMPORARY TABLE schema_comparison AS (
    WITH source_columns AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'NCP_BRONZE'
    ),
    target_columns AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'TRANSACTIONS_SILVER'
        AND $target_exists > 0
    ),
    new_columns AS (
        SELECT s.COLUMN_NAME, s.DATA_TYPE
        FROM source_columns s
        LEFT JOIN target_columns t ON s.COLUMN_NAME = t.COLUMN_NAME
        WHERE t.COLUMN_NAME IS NULL AND $target_exists > 0
    )
    SELECT * FROM new_columns
);

-- ETL TRANSFORMATION AND TABLE CREATION
CREATE OR REPLACE TEMPORARY TABLE processed_data AS (
    WITH incremental_data AS (
        -- Step 1: LAST 3 DAYS OF DATA FOR PRODUCTION RUN
        SELECT *
        FROM IDENTIFIER($SOURCE_TABLE)
        WHERE transaction_date >= DATEADD(day, -3, CURRENT_DATE())
          AND transaction_date < CURRENT_DATE()
    ),

    cleaned_data AS (
        -- Step 2: Data quality checks and error handling
        SELECT *,
            CASE WHEN transaction_main_id IS NULL OR transaction_main_id = '' THEN 'INVALID_ID' ELSE 'VALID' END AS data_quality_flag,
            CASE WHEN transaction_date IS NULL OR TRY_TO_TIMESTAMP(transaction_date::STRING) IS NULL THEN 'BAD_DATE' ELSE 'VALID_DATE' END AS date_quality_flag
        FROM incremental_data
        WHERE transaction_main_id IS NOT NULL 
          AND transaction_date IS NOT NULL
    ),

    databricks_style_cleaned AS (
        -- Step 3: COMPLETE DATABRICKS DATA TYPE FIXING AND NORMALIZATION
        SELECT 
            -- Keep all original columns but apply Databricks transformations
            transaction_main_id,
            transaction_date,
            transaction_id_life_cycle,
            transaction_date_life_cycle,
            transaction_type_id,
            transaction_type,
            transaction_result_id,
            final_transaction_status,
            threed_flow_status,
            challenge_preference,
            preference_reason,
            authentication_flow,
            threed_flow,
            
            -- BOOLEAN FIELDS: Apply Databricks boolean normalization
            CASE 
                WHEN LOWER(TRIM(is_void)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_void)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_void,
            
            CASE 
                WHEN LOWER(TRIM(liability_shift)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(liability_shift)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS liability_shift,
            
            CASE 
                WHEN LOWER(TRIM(is_sale_3d)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_sale_3d)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_sale_3d,
            
            CASE 
                WHEN LOWER(TRIM(manage_3d_decision)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(manage_3d_decision)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS manage_3d_decision,
            
            CASE 
                WHEN LOWER(TRIM(is_external_mpi)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_external_mpi)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_external_mpi,
            
            CASE 
                WHEN LOWER(TRIM(rebill)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(rebill)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS rebill,
            
            CASE 
                WHEN LOWER(TRIM(is_prepaid)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_prepaid)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_prepaid,
            
            CASE 
                WHEN LOWER(TRIM(is_eea)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_eea)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_eea,
            
            CASE 
                WHEN LOWER(TRIM(is_currency_converted)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_currency_converted)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_currency_converted,
            
            CASE 
                WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(mc_scheme_token_used)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS mc_scheme_token_used,
            
            CASE 
                WHEN LOWER(TRIM(is_3d)) IN ('true', '1', 'yes', '1.0') THEN true
                WHEN LOWER(TRIM(is_3d)) IN ('false', '0', 'no', '0.0') THEN false
                ELSE NULL
            END AS is_3d,
            
            -- STRING FIELDS: Apply Databricks string normalization
            CASE 
                WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1)
                ELSE TRIM(LOWER(status))
            END AS status,
            
            CASE 
                WHEN LOWER(TRIM(acs_url)) IN ('<na>', 'na', 'nan', 'none', '', ' ', 'deprecated') THEN NULL
                ELSE TRIM(LOWER(acs_url))
            END AS acs_url,
            
            CASE 
                WHEN LOWER(TRIM(user_agent_3d)) = 'deprecated' THEN NULL
                ELSE TRIM(LOWER(user_agent_3d))
            END AS user_agent_3d,
            
            CASE 
                WHEN LOWER(TRIM(authentication_request)) = 'deprecated' THEN NULL
                ELSE TRIM(LOWER(authentication_request))
            END AS authentication_request,
            
            CASE 
                WHEN LOWER(TRIM(authentication_response)) = 'deprecated' THEN NULL
                ELSE TRIM(LOWER(authentication_response))
            END AS authentication_response,
            
            -- NUMERIC FIELDS: Handle NaN and convert properly
            CASE 
                WHEN amount_in_usd IS NULL THEN NULL
                ELSE TRY_TO_NUMBER(amount_in_usd)
            END AS amount_in_usd,
            
            CASE 
                WHEN approved_amount_in_usd IS NULL THEN NULL
                ELSE TRY_TO_NUMBER(approved_amount_in_usd)
            END AS approved_amount_in_usd,
            
            CASE 
                WHEN original_currency_amount IS NULL THEN NULL
                ELSE TRY_TO_NUMBER(original_currency_amount)
            END AS original_currency_amount,
            
            CASE 
                WHEN rate_usd IS NULL THEN NULL
                ELSE TRY_TO_NUMBER(rate_usd)
            END AS rate_usd,
            
            -- ALL REMAINING COLUMNS: Pass through exactly as they are
            acs_res_authentication_status, r_req_authentication_status, transaction_status_reason,
            interaction_counter, challenge_cancel, three_ds_method_indication, decline_reason,
            currency_code, three_ds_protocol_version, device_channel, device_type, device_name,
            device_os, challenge_window_size, type_of_authentication_method, multi_client_id,
            client_id, multi_client_name, client_name, industry_code, credit_card_id, cccid,
            bin, card_scheme, card_type, consumer_id, issuer_bank_name, device_channel_name,
            bin_country, region, payment_instrument, source_application, is_partial_amount,
            enable_partial_approval, partial_approval_is_void, partial_approval_void_id,
            partial_approval_void_time, partial_approval_requested_amount, partial_approval_requested_currency,
            partial_approval_processed_amount, partial_approval_processed_currency, partial_approval_processed_amount_in_usd,
            website_id, browser_user_agent, ip_country, processor_id, processor_name, risk_email_id,
            email_seniority_start_date, email_payment_attempts, final_fraud_decision_id, external_token_eci,
            risk_threed_eci, threed_eci, cvv_code, provider_response_code, issuer_card_program_id,
            scenario_id, previous_id, next_id, step, reprocess_3d_reason, data_only_authentication_result,
            is_cascaded_after_data_only_authentication, next_action, authentication_method, cavv_verification_code,
            channel, cc_hash, exp_date, message_version_3d, cc_seniority_start_date, inserted_at,
            stored_credentials_mode, avs_code, credit_type_id, subscription_step, scheme_token_fetching_result,
            browser_screen_height, browser_screen_width, filter_reason_id, reason_code, reason,
            request_timestamp_service, token_unique_reference_service, response_timestamp_service, api_type_service,
            request_timestamp_fetching, token_unique_reference_fetching, response_timestamp_fetching, api_type_fetching,
            is_cryptogram_fetching_skipped, is_external_scheme_token, three_ds_server_trans_id, gateway_id,
            cc_request_type_id, upo_id, iscardReplaced, isvdcuFeeApplied, aftType, secondarycccid,
            transaction_duration, authorization_req_duration, firstInstallment, periodicalInstallment,
            numberOfInstallments, installmentProgram, installmentFundingType, first_installment_usd,
            periodical_installment_usd, applicableScenarios, cascading_ab_test_experimant_name, raw_line,
            
            -- Keep the added data quality columns from previous CTE
            data_quality_flag, date_quality_flag
        FROM cleaned_data
    ),

    deduplicated_data AS (
        -- Step 4: Remove duplicates
        SELECT *
        FROM databricks_style_cleaned
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) = 1
    ),

    filtered_data AS (
        -- Step 5: Apply test client filter - FIXED FOR CASE SENSITIVITY
        SELECT *
        FROM deduplicated_data
        WHERE LOWER(multi_client_name) NOT IN (
            'test multi', 
            'davidh test2 multi', 
            'ice demo multi', 
            'monitoring client pod2 multi'
        )
    ),

    with_status_flags AS (
        -- Step 6: Add transaction status flags - SIMPLIFIED AND FIXED
        SELECT *,
            threed_flow_status AS "3d_flow_status",
            
            -- FIXED STATUS FLAGS: Use correct case-sensitive transaction types
            CASE WHEN transaction_type = 'InitAuth3D' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS init_status,
            
            CASE WHEN transaction_type = 'Auth3D' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS auth_3d_status,
            
            CASE WHEN transaction_type = 'Sale' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS sale_status,
            
            CASE WHEN transaction_type = 'Auth' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS auth_status,
            
            CASE WHEN transaction_type = 'Settle' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS settle_status,
            
            CASE WHEN transaction_type = 'verify_auth_3d' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS verify_auth_3d_status,
            
            -- SUCCESS METRICS: Match your actual data values
            CASE 
                WHEN threed_flow_status = '3D_Success' THEN true
                WHEN threed_flow_status = '3D_Failure' THEN false
                ELSE NULL
            END AS is_successful_challenge,
            
            CASE 
                WHEN authentication_flow = 'Exemption' THEN true
                ELSE NULL
            END AS is_successful_exemption,
            
            CASE 
                WHEN authentication_flow = 'Frictionless' AND status = '40' THEN true
                WHEN authentication_flow = 'Frictionless' AND status != '40' THEN false
                ELSE NULL
            END AS is_successful_frictionless,
            
            CASE 
                WHEN threed_flow_status = '3D_Success' THEN true
                WHEN authentication_flow = 'Frictionless' AND status = '40' THEN true
                WHEN authentication_flow = 'Exemption' THEN true
                WHEN threed_flow_status = '3D_Failure' THEN false
                WHEN authentication_flow = 'Frictionless' AND status != '40' THEN false
                ELSE NULL
            END AS is_successful_authentication
            
        FROM filtered_data
    ),

    final_data AS (
        -- Step 7: Add derived business logic - FIXED FOR ACTUAL TRANSACTION TYPES
        SELECT *,
            CASE 
                WHEN auth_status = true OR sale_status = true THEN true
                WHEN auth_status = false OR sale_status = false THEN false
                ELSE NULL
            END AS is_approved,
            
            CASE 
                WHEN transaction_type IN ('Sale', 'Auth') AND transaction_result_id = '1008' THEN true
                WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN false
                ELSE NULL
            END AS is_declined
            
        FROM with_status_flags
    )

    SELECT * FROM final_data
);

-- CREATE TARGET TABLE AND LOAD DATA
SET total_rows = (SELECT COUNT(*) FROM processed_data);

DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
SELECT * FROM processed_data WHERE 1=0;

INSERT INTO IDENTIFIER($TARGET_TABLE)
SELECT * FROM processed_data;

-- UPDATE CHECKPOINT
MERGE INTO POC.PUBLIC.metadata_table AS target
USING (SELECT $TARGET_TABLE AS table_name, $total_rows AS row_count) AS source
ON target.table_name = source.table_name
WHEN MATCHED AND source.row_count > 0 THEN 
    UPDATE SET checkpoint = $curr_timestamp;

-- CLEANUP
DROP TABLE IF EXISTS processed_data;
DROP TABLE IF EXISTS schema_comparison;
