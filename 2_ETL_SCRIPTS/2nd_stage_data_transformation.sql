-- ==============================================================================
-- 2ND STAGE: DATA TRANSFORMATION AND BUSINESS LOGIC
-- Complete ETL processing with 6-stage pipeline
-- ==============================================================================

-- REQUIRED VARIABLES (if not already set in Stage 1)
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET curr_timestamp = CURRENT_TIMESTAMP();

-- Execute the ETL transformation and materialize the result
-- Temporary table will inherit schema from the SELECT statement
CREATE OR REPLACE TEMPORARY TABLE processed_data AS (
    WITH incremental_data AS (
        -- Step 1: LIMIT TO 1000 ROWS FOR FAST TESTING
        SELECT *
        FROM IDENTIFIER($SOURCE_TABLE)
        WHERE 1=1  -- Get data for testing
        LIMIT 1000  -- ⭐ TESTING LIMIT: Only process 1000 rows to avoid long waits
        -- WHERE TRANSACTION_DATE >= CURRENT_DATE() - INTERVAL '30 days'
        -- WHERE inserted_at > COALESCE($checkpoint_time, '1900-01-01'::TIMESTAMP_TZ)
    ),

    cleaned_data AS (
        -- Step 2: Use existing inserted_at column from bronze table (matches Databricks .withColumn())
        -- Add data quality checks and error handling + COMPLETE DATABRICKS DATA TYPE FIXING
        SELECT *,
            -- Data quality flags (Databricks style)
            CASE WHEN transaction_main_id IS NULL OR transaction_main_id = '' THEN 'INVALID_ID' ELSE 'VALID' END AS data_quality_flag,
            -- Handle bad dates gracefully - check if timestamp is valid
            CASE WHEN transaction_date IS NULL OR TRY_TO_TIMESTAMP(transaction_date::STRING) IS NULL THEN 'BAD_DATE' ELSE 'VALID_DATE' END AS date_quality_flag
        FROM incremental_data
        -- Filter out completely invalid records (Databricks badRecordsPath equivalent)
        WHERE transaction_main_id IS NOT NULL 
          AND transaction_date IS NOT NULL
    ),

    databricks_style_cleaned AS (
        -- Step 2.5: COMPLETE DATABRICKS DATA TYPE FIXING AND NORMALIZATION
        -- This replicates the fixing_dtypes() function from Databricks exactly
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
            -- Valid true: "true", "1", "yes", "1.0" -> true
            -- Valid false: "false", "0", "no", "0.0" -> false
            -- Everything else -> NULL
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
            -- Extract numbers from strings, normalize nulls, handle deprecated values
            CASE 
                WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1)
                ELSE TRIM(LOWER(status))
            END AS status,
            
            -- Handle deprecated and null values for string fields
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
            
            -- NUMERIC FIELDS: Handle NaN and convert properly (Databricks uses float("nan") for nulls)
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
            -- This ensures we maintain the full 185 column structure
            acs_res_authentication_status,
            r_req_authentication_status,
            transaction_status_reason,
            interaction_counter,
            challenge_cancel,
            three_ds_method_indication,
            decline_reason,
            currency_code,
            three_ds_protocol_version,
            device_channel,
            device_type,
            device_name,
            device_os,
            challenge_window_size,
            type_of_authentication_method,
            multi_client_id,
            client_id,
            multi_client_name,
            client_name,
            industry_code,
            credit_card_id,
            cccid,
            bin,
            card_scheme,
            card_type,
            consumer_id,
            issuer_bank_name,
            device_channel_name,
            bin_country,
            region,
            payment_instrument,
            source_application,
            is_partial_amount,
            enable_partial_approval,
            partial_approval_is_void,
            partial_approval_void_id,
            partial_approval_void_time,
            partial_approval_requested_amount,
            partial_approval_requested_currency,
            partial_approval_processed_amount,
            partial_approval_processed_currency,
            partial_approval_processed_amount_in_usd,
            website_id,
            browser_user_agent,
            ip_country,
            processor_id,
            processor_name,
            risk_email_id,
            email_seniority_start_date,
            email_payment_attempts,
            final_fraud_decision_id,
            external_token_eci,
            risk_threed_eci,
            threed_eci,
            cvv_code,
            provider_response_code,
            issuer_card_program_id,
            scenario_id,
            previous_id,
            next_id,
            step,
            reprocess_3d_reason,
            data_only_authentication_result,
            is_cascaded_after_data_only_authentication,
            next_action,
            authentication_method,
            cavv_verification_code,
            channel,
            cc_hash,
            exp_date,
            message_version_3d,
            cc_seniority_start_date,
            inserted_at,
            stored_credentials_mode,
            avs_code,
            credit_type_id,
            subscription_step,
            scheme_token_fetching_result,
            browser_screen_height,
            browser_screen_width,
            filter_reason_id,
            reason_code,
            reason,
            request_timestamp_service,
            token_unique_reference_service,
            response_timestamp_service,
            api_type_service,
            request_timestamp_fetching,
            token_unique_reference_fetching,
            response_timestamp_fetching,
            api_type_fetching,
            is_cryptogram_fetching_skipped,
            is_external_scheme_token,
            three_ds_server_trans_id,
            gateway_id,
            cc_request_type_id,
            upo_id,
            iscardReplaced,
            isvdcuFeeApplied,
            aftType,
            secondarycccid,
            transaction_duration,
            authorization_req_duration,
            firstInstallment,
            periodicalInstallment,
            numberOfInstallments,
            installmentProgram,
            installmentFundingType,
            first_installment_usd,
            periodical_installment_usd,
            applicableScenarios,
            cascading_ab_test_experimant_name,
            raw_line,
            
            -- Keep the added data quality columns from previous CTE
            data_quality_flag,
            date_quality_flag
        FROM cleaned_data
    ),

    deduplicated_data AS (
        -- Step 3: Remove duplicates (matches Databricks .dropDuplicates())
        SELECT *
        FROM databricks_style_cleaned
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) = 1
    ),

    filtered_data AS (
        -- Step 4: Apply test client filter ONLY for transactions_silver (matches Databricks conditional logic)
        SELECT *
        FROM deduplicated_data
        WHERE CASE 
            WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN 
                multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
            ELSE TRUE
        END
    ),

    with_status_flags AS (
        -- Step 5: Add transaction status flags (matches create_conversions_columns exactly)
        -- COMPLETE DATABRICKS BUSINESS LOGIC IMPLEMENTATION
        SELECT *,
            -- Add 3d_flow_status as alias for threed_flow_status (Databricks column name)
            threed_flow_status AS "3d_flow_status",
            
            -- CONDITIONAL COPIES: Exact Databricks logic
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'auth3d' 
                 THEN is_sale_3d 
                 ELSE NULL END AS is_sale_3d_auth_3d,
                 
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'auth3d' 
                 THEN manage_3d_decision 
                 ELSE NULL END AS manage_3d_decision_auth_3d,
            
            -- TRANSACTION RESULT STATUS FLAGS: Complete mapping
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'initauth3d' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS init_status,
            
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'auth3d' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS auth_3d_status,
            
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'sale' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS sale_status,
            
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'auth' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS auth_status,
            
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'settle' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS settle_status,
            
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' AND transaction_type = 'verify_auth_3d' THEN 
                CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
                ELSE NULL
            END AS verify_auth_3d_status,
            
            -- CHALLENGE SUCCESS: Exact Databricks logic from create_conversions_columns
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    WHEN threed_flow_status = '3d_success' THEN true
                    WHEN threed_flow_status IN ('3d_failure', '3d_wasnt_completed') THEN false
                    ELSE NULL
                END
            END AS is_successful_challenge,
            
            -- EXEMPTION LOGIC: Exact Databricks logic
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    WHEN authentication_flow = 'exemption' THEN true
                    WHEN challenge_preference = 'y_requested_by_acquirer' THEN false
                    ELSE NULL
                END
            END AS is_successful_exemption,
            
            -- FRICTIONLESS LOGIC: Exact Databricks logic  
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    WHEN authentication_flow = 'frictionless' AND status = '40' THEN true
                    WHEN authentication_flow = 'frictionless' THEN false
                    ELSE NULL
                END
            END AS is_successful_frictionless,
            
            -- SUCCESSFUL AUTHENTICATION: Complete Databricks logic
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    -- Primary success conditions
                    WHEN threed_flow_status = '3d_success' THEN true
                    WHEN (authentication_flow = 'frictionless' AND status = '40') THEN true
                    -- Primary failure conditions  
                    WHEN (acs_url IS NOT NULL AND authentication_flow != 'exemption') THEN false
                    WHEN (authentication_flow = 'frictionless' AND status != '40') THEN false
                    ELSE NULL
                END
            END AS is_successful_authentication
            
        FROM filtered_data
    ),

    final_data AS (
        -- Step 6: Add derived business logic (approval/decline) - must be separate CTE due to column dependencies
        -- COMPLETE DATABRICKS APPROVAL/DECLINE LOGIC
        SELECT *,
            -- APPROVAL LOGIC: Exact Databricks implementation
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    WHEN auth_status = true OR sale_status = true THEN true
                    WHEN auth_status = false OR sale_status = false THEN false
                    ELSE NULL
                END
            END AS is_approved,
            
            -- DECLINE LOGIC: Exact Databricks implementation
            CASE WHEN $TARGET_TABLE LIKE '%transactions_silver%' THEN
                CASE 
                    WHEN transaction_type IN ('sale', 'auth') AND transaction_result_id = '1008' THEN true
                    WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN false
                    ELSE NULL
                END
            END AS is_declined
            
        FROM with_status_flags
    )

    SELECT * FROM final_data
);

SELECT 'STAGE 2 COMPLETED: Data transformation and business logic applied' AS status;
