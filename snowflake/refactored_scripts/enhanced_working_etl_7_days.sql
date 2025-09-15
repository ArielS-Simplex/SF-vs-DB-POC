-- ==============================================================================
-- ENHANCED WORKING ETL - Based on final/02_bronze_to_silver_sept2-9.sql
-- Added missing columns for 173-column parity with current ETL
-- Status: Working row counts + business logic + expanded schema
-- ==============================================================================
-- BRONZE TO SILVER ETL - SEPTEMBER 2-8, 2025 (7 DAYS)
-- Processing 7-day range with complete 143-column Databricks parity
-- V2_7_DAYS VERSION - Multi-day processing
-- ==============================================================================

SET DATE_RANGE_START = '2025-09-02';
SET DATE_RANGE_END = '2025-09-08';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V2_7_DAYS';

-- ==============================================================================
-- 1. CLEAN SLATE - DROP AND RECREATE TARGET TABLE
-- ==============================================================================

DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

-- ==============================================================================
-- 2. EXACT DATABRICKS PARITY ETL - SEPTEMBER 5, 2025 (1 DAY - 143 COLUMNS)
-- ==============================================================================

CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) >= $DATE_RANGE_START
      AND DATE(transaction_date) <= $DATE_RANGE_END
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
),
status_flags_calculated AS (
SELECT 
    -- Keep all original columns
    *,
    
    -- DATABRICKS DERIVED COLUMNS - Transaction result status flags (FIXED CASE SENSITIVITY)
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'INITAUTH3D' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'INITAUTH3D' THEN FALSE
        ELSE NULL
    END AS init_status,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH3D' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH3D' THEN FALSE
        ELSE NULL
    END AS auth_3d_status,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'SALE' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'SALE' THEN FALSE
        ELSE NULL
    END AS sale_status,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH' THEN FALSE
        ELSE NULL
    END AS auth_status,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'SETTLE' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'SETTLE' THEN FALSE
        ELSE NULL
    END AS settle_status,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'VERIFY_AUTH_3D' AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'VERIFY_AUTH_3D' THEN FALSE
        ELSE NULL
    END AS verify_auth_3d_status,
    
    -- DATABRICKS DERIVED COLUMNS - Conditional copies
    CASE 
        WHEN LOWER(TRIM(COALESCE(transaction_type, ''))) = 'auth3d' THEN CASE 
            WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
            ELSE NULL
        END
        ELSE NULL
    END AS is_sale_3d_auth_3d,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(transaction_type, ''))) = 'auth3d' THEN CASE 
            WHEN LOWER(TRIM(COALESCE(manage_3d_decision, ''))) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(COALESCE(manage_3d_decision, ''))) IN ('no', 'false', '0', '') THEN FALSE
            ELSE NULL
        END
        ELSE NULL
    END AS manage_3d_decision_auth_3d

FROM filtered_data
)

SELECT 
    -- Core transaction fields
    transaction_main_id,
    transaction_date,
    
    -- Boolean normalization - EXACT Databricks logic using actual columns
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_void,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_sale_3d,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_external_mpi, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_external_mpi, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_external_mpi,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_prepaid, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_prepaid, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_prepaid,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_3d,
    
    -- DATABRICKS DERIVED COLUMNS - Reference calculated status flags
    init_status,
    auth_3d_status,
    sale_status,
    auth_status,
    settle_status,
    verify_auth_3d_status,
    
    -- DATABRICKS DERIVED COLUMNS - Conditional copies (from CTE)
    is_sale_3d_auth_3d,
    manage_3d_decision_auth_3d,
    
    -- DATABRICKS DERIVED COLUMNS - 3D Secure success analysis (FIXED CASE SENSITIVITY)
    CASE 
        WHEN UPPER(TRIM(COALESCE(threed_flow_status, ''))) = '3D_SUCCESS' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(threed_flow_status, ''))) IN ('3D_FAILURE', '3D_WASNT_COMPLETED') THEN FALSE
        ELSE NULL
    END AS is_successful_challenge,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(authentication_flow, ''))) = 'EXEMPTION' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(challenge_preference, ''))) = 'Y_REQUESTED_BY_ACQUIRER' THEN FALSE
        ELSE NULL
    END AS is_successful_exemption,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(authentication_flow, ''))) = 'FRICTIONLESS' AND TRIM(COALESCE(status, '')) = '40' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(authentication_flow, ''))) = 'FRICTIONLESS' THEN FALSE
        ELSE NULL
    END AS is_successful_frictionless,
    
    -- DATABRICKS DERIVED COLUMNS - Successful authentication (complex logic) (FIXED CASE SENSITIVITY)
    CASE 
        WHEN UPPER(TRIM(COALESCE(threed_flow_status, ''))) = '3D_SUCCESS' 
          OR (UPPER(TRIM(COALESCE(authentication_flow, ''))) = 'FRICTIONLESS' AND TRIM(COALESCE(status, '')) = '40') THEN TRUE
        WHEN (TRIM(COALESCE(acs_url, '')) IS NOT NULL AND TRIM(COALESCE(acs_url, '')) != '' AND UPPER(TRIM(COALESCE(authentication_flow, ''))) != 'EXEMPTION')
          OR (UPPER(TRIM(COALESCE(authentication_flow, ''))) = 'FRICTIONLESS' AND TRIM(COALESCE(status, '')) != '40' AND TRIM(COALESCE(status, '')) != '') THEN FALSE
        ELSE NULL
    END AS is_successful_authentication,
    
    -- DATABRICKS DERIVED COLUMNS - High-level approval/decline logic (FIXED - now references status flags)
    CASE 
        WHEN auth_status = TRUE OR sale_status = TRUE THEN TRUE
        WHEN auth_status = FALSE OR sale_status = FALSE THEN FALSE
        ELSE NULL
    END AS is_approved,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) IN ('SALE', 'AUTH') AND TRIM(COALESCE(transaction_result_id, '')) = '1008' THEN TRUE
        WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN FALSE
        ELSE NULL
    END AS is_declined,
    
    -- String cleaning - exact Databricks approach using actual columns
    CASE 
        WHEN TRIM(COALESCE(transaction_type, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(transaction_type, ''), '[^A-Za-z0-9\\s]', '')))
    END AS transaction_type,
    
    CASE 
        WHEN TRIM(COALESCE(multi_client_name, '')) = '' THEN NULL
        ELSE TRIM(REGEXP_REPLACE(COALESCE(multi_client_name, ''), '[^A-Za-z0-9\\s]', ''))
    END AS multi_client_name,
    
    CASE 
        WHEN TRIM(COALESCE(final_transaction_status, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(final_transaction_status, ''), '[^A-Za-z0-9\\s]', '')))
    END AS final_transaction_status,
    
    CASE 
        WHEN TRIM(COALESCE(card_scheme, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(card_scheme, ''), '[^A-Za-z0-9\\s]', '')))
    END AS card_scheme,
    
    -- Core Databricks fields we need for perfect parity
    transaction_result_id,
    threed_flow_status AS three_ds_flow_status,  -- Map bronze->silver column names
    challenge_preference,
    authentication_flow,
    status,
    acs_url,
    transaction_id_life_cycle,
    decline_reason,
    
    -- MISSING CORE COLUMNS - Adding all 100+ missing columns from Databricks schema
    transaction_date_life_cycle,
    transaction_type_id,
    preference_reason,
    
    -- 3D Secure columns (using actual bronze column names)
    threed_flow_status AS "3d_flow_status",  -- Databricks expects this name
    threed_flow AS "3d_flow",  -- Databricks expects this name
    CASE 
        WHEN LOWER(TRIM(COALESCE(liability_shift, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(liability_shift, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS liability_shift,
    acs_res_authentication_status,
    r_req_authentication_status,
    transaction_status_reason,
    interaction_counter,
    challenge_cancel,
    three_ds_method_indication,
    manage_3d_decision,    -- Financial columns
    rate_usd,
    currency_code,
    three_ds_protocol_version,
    
    -- Device and channel columns
    rebill,
    device_channel,
    user_agent_3d,
    device_type,
    device_name,
    device_os,
    challenge_window_size,
    type_of_authentication_method,
    
    -- Client identification columns
    multi_client_id,
    client_id,
    client_name,
    industry_code,
    
    -- Card information columns
    credit_card_id,
    cccid,
    bin,
    card_type,
    consumer_id,
    issuer_bank_name,
    device_channel_name,
    bin_country,
    
    -- Geographic and regulatory columns
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_eea, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_eea, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_eea,
    region,
    payment_instrument,
    source_application,
    
    -- Partial amount processing columns
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_partial_amount, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_partial_amount, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_partial_amount,
    CASE 
        WHEN LOWER(TRIM(COALESCE(enable_partial_approval, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(enable_partial_approval, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS enable_partial_approval,
    CASE 
        WHEN LOWER(TRIM(COALESCE(partial_approval_is_void, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(partial_approval_is_void, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS partial_approval_is_void,
    partial_approval_void_id,
    partial_approval_void_time,
    partial_approval_requested_amount,
    partial_approval_requested_currency,
    partial_approval_processed_amount,
    partial_approval_processed_currency,
    COALESCE(TRY_CAST(partial_approval_processed_amount_in_usd AS DECIMAL(18,2)), 0) AS partial_approval_processed_amount_in_usd,
    
    -- Website and browser columns
    website_id,
    browser_user_agent,
    ip_country,
    processor_id,
    processor_name,
    
    -- Risk and fraud columns
    risk_email_id,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_currency_converted, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_currency_converted, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_currency_converted,
    TRY_CAST(email_seniority_start_date AS TIMESTAMP) AS email_seniority_start_date,
    email_payment_attempts,
    final_fraud_decision_id,
    
    -- Token and security columns
    external_token_eci,
    risk_threed_eci,
    threed_eci,
    cvv_code,
    provider_response_code,
    issuer_card_program_id,
    
    -- Transaction flow columns
    scenario_id,
    previous_id,
    next_id,
    step,
    reprocess_3d_reason,
    data_only_authentication_result,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_cascaded_after_data_only_authentication, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_cascaded_after_data_only_authentication, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_cascaded_after_data_only_authentication,
    next_action,
    authentication_method,
    cavv_verification_code,
    channel,
    
    -- Authentication request/response columns
    authentication_request,
    authentication_response,
    
    -- Card details columns
    cc_hash,
    TRY_CAST(exp_date AS TIMESTAMP) AS exp_date,
    message_version_3d,
    TRY_CAST(cc_seniority_start_date AS TIMESTAMP) AS cc_seniority_start_date,
    mc_scheme_token_used,
    stored_credentials_mode,
    avs_code,
    credit_type_id,
    subscription_step,
    
    -- Token fetching columns
    scheme_token_fetching_result,
    browser_screen_height,
    browser_screen_width,
    filter_reason_id,
    reason_code,
    reason,
    
    -- Service timestamp columns
    request_timestamp_service,
    token_unique_reference_service,
    response_timestamp_service,
    api_type_service,
    request_timestamp_fetching,
    token_unique_reference_fetching,
    response_timestamp_fetching,
    api_type_fetching,
    
    -- Token processing flags
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_cryptogram_fetching_skipped, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_cryptogram_fetching_skipped, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_cryptogram_fetching_skipped,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_external_scheme_token, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_external_scheme_token, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_external_scheme_token,
    
    -- 3DS server and gateway columns
    three_ds_server_trans_id,
    gateway_id,
    cc_request_type_id,
    upo_id,
    
    -- Additional boolean flags (fix column names from bronze)
    iscardReplaced AS IsCardReplaced,
    isvdcuFeeApplied AS IsVdcuFeeApplied,
    aftType AS AftType,
    secondarycccid,
    
    -- Duration columns
    transaction_duration,
    authorization_req_duration,
    
    -- Installment columns (fix column names from bronze)
    firstInstallment AS FirstInstallment,
    periodicalInstallment AS PeriodicalInstallment,
    numberOfInstallments,
    installmentProgram AS InstallmentProgram,
    installmentFundingType AS InstallmentFundingType,
    COALESCE(TRY_CAST(first_installment_usd AS DECIMAL(18,2)), 0) AS first_installment_usd,
    COALESCE(TRY_CAST(periodical_installment_usd AS DECIMAL(18,2)), 0) AS periodical_installment_usd,
    
    -- Advanced processing columns (fix column names from bronze)
    applicableScenarios AS ApplicableScenarios,
    cascading_ab_test_experimant_name,
    
    -- Integer flags (Databricks specific) - Add missing ones as NULL
    NULL AS IsOnlineRefund,
    NULL AS IsNoCVV, 
    NULL AS IsSupportedOCT,
    
    -- Transaction type and merchant columns - Add missing ones as NULL
    NULL AS ExternalTokenTrasactionType,
    NULL AS SubscriptionType,
    NULL AS MerchantCountryCodeNum,
    NULL AS MCMerchantAdviceCode,
    NULL AS AcquirerBinCountryId,
    NULL AS AcquirerBin,
    
    -- Regulatory compliance flags - Add missing ones as NULL
    NULL AS IsPSD2,
    NULL AS IsSCAScope,
    NULL AS IsAirline,
    NULL AS RequestedCCCID,
    
    -- Final geographic column - Add missing as NULL
    NULL AS merchant_country,
    
    -- Numeric fields using actual columns - now proper STRING columns from bronze
    COALESCE(TRY_CAST(amount_in_usd AS DECIMAL(18,2)), 0) AS amount_in_usd,
    COALESCE(TRY_CAST(approved_amount_in_usd AS DECIMAL(18,2)), 0) AS approved_amount_in_usd,
    COALESCE(TRY_CAST(original_currency_amount AS DECIMAL(18,2)), 0) AS original_currency_amount,
    
    -- Metadata (keep at the end)
    inserted_at
    
FROM status_flags_calculated
ORDER BY transaction_date, transaction_main_id;
