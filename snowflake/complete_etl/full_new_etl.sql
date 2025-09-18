
-- STEP 1: STAGING DATA LOADING - SEPTEMBER 5, 2025
-- This script loads raw production data into staging table
-- Run this FIRST, then run the bronze processing script

-- STEP 1: Create staging table for raw data loading - V2 VERSION
CREATE OR REPLACE TABLE poc.public.ncp_bronze_staging_v2 (
    filename STRING,
    loaded_at TIMESTAMP_NTZ,
    raw_line STRING
);

-- STEP 2: Create file format for raw line loading
CREATE OR REPLACE FILE FORMAT txt_format_raw
TYPE = 'CSV' 
FIELD_DELIMITER = NONE  -- No field delimiter - entire line is one field
SKIP_HEADER = 0 
ENCODING = 'ISO-8859-1' 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 3: Load production data - 7 DAYS (September 5, 2025)
-- Process full week of data for comprehensive testing - OPTIMIZED SINGLE COMMAND
COPY INTO poc.public.ncp_bronze_staging_v2 (filename, loaded_at, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-01.*' 
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;



-- BRONZE TABLE CREATION FROM STAGING DATA
-- This script processes staging data into bronze table with proper column types
-- RUN AFTER: 00_staging_data_loader.sql

-- STEP 2: Clear current bronze table and recreate with production data
-- Drop existing bronze table V2
DROP TABLE IF EXISTS poc.public.ncp_bronze_v2;

-- STEP 3: Recreate bronze table V2 with full 144-column structure using parsed production data - OPTIMIZED VERSION
CREATE TABLE poc.public.ncp_bronze_v2 AS
WITH parsed_data AS (
  SELECT 
    filename,
    loaded_at as inserted_at,
    SPLIT(raw_line, '\t') AS cols,
    raw_line
  FROM poc.public.ncp_bronze_staging_v2
  WHERE raw_line IS NOT NULL 
    AND raw_line != ''
    AND LENGTH(raw_line) > 100
)
SELECT 
    filename,
    inserted_at,
    
    cols[0]::STRING AS transaction_main_id,
    TRY_TO_TIMESTAMP(cols[1]::STRING) AS transaction_date,
    cols[2]::STRING AS transaction_id_life_cycle,
    TRY_TO_TIMESTAMP(cols[3]::STRING) AS transaction_date_life_cycle,
    cols[4]::STRING AS transaction_type_id,
    cols[5]::STRING AS transaction_type,
    cols[6]::STRING AS transaction_result_id,
    cols[7]::STRING AS final_transaction_status,
    cols[8]::STRING AS threed_flow_status,
    cols[9]::STRING AS challenge_preference,
    cols[10]::STRING AS preference_reason,
    cols[11]::STRING AS authentication_flow,
    cols[12]::STRING AS threed_flow,
    cols[13]::STRING AS is_void,
    cols[14]::STRING AS liability_shift,
    cols[15]::STRING AS status,
    cols[16]::STRING AS acs_url,
    cols[17]::STRING AS acs_res_authentication_status,
    cols[18]::STRING AS r_req_authentication_status,
    cols[19]::STRING AS transaction_status_reason,
    cols[20]::STRING AS interaction_counter,
    cols[21]::STRING AS challenge_cancel,
    cols[22]::STRING AS three_ds_method_indication,
    cols[23]::STRING AS is_sale_3d,
    cols[24]::STRING AS manage_3d_decision,
    cols[25]::STRING AS decline_reason,
    cols[26]::STRING AS amount_in_usd,
    cols[27]::STRING AS approved_amount_in_usd,
    cols[28]::STRING AS original_currency_amount,
    cols[29]::STRING AS rate_usd,
    cols[30]::STRING AS currency_code,
    cols[31]::STRING AS three_ds_protocol_version,
    cols[32]::STRING AS is_external_mpi,
    cols[33]::STRING AS rebill,
    cols[34]::STRING AS device_channel,
    cols[35]::STRING AS user_agent_3d,
    cols[36]::STRING AS device_type,
    cols[37]::STRING AS device_name,
    cols[38]::STRING AS device_os,
    cols[39]::STRING AS challenge_window_size,
    cols[40]::STRING AS type_of_authentication_method,
    cols[41]::STRING AS multi_client_id,
    cols[42]::STRING AS client_id,
    cols[43]::STRING AS multi_client_name,
    cols[44]::STRING AS client_name,
    cols[45]::STRING AS industry_code,
    cols[46]::STRING AS credit_card_id,
    cols[47]::STRING AS cccid,
    cols[48]::STRING AS bin,
    cols[49]::STRING AS is_prepaid,
    cols[50]::STRING AS card_scheme,
    cols[51]::STRING AS card_type,
    cols[52]::STRING AS consumer_id,
    cols[53]::STRING AS issuer_bank_name,
    cols[54]::STRING AS device_channel_name,
    cols[55]::STRING AS bin_country,
    cols[56]::STRING AS is_eea,
    cols[57]::STRING AS region,
    cols[58]::STRING AS payment_instrument,
    cols[59]::STRING AS source_application,
    cols[60]::STRING AS is_partial_amount,
    cols[61]::STRING AS enable_partial_approval,
    cols[62]::STRING AS partial_approval_is_void,
    cols[63]::STRING AS partial_approval_void_id,
    cols[64]::STRING AS partial_approval_void_time,
    cols[65]::STRING AS partial_approval_requested_amount,
    cols[66]::STRING AS partial_approval_requested_currency,
    cols[67]::STRING AS partial_approval_processed_amount,
    cols[68]::STRING AS partial_approval_processed_currency,
    cols[69]::STRING AS partial_approval_processed_amount_in_usd,
    cols[70]::STRING AS website_id,
    cols[71]::STRING AS browser_user_agent,
    cols[72]::STRING AS ip_country,
    cols[73]::STRING AS processor_id,
    cols[74]::STRING AS processor_name,
    cols[75]::STRING AS risk_email_id,
    cols[76]::STRING AS is_currency_converted,
    cols[77]::STRING AS email_seniority_start_date,
    cols[78]::STRING AS email_payment_attempts,
    cols[79]::STRING AS final_fraud_decision_id,
    cols[80]::STRING AS external_token_eci,
    cols[81]::STRING AS risk_threed_eci,
    cols[82]::STRING AS threed_eci,
    cols[83]::STRING AS cvv_code,
    cols[84]::STRING AS provider_response_code,
    cols[85]::STRING AS issuer_card_program_id,
    cols[86]::STRING AS scenario_id,
    cols[87]::STRING AS previous_id,
    cols[88]::STRING AS next_id,
    cols[89]::STRING AS step,
    cols[90]::STRING AS reprocess_3d_reason,
    cols[91]::STRING AS data_only_authentication_result,
    cols[92]::STRING AS is_cascaded_after_data_only_authentication,
    cols[93]::STRING AS next_action,
    cols[94]::STRING AS authentication_method,
    cols[95]::STRING AS cavv_verification_code,
    cols[96]::STRING AS channel,
    cols[97]::STRING AS authentication_request,
    cols[98]::STRING AS authentication_response,
    cols[99]::STRING AS cc_hash,
    cols[100]::STRING AS exp_date,
    cols[101]::STRING AS message_version_3d,
    cols[102]::STRING AS cc_seniority_start_date,
    cols[103]::STRING AS mc_scheme_token_used,
    cols[104]::STRING AS stored_credentials_mode,
    cols[105]::STRING AS avs_code,
    cols[106]::STRING AS is_3d,
    cols[107]::STRING AS credit_type_id,
    cols[108]::STRING AS subscription_step,
    cols[109]::STRING AS scheme_token_fetching_result,
    cols[110]::STRING AS browser_screen_height,
    cols[111]::STRING AS browser_screen_width,
    cols[112]::STRING AS filter_reason_id,
    cols[113]::STRING AS reason_code,
    cols[114]::STRING AS reason,
    cols[115]::STRING AS request_timestamp_service,
    cols[116]::STRING AS token_unique_reference_service,
    cols[117]::STRING AS response_timestamp_service,
    cols[118]::STRING AS api_type_service,
    cols[119]::STRING AS request_timestamp_fetching,
    cols[120]::STRING AS token_unique_reference_fetching,
    cols[121]::STRING AS response_timestamp_fetching,
    cols[122]::STRING AS api_type_fetching,
    cols[123]::STRING AS is_cryptogram_fetching_skipped,
    cols[124]::STRING AS is_external_scheme_token,
    cols[125]::STRING AS three_ds_server_trans_id,
    cols[126]::STRING AS gateway_id,
    cols[127]::STRING AS cc_request_type_id,
    cols[128]::STRING AS upo_id,
    cols[129]::STRING AS iscardReplaced,
    cols[130]::STRING AS isvdcuFeeApplied,
    cols[131]::STRING AS aftType,
    cols[132]::STRING AS secondarycccid,
    cols[133]::STRING AS transaction_duration,
    cols[134]::STRING AS authorization_req_duration,
    cols[135]::STRING AS firstInstallment,
    cols[136]::STRING AS periodicalInstallment,
    cols[137]::STRING AS numberOfInstallments,
    cols[138]::STRING AS installmentProgram,
    cols[139]::STRING AS installmentFundingType,
    cols[140]::STRING AS first_installment_usd,
    cols[141]::STRING AS periodical_installment_usd,
    cols[142]::STRING AS applicableScenarios,
    cols[143]::STRING AS cascading_ab_test_experimant_name,
    
    raw_line
    
FROM parsed_data;



-- ==============================================================================
-- INCREMENTAL ENHANCED ETL V1 - Based on enhanced_working_etl.sql
-- Converted from full table recreation to incremental MERGE processing
-- Features: Metadata table checkpoints + MERGE operations + 174-column parity
-- Status: Production-ready incremental ETL with Databricks-style checkpoint management
-- ==============================================================================
-- BRONZE TO SILVER INCREMENTAL ETL - DATABRICKS-STYLE PROCESSING
-- Daily incremental processing with metadata table checkpoint management
-- MERGE operations for upserts (INSERT + UPDATE)
-- ==============================================================================

-- ==============================================================================
-- INCREMENTAL PROCESSING VARIABLES
-- ==============================================================================
SET ETL_NAME = 'BRONZE_TO_SILVER_INCREMENTAL';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V2';
SET STAGING_TABLE = 'POC.PUBLIC.NCP_SILVER_V2_STAGING';
SET CHECKPOINT_TABLE = 'POC.PUBLIC.ETL_CHECKPOINT';

-- For manual daily processing (you'll update this date each day) - MATCH ORIGINAL EXACTLY
SET DATE_RANGE_START = '2025-09-01';
SET DATE_RANGE_END = '2025-09-01';

-- ==============================================================================
-- 1. METADATA TABLE INFRASTRUCTURE - DATABRICKS-STYLE CHECKPOINT MANAGEMENT
-- ==============================================================================

-- Create ETL_CHECKPOINT table if it doesn't exist (like Databricks Delta checkpoints)
CREATE TABLE IF NOT EXISTS IDENTIFIER($CHECKPOINT_TABLE) (
    etl_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_updated_timestamp TIMESTAMP,
    status STRING,
    records_processed INTEGER,
    PRIMARY KEY (etl_name)
);

-- Initialize checkpoint for first-time run
INSERT INTO IDENTIFIER($CHECKPOINT_TABLE) (etl_name, last_processed_timestamp, last_updated_timestamp, status, records_processed)
SELECT $ETL_NAME, '1900-01-01'::TIMESTAMP, CURRENT_TIMESTAMP(), 'INITIALIZED', 0
WHERE NOT EXISTS (SELECT 1 FROM IDENTIFIER($CHECKPOINT_TABLE) WHERE etl_name = $ETL_NAME);

-- Get last checkpoint timestamp
SET LAST_CHECKPOINT = (
    SELECT last_processed_timestamp 
    FROM IDENTIFIER($CHECKPOINT_TABLE) 
    WHERE etl_name = $ETL_NAME
);

-- ==============================================================================
-- 2. STAGING TABLE CREATION - INCREMENTAL DATA PROCESSING
-- ==============================================================================

-- Drop staging table if exists
DROP TABLE IF EXISTS IDENTIFIER($STAGING_TABLE);

-- Create staging table with transformed incremental data
CREATE TABLE IDENTIFIER($STAGING_TABLE) AS
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    -- MATCH ORIGINAL enhanced_working_etl.sql filtering EXACTLY
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

-- ==============================================================================
-- 3. TARGET TABLE CREATION (IF NOT EXISTS) - PRESERVE EXISTING DATA
-- ==============================================================================

-- Create target table with same structure if it doesn't exist
CREATE TABLE IF NOT EXISTS IDENTIFIER($TARGET_TABLE) AS 
SELECT * FROM IDENTIFIER($STAGING_TABLE) WHERE 1=0;  -- Empty table with correct schema

-- ==============================================================================
-- 4. MERGE OPERATIONS - DATABRICKS-STYLE UPSERTS (INSERT + UPDATE)
-- Complete MERGE with all 174 columns for fair POC comparison
-- ==============================================================================

MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING IDENTIFIER($STAGING_TABLE) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN UPDATE SET
    -- Core transaction fields
    target.transaction_main_id = source.transaction_main_id,
    target.transaction_date = source.transaction_date,
    target.is_void = source.is_void,
    target.is_sale_3d = source.is_sale_3d,
    target.is_external_mpi = source.is_external_mpi,
    target.is_prepaid = source.is_prepaid,
    target.is_3d = source.is_3d,
    
    -- Status flags
    target.init_status = source.init_status,
    target.auth_3d_status = source.auth_3d_status,
    target.sale_status = source.sale_status,
    target.auth_status = source.auth_status,
    target.settle_status = source.settle_status,
    target.verify_auth_3d_status = source.verify_auth_3d_status,
    
    -- Conditional copies
    target.is_sale_3d_auth_3d = source.is_sale_3d_auth_3d,
    target.manage_3d_decision_auth_3d = source.manage_3d_decision_auth_3d,
    
    -- 3D Secure success analysis
    target.is_successful_challenge = source.is_successful_challenge,
    target.is_successful_exemption = source.is_successful_exemption,
    target.is_successful_frictionless = source.is_successful_frictionless,
    target.is_successful_authentication = source.is_successful_authentication,
    
    -- Approval/decline logic
    target.is_approved = source.is_approved,
    target.is_declined = source.is_declined,
    
    -- String cleaned fields
    target.transaction_type = source.transaction_type,
    target.multi_client_name = source.multi_client_name,
    target.final_transaction_status = source.final_transaction_status,
    target.card_scheme = source.card_scheme,
    
    -- Core fields
    target.transaction_result_id = source.transaction_result_id,
    target.three_ds_flow_status = source.three_ds_flow_status,
    target.challenge_preference = source.challenge_preference,
    target.authentication_flow = source.authentication_flow,
    target.status = source.status,
    target.acs_url = source.acs_url,
    target.transaction_id_life_cycle = source.transaction_id_life_cycle,
    target.decline_reason = source.decline_reason,
    
    -- Additional core columns
    target.transaction_date_life_cycle = source.transaction_date_life_cycle,
    target.transaction_type_id = source.transaction_type_id,
    target.preference_reason = source.preference_reason,
    
    -- 3D Secure columns
    target."3d_flow_status" = source."3d_flow_status",
    target."3d_flow" = source."3d_flow",
    target.liability_shift = source.liability_shift,
    target.acs_res_authentication_status = source.acs_res_authentication_status,
    target.r_req_authentication_status = source.r_req_authentication_status,
    target.transaction_status_reason = source.transaction_status_reason,
    target.interaction_counter = source.interaction_counter,
    target.challenge_cancel = source.challenge_cancel,
    target.three_ds_method_indication = source.three_ds_method_indication,
    target.manage_3d_decision = source.manage_3d_decision,
    
    -- Financial columns
    target.rate_usd = source.rate_usd,
    target.currency_code = source.currency_code,
    target.three_ds_protocol_version = source.three_ds_protocol_version,
    
    -- Device and channel columns
    target.rebill = source.rebill,
    target.device_channel = source.device_channel,
    target.user_agent_3d = source.user_agent_3d,
    target.device_type = source.device_type,
    target.device_name = source.device_name,
    target.device_os = source.device_os,
    target.challenge_window_size = source.challenge_window_size,
    target.type_of_authentication_method = source.type_of_authentication_method,
    
    -- Client identification columns
    target.multi_client_id = source.multi_client_id,
    target.client_id = source.client_id,
    target.client_name = source.client_name,
    target.industry_code = source.industry_code,
    
    -- Card information columns
    target.credit_card_id = source.credit_card_id,
    target.cccid = source.cccid,
    target.bin = source.bin,
    target.card_type = source.card_type,
    target.consumer_id = source.consumer_id,
    target.issuer_bank_name = source.issuer_bank_name,
    target.device_channel_name = source.device_channel_name,
    target.bin_country = source.bin_country,
    
    -- Geographic and regulatory columns
    target.is_eea = source.is_eea,
    target.region = source.region,
    target.payment_instrument = source.payment_instrument,
    target.source_application = source.source_application,
    
    -- Partial amount processing columns
    target.is_partial_amount = source.is_partial_amount,
    target.enable_partial_approval = source.enable_partial_approval,
    target.partial_approval_is_void = source.partial_approval_is_void,
    target.partial_approval_void_id = source.partial_approval_void_id,
    target.partial_approval_void_time = source.partial_approval_void_time,
    target.partial_approval_requested_amount = source.partial_approval_requested_amount,
    target.partial_approval_requested_currency = source.partial_approval_requested_currency,
    target.partial_approval_processed_amount = source.partial_approval_processed_amount,
    target.partial_approval_processed_currency = source.partial_approval_processed_currency,
    target.partial_approval_processed_amount_in_usd = source.partial_approval_processed_amount_in_usd,
    
    -- Website and browser columns
    target.website_id = source.website_id,
    target.browser_user_agent = source.browser_user_agent,
    target.ip_country = source.ip_country,
    target.processor_id = source.processor_id,
    target.processor_name = source.processor_name,
    
    -- Risk and fraud columns
    target.risk_email_id = source.risk_email_id,
    target.is_currency_converted = source.is_currency_converted,
    target.email_seniority_start_date = source.email_seniority_start_date,
    target.email_payment_attempts = source.email_payment_attempts,
    target.final_fraud_decision_id = source.final_fraud_decision_id,
    
    -- Token and security columns
    target.external_token_eci = source.external_token_eci,
    target.risk_threed_eci = source.risk_threed_eci,
    target.threed_eci = source.threed_eci,
    target.cvv_code = source.cvv_code,
    target.provider_response_code = source.provider_response_code,
    target.issuer_card_program_id = source.issuer_card_program_id,
    
    -- Transaction flow columns
    target.scenario_id = source.scenario_id,
    target.previous_id = source.previous_id,
    target.next_id = source.next_id,
    target.step = source.step,
    target.reprocess_3d_reason = source.reprocess_3d_reason,
    target.data_only_authentication_result = source.data_only_authentication_result,
    target.is_cascaded_after_data_only_authentication = source.is_cascaded_after_data_only_authentication,
    target.next_action = source.next_action,
    target.authentication_method = source.authentication_method,
    target.cavv_verification_code = source.cavv_verification_code,
    target.channel = source.channel,
    
    -- Authentication request/response columns
    target.authentication_request = source.authentication_request,
    target.authentication_response = source.authentication_response,
    
    -- Card details columns
    target.cc_hash = source.cc_hash,
    target.exp_date = source.exp_date,
    target.message_version_3d = source.message_version_3d,
    target.cc_seniority_start_date = source.cc_seniority_start_date,
    target.mc_scheme_token_used = source.mc_scheme_token_used,
    target.stored_credentials_mode = source.stored_credentials_mode,
    target.avs_code = source.avs_code,
    target.credit_type_id = source.credit_type_id,
    target.subscription_step = source.subscription_step,
    
    -- Token fetching columns
    target.scheme_token_fetching_result = source.scheme_token_fetching_result,
    target.browser_screen_height = source.browser_screen_height,
    target.browser_screen_width = source.browser_screen_width,
    target.filter_reason_id = source.filter_reason_id,
    target.reason_code = source.reason_code,
    target.reason = source.reason,
    
    -- Service timestamp columns
    target.request_timestamp_service = source.request_timestamp_service,
    target.token_unique_reference_service = source.token_unique_reference_service,
    target.response_timestamp_service = source.response_timestamp_service,
    target.api_type_service = source.api_type_service,
    target.request_timestamp_fetching = source.request_timestamp_fetching,
    target.token_unique_reference_fetching = source.token_unique_reference_fetching,
    target.response_timestamp_fetching = source.response_timestamp_fetching,
    target.api_type_fetching = source.api_type_fetching,
    
    -- Token processing flags
    target.is_cryptogram_fetching_skipped = source.is_cryptogram_fetching_skipped,
    target.is_external_scheme_token = source.is_external_scheme_token,
    
    -- 3DS server and gateway columns
    target.three_ds_server_trans_id = source.three_ds_server_trans_id,
    target.gateway_id = source.gateway_id,
    target.cc_request_type_id = source.cc_request_type_id,
    target.upo_id = source.upo_id,
    
    -- Additional boolean flags
    target.IsCardReplaced = source.IsCardReplaced,
    target.IsVdcuFeeApplied = source.IsVdcuFeeApplied,
    target.AftType = source.AftType,
    target.secondarycccid = source.secondarycccid,
    
    -- Duration columns
    target.transaction_duration = source.transaction_duration,
    target.authorization_req_duration = source.authorization_req_duration,
    
    -- Installment columns
    target.FirstInstallment = source.FirstInstallment,
    target.PeriodicalInstallment = source.PeriodicalInstallment,
    target.numberOfInstallments = source.numberOfInstallments,
    target.InstallmentProgram = source.InstallmentProgram,
    target.InstallmentFundingType = source.InstallmentFundingType,
    target.first_installment_usd = source.first_installment_usd,
    target.periodical_installment_usd = source.periodical_installment_usd,
    
    -- Advanced processing columns
    target.ApplicableScenarios = source.ApplicableScenarios,
    target.cascading_ab_test_experimant_name = source.cascading_ab_test_experimant_name,
    
    -- Missing integer flags (Databricks specific)
    target.IsOnlineRefund = source.IsOnlineRefund,
    target.IsNoCVV = source.IsNoCVV,
    target.IsSupportedOCT = source.IsSupportedOCT,
    
    -- Missing transaction type and merchant columns
    target.ExternalTokenTrasactionType = source.ExternalTokenTrasactionType,
    target.SubscriptionType = source.SubscriptionType,
    target.MerchantCountryCodeNum = source.MerchantCountryCodeNum,
    target.MCMerchantAdviceCode = source.MCMerchantAdviceCode,
    target.AcquirerBinCountryId = source.AcquirerBinCountryId,
    target.AcquirerBin = source.AcquirerBin,
    
    -- Missing regulatory compliance flags
    target.IsPSD2 = source.IsPSD2,
    target.IsSCAScope = source.IsSCAScope,
    target.IsAirline = source.IsAirline,
    target.RequestedCCCID = source.RequestedCCCID,
    
    -- Missing geographic column
    target.merchant_country = source.merchant_country,
    
    -- Numeric fields
    target.amount_in_usd = source.amount_in_usd,
    target.approved_amount_in_usd = source.approved_amount_in_usd,
    target.original_currency_amount = source.original_currency_amount,
    
    -- Metadata (keep at the end)
    target.inserted_at = source.inserted_at

WHEN NOT MATCHED THEN INSERT (
    -- List all columns for INSERT
    transaction_main_id,
    transaction_date,
    is_void,
    is_sale_3d,
    is_external_mpi,
    is_prepaid,
    is_3d,
    init_status,
    auth_3d_status,
    sale_status,
    auth_status,
    settle_status,
    verify_auth_3d_status,
    is_sale_3d_auth_3d,
    manage_3d_decision_auth_3d,
    is_successful_challenge,
    is_successful_exemption,
    is_successful_frictionless,
    is_successful_authentication,
    is_approved,
    is_declined,
    transaction_type,
    multi_client_name,
    final_transaction_status,
    card_scheme,
    transaction_result_id,
    three_ds_flow_status,
    challenge_preference,
    authentication_flow,
    status,
    acs_url,
    transaction_id_life_cycle,
    decline_reason,
    transaction_date_life_cycle,
    transaction_type_id,
    preference_reason,
    "3d_flow_status",
    "3d_flow",
    liability_shift,
    acs_res_authentication_status,
    r_req_authentication_status,
    transaction_status_reason,
    interaction_counter,
    challenge_cancel,
    three_ds_method_indication,
    manage_3d_decision,
    rate_usd,
    currency_code,
    three_ds_protocol_version,
    rebill,
    device_channel,
    user_agent_3d,
    device_type,
    device_name,
    device_os,
    challenge_window_size,
    type_of_authentication_method,
    multi_client_id,
    client_id,
    client_name,
    industry_code,
    credit_card_id,
    cccid,
    bin,
    card_type,
    consumer_id,
    issuer_bank_name,
    device_channel_name,
    bin_country,
    is_eea,
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
    is_currency_converted,
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
    authentication_request,
    authentication_response,
    cc_hash,
    exp_date,
    message_version_3d,
    cc_seniority_start_date,
    mc_scheme_token_used,
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
    IsCardReplaced,
    IsVdcuFeeApplied,
    AftType,
    secondarycccid,
    transaction_duration,
    authorization_req_duration,
    FirstInstallment,
    PeriodicalInstallment,
    numberOfInstallments,
    InstallmentProgram,
    InstallmentFundingType,
    first_installment_usd,
    periodical_installment_usd,
    ApplicableScenarios,
    cascading_ab_test_experimant_name,
    IsOnlineRefund,
    IsNoCVV,
    IsSupportedOCT,
    ExternalTokenTrasactionType,
    SubscriptionType,
    MerchantCountryCodeNum,
    MCMerchantAdviceCode,
    AcquirerBinCountryId,
    AcquirerBin,
    IsPSD2,
    IsSCAScope,
    IsAirline,
    RequestedCCCID,
    merchant_country,
    amount_in_usd,
    approved_amount_in_usd,
    original_currency_amount,
    inserted_at
) VALUES (
    -- All source values in same order as columns above
    source.transaction_main_id,
    source.transaction_date,
    source.is_void,
    source.is_sale_3d,
    source.is_external_mpi,
    source.is_prepaid,
    source.is_3d,
    source.init_status,
    source.auth_3d_status,
    source.sale_status,
    source.auth_status,
    source.settle_status,
    source.verify_auth_3d_status,
    source.is_sale_3d_auth_3d,
    source.manage_3d_decision_auth_3d,
    source.is_successful_challenge,
    source.is_successful_exemption,
    source.is_successful_frictionless,
    source.is_successful_authentication,
    source.is_approved,
    source.is_declined,
    source.transaction_type,
    source.multi_client_name,
    source.final_transaction_status,
    source.card_scheme,
    source.transaction_result_id,
    source.three_ds_flow_status,
    source.challenge_preference,
    source.authentication_flow,
    source.status,
    source.acs_url,
    source.transaction_id_life_cycle,
    source.decline_reason,
    source.transaction_date_life_cycle,
    source.transaction_type_id,
    source.preference_reason,
    source."3d_flow_status",
    source."3d_flow",
    source.liability_shift,
    source.acs_res_authentication_status,
    source.r_req_authentication_status,
    source.transaction_status_reason,
    source.interaction_counter,
    source.challenge_cancel,
    source.three_ds_method_indication,
    source.manage_3d_decision,
    source.rate_usd,
    source.currency_code,
    source.three_ds_protocol_version,
    source.rebill,
    source.device_channel,
    source.user_agent_3d,
    source.device_type,
    source.device_name,
    source.device_os,
    source.challenge_window_size,
    source.type_of_authentication_method,
    source.multi_client_id,
    source.client_id,
    source.client_name,
    source.industry_code,
    source.credit_card_id,
    source.cccid,
    source.bin,
    source.card_type,
    source.consumer_id,
    source.issuer_bank_name,
    source.device_channel_name,
    source.bin_country,
    source.is_eea,
    source.region,
    source.payment_instrument,
    source.source_application,
    source.is_partial_amount,
    source.enable_partial_approval,
    source.partial_approval_is_void,
    source.partial_approval_void_id,
    source.partial_approval_void_time,
    source.partial_approval_requested_amount,
    source.partial_approval_requested_currency,
    source.partial_approval_processed_amount,
    source.partial_approval_processed_currency,
    source.partial_approval_processed_amount_in_usd,
    source.website_id,
    source.browser_user_agent,
    source.ip_country,
    source.processor_id,
    source.processor_name,
    source.risk_email_id,
    source.is_currency_converted,
    source.email_seniority_start_date,
    source.email_payment_attempts,
    source.final_fraud_decision_id,
    source.external_token_eci,
    source.risk_threed_eci,
    source.threed_eci,
    source.cvv_code,
    source.provider_response_code,
    source.issuer_card_program_id,
    source.scenario_id,
    source.previous_id,
    source.next_id,
    source.step,
    source.reprocess_3d_reason,
    source.data_only_authentication_result,
    source.is_cascaded_after_data_only_authentication,
    source.next_action,
    source.authentication_method,
    source.cavv_verification_code,
    source.channel,
    source.authentication_request,
    source.authentication_response,
    source.cc_hash,
    source.exp_date,
    source.message_version_3d,
    source.cc_seniority_start_date,
    source.mc_scheme_token_used,
    source.stored_credentials_mode,
    source.avs_code,
    source.credit_type_id,
    source.subscription_step,
    source.scheme_token_fetching_result,
    source.browser_screen_height,
    source.browser_screen_width,
    source.filter_reason_id,
    source.reason_code,
    source.reason,
    source.request_timestamp_service,
    source.token_unique_reference_service,
    source.response_timestamp_service,
    source.api_type_service,
    source.request_timestamp_fetching,
    source.token_unique_reference_fetching,
    source.response_timestamp_fetching,
    source.api_type_fetching,
    source.is_cryptogram_fetching_skipped,
    source.is_external_scheme_token,
    source.three_ds_server_trans_id,
    source.gateway_id,
    source.cc_request_type_id,
    source.upo_id,
    source.IsCardReplaced,
    source.IsVdcuFeeApplied,
    source.AftType,
    source.secondarycccid,
    source.transaction_duration,
    source.authorization_req_duration,
    source.FirstInstallment,
    source.PeriodicalInstallment,
    source.numberOfInstallments,
    source.InstallmentProgram,
    source.InstallmentFundingType,
    source.first_installment_usd,
    source.periodical_installment_usd,
    source.ApplicableScenarios,
    source.cascading_ab_test_experimant_name,
    source.IsOnlineRefund,
    source.IsNoCVV,
    source.IsSupportedOCT,
    source.ExternalTokenTrasactionType,
    source.SubscriptionType,
    source.MerchantCountryCodeNum,
    source.MCMerchantAdviceCode,
    source.AcquirerBinCountryId,
    source.AcquirerBin,
    source.IsPSD2,
    source.IsSCAScope,
    source.IsAirline,
    source.RequestedCCCID,
    source.merchant_country,
    source.amount_in_usd,
    source.approved_amount_in_usd,
    source.original_currency_amount,
    source.inserted_at
);

-- ==============================================================================
-- 5. CHECKPOINT MANAGEMENT - UPDATE METADATA AFTER SUCCESSFUL MERGE
-- ==============================================================================

-- Get count of processed records
SET RECORDS_PROCESSED = (SELECT COUNT(*) FROM IDENTIFIER($STAGING_TABLE));

-- Update checkpoint with successful processing timestamp
UPDATE IDENTIFIER($CHECKPOINT_TABLE) 
SET 
    last_processed_timestamp = CURRENT_TIMESTAMP(),
    last_updated_timestamp = CURRENT_TIMESTAMP(),
    status = 'SUCCESS',
    records_processed = $RECORDS_PROCESSED
WHERE etl_name = $ETL_NAME;

-- ==============================================================================
-- 6. CLEANUP - DROP STAGING TABLE
-- ==============================================================================

DROP TABLE IF EXISTS IDENTIFIER($STAGING_TABLE);

-- ==============================================================================
-- 7. SUCCESS MESSAGE
-- ==============================================================================

SELECT 
    'INCREMENTAL ETL COMPLETED SUCCESSFULLY' AS status,
    $RECORDS_PROCESSED AS records_processed,
    $PROCESSING_DATE AS processing_date,
    CURRENT_TIMESTAMP() AS completed_at;

    
select count(*) from poc.public.ncp_silver_v2
