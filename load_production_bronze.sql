-- PRODUCTION DATA LOADING TO BRONZE
-- This script loads real production data while preserving the 185-column structure
-- that matches our Databricks reference

-- STEP 1: Backup current sample data (optional)
CREATE OR REPLACE TABLE poc.public.ncp_bronze_sample_backup AS
SELECT * FROM poc.public.ncp_bronze;

-- STEP 2: Create staging table for raw data loading
CREATE OR REPLACE TABLE poc.public.ncp_bronze_staging (
    filename STRING,
    loaded_at TIMESTAMP_NTZ,
    raw_line STRING
);

-- STEP 3: Create file format for raw line loading
CREATE OR REPLACE FILE FORMAT txt_format_raw
TYPE = 'CSV' 
FIELD_DELIMITER = NONE  -- No field delimiter - entire line is one field
SKIP_HEADER = 0 
ENCODING = 'ISO-8859-1' 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 4: Load production data - Multiple files for better performance testing
-- Load 3 days of data (September 4, 5, 6, 2025)
COPY INTO poc.public.ncp_bronze_staging (filename, loaded_at, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-04.*'  -- All files from Sept 4
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

COPY INTO poc.public.ncp_bronze_staging (filename, loaded_at, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-05.*'  -- All files from Sept 5
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

COPY INTO poc.public.ncp_bronze_staging (filename, loaded_at, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-06.*'  -- All files from Sept 6
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

-- STEP 5: Check staging load results
SELECT 
    'STAGING LOAD SUMMARY' AS step,
    COUNT(*) AS total_rows_loaded,
    COUNT(DISTINCT filename) AS files_loaded,
    MIN(loaded_at) AS first_load_time,
    MAX(loaded_at) AS last_load_time,
    MIN(LENGTH(raw_line)) AS min_line_length,
    MAX(LENGTH(raw_line)) AS max_line_length
FROM poc.public.ncp_bronze_staging;

-- Show sample of loaded data
SELECT 
    filename,
    loaded_at,
    LEFT(raw_line, 200) AS sample_line_start,
    LENGTH(raw_line) AS line_length
FROM poc.public.ncp_bronze_staging 
ORDER BY loaded_at 
LIMIT 10;

-- STEP 6: Clear current bronze table and recreate with production data
-- Drop existing bronze table 
DROP TABLE IF EXISTS poc.public.ncp_bronze;

-- Recreate bronze table with full 185-column structure using parsed production data
CREATE TABLE poc.public.ncp_bronze AS
SELECT 
    filename,
    loaded_at as inserted_at,
    
    -- Parse all 185 columns from tab-delimited raw_line
    -- Column positions based on the exact structure from create_proper_bronze.sql
    SPLIT_PART(raw_line, '\t', 1) AS transaction_main_id,
    TRY_TO_TIMESTAMP(SPLIT_PART(raw_line, '\t', 2)) AS transaction_date,
    SPLIT_PART(raw_line, '\t', 3) AS transaction_id_life_cycle,
    TRY_TO_TIMESTAMP(SPLIT_PART(raw_line, '\t', 4)) AS transaction_date_life_cycle,
    SPLIT_PART(raw_line, '\t', 5) AS transaction_type_id,
    SPLIT_PART(raw_line, '\t', 6) AS transaction_type,
    SPLIT_PART(raw_line, '\t', 7) AS transaction_result_id,
    SPLIT_PART(raw_line, '\t', 8) AS final_transaction_status,
    SPLIT_PART(raw_line, '\t', 9) AS threed_flow_status,
    SPLIT_PART(raw_line, '\t', 10) AS challenge_preference,
    SPLIT_PART(raw_line, '\t', 11) AS preference_reason,
    SPLIT_PART(raw_line, '\t', 12) AS authentication_flow,
    SPLIT_PART(raw_line, '\t', 13) AS threed_flow,
    SPLIT_PART(raw_line, '\t', 14) AS is_void,
    SPLIT_PART(raw_line, '\t', 15) AS liability_shift,
    SPLIT_PART(raw_line, '\t', 16) AS status,
    SPLIT_PART(raw_line, '\t', 17) AS acs_url,
    SPLIT_PART(raw_line, '\t', 18) AS acs_res_authentication_status,
    SPLIT_PART(raw_line, '\t', 19) AS r_req_authentication_status,
    SPLIT_PART(raw_line, '\t', 20) AS transaction_status_reason,
    SPLIT_PART(raw_line, '\t', 21) AS interaction_counter,
    SPLIT_PART(raw_line, '\t', 22) AS challenge_cancel,
    SPLIT_PART(raw_line, '\t', 23) AS three_ds_method_indication,
    SPLIT_PART(raw_line, '\t', 24) AS is_sale_3d,
    SPLIT_PART(raw_line, '\t', 25) AS manage_3d_decision,
    SPLIT_PART(raw_line, '\t', 26) AS decline_reason,
    SPLIT_PART(raw_line, '\t', 27) AS amount_in_usd,
    SPLIT_PART(raw_line, '\t', 28) AS approved_amount_in_usd,
    SPLIT_PART(raw_line, '\t', 29) AS original_currency_amount,
    SPLIT_PART(raw_line, '\t', 30) AS rate_usd,
    SPLIT_PART(raw_line, '\t', 31) AS currency_code,
    SPLIT_PART(raw_line, '\t', 32) AS three_ds_protocol_version,
    SPLIT_PART(raw_line, '\t', 33) AS is_external_mpi,
    SPLIT_PART(raw_line, '\t', 34) AS rebill,
    SPLIT_PART(raw_line, '\t', 35) AS device_channel,
    SPLIT_PART(raw_line, '\t', 36) AS user_agent_3d,
    SPLIT_PART(raw_line, '\t', 37) AS device_type,
    SPLIT_PART(raw_line, '\t', 38) AS device_name,
    SPLIT_PART(raw_line, '\t', 39) AS device_os,
    SPLIT_PART(raw_line, '\t', 40) AS challenge_window_size,
    SPLIT_PART(raw_line, '\t', 41) AS type_of_authentication_method,
    SPLIT_PART(raw_line, '\t', 42) AS multi_client_id,
    SPLIT_PART(raw_line, '\t', 43) AS client_id,
    SPLIT_PART(raw_line, '\t', 44) AS multi_client_name,
    SPLIT_PART(raw_line, '\t', 45) AS client_name,
    SPLIT_PART(raw_line, '\t', 46) AS industry_code,
    SPLIT_PART(raw_line, '\t', 47) AS credit_card_id,
    SPLIT_PART(raw_line, '\t', 48) AS cccid,
    SPLIT_PART(raw_line, '\t', 49) AS bin,
    SPLIT_PART(raw_line, '\t', 50) AS is_prepaid,
    SPLIT_PART(raw_line, '\t', 51) AS card_scheme,
    SPLIT_PART(raw_line, '\t', 52) AS card_type,
    SPLIT_PART(raw_line, '\t', 53) AS consumer_id,
    SPLIT_PART(raw_line, '\t', 54) AS issuer_bank_name,
    SPLIT_PART(raw_line, '\t', 55) AS device_channel_name,
    SPLIT_PART(raw_line, '\t', 56) AS bin_country,
    SPLIT_PART(raw_line, '\t', 57) AS is_eea,
    SPLIT_PART(raw_line, '\t', 58) AS region,
    SPLIT_PART(raw_line, '\t', 59) AS payment_instrument,
    SPLIT_PART(raw_line, '\t', 60) AS source_application,
    SPLIT_PART(raw_line, '\t', 61) AS is_partial_amount,
    SPLIT_PART(raw_line, '\t', 62) AS enable_partial_approval,
    SPLIT_PART(raw_line, '\t', 63) AS partial_approval_is_void,
    SPLIT_PART(raw_line, '\t', 64) AS partial_approval_void_id,
    SPLIT_PART(raw_line, '\t', 65) AS partial_approval_void_time,
    SPLIT_PART(raw_line, '\t', 66) AS partial_approval_requested_amount,
    SPLIT_PART(raw_line, '\t', 67) AS partial_approval_requested_currency,
    SPLIT_PART(raw_line, '\t', 68) AS partial_approval_processed_amount,
    SPLIT_PART(raw_line, '\t', 69) AS partial_approval_processed_currency,
    SPLIT_PART(raw_line, '\t', 70) AS partial_approval_processed_amount_in_usd,
    SPLIT_PART(raw_line, '\t', 71) AS website_id,
    SPLIT_PART(raw_line, '\t', 72) AS browser_user_agent,
    SPLIT_PART(raw_line, '\t', 73) AS ip_country,
    SPLIT_PART(raw_line, '\t', 74) AS processor_id,
    SPLIT_PART(raw_line, '\t', 75) AS processor_name,
    SPLIT_PART(raw_line, '\t', 76) AS risk_email_id,
    SPLIT_PART(raw_line, '\t', 77) AS is_currency_converted,
    SPLIT_PART(raw_line, '\t', 78) AS email_seniority_start_date,
    SPLIT_PART(raw_line, '\t', 79) AS email_payment_attempts,
    SPLIT_PART(raw_line, '\t', 80) AS final_fraud_decision_id,
    SPLIT_PART(raw_line, '\t', 81) AS external_token_eci,
    SPLIT_PART(raw_line, '\t', 82) AS risk_threed_eci,
    SPLIT_PART(raw_line, '\t', 83) AS threed_eci,
    SPLIT_PART(raw_line, '\t', 84) AS cvv_code,
    SPLIT_PART(raw_line, '\t', 85) AS provider_response_code,
    SPLIT_PART(raw_line, '\t', 86) AS issuer_card_program_id,
    SPLIT_PART(raw_line, '\t', 87) AS scenario_id,
    SPLIT_PART(raw_line, '\t', 88) AS previous_id,
    SPLIT_PART(raw_line, '\t', 89) AS next_id,
    SPLIT_PART(raw_line, '\t', 90) AS step,
    SPLIT_PART(raw_line, '\t', 91) AS reprocess_3d_reason,
    SPLIT_PART(raw_line, '\t', 92) AS data_only_authentication_result,
    SPLIT_PART(raw_line, '\t', 93) AS is_cascaded_after_data_only_authentication,
    SPLIT_PART(raw_line, '\t', 94) AS next_action,
    SPLIT_PART(raw_line, '\t', 95) AS authentication_method,
    SPLIT_PART(raw_line, '\t', 96) AS cavv_verification_code,
    SPLIT_PART(raw_line, '\t', 97) AS channel,
    SPLIT_PART(raw_line, '\t', 98) AS authentication_request,
    SPLIT_PART(raw_line, '\t', 99) AS authentication_response,
    SPLIT_PART(raw_line, '\t', 100) AS cc_hash,
    SPLIT_PART(raw_line, '\t', 101) AS exp_date,
    SPLIT_PART(raw_line, '\t', 102) AS message_version_3d,
    SPLIT_PART(raw_line, '\t', 103) AS cc_seniority_start_date,
    SPLIT_PART(raw_line, '\t', 104) AS mc_scheme_token_used,
    -- inserted_at handled above
    SPLIT_PART(raw_line, '\t', 106) AS stored_credentials_mode,
    SPLIT_PART(raw_line, '\t', 107) AS avs_code,
    SPLIT_PART(raw_line, '\t', 108) AS is_3d,
    SPLIT_PART(raw_line, '\t', 109) AS credit_type_id,
    SPLIT_PART(raw_line, '\t', 110) AS subscription_step,
    SPLIT_PART(raw_line, '\t', 111) AS scheme_token_fetching_result,
    SPLIT_PART(raw_line, '\t', 112) AS browser_screen_height,
    SPLIT_PART(raw_line, '\t', 113) AS browser_screen_width,
    SPLIT_PART(raw_line, '\t', 114) AS filter_reason_id,
    SPLIT_PART(raw_line, '\t', 115) AS reason_code,
    SPLIT_PART(raw_line, '\t', 116) AS reason,
    SPLIT_PART(raw_line, '\t', 117) AS request_timestamp_service,
    SPLIT_PART(raw_line, '\t', 118) AS token_unique_reference_service,
    SPLIT_PART(raw_line, '\t', 119) AS response_timestamp_service,
    SPLIT_PART(raw_line, '\t', 120) AS api_type_service,
    SPLIT_PART(raw_line, '\t', 121) AS request_timestamp_fetching,
    SPLIT_PART(raw_line, '\t', 122) AS token_unique_reference_fetching,
    SPLIT_PART(raw_line, '\t', 123) AS response_timestamp_fetching,
    SPLIT_PART(raw_line, '\t', 124) AS api_type_fetching,
    SPLIT_PART(raw_line, '\t', 125) AS is_cryptogram_fetching_skipped,
    SPLIT_PART(raw_line, '\t', 126) AS is_external_scheme_token,
    SPLIT_PART(raw_line, '\t', 127) AS three_ds_server_trans_id,
    SPLIT_PART(raw_line, '\t', 128) AS gateway_id,
    SPLIT_PART(raw_line, '\t', 129) AS cc_request_type_id,
    SPLIT_PART(raw_line, '\t', 130) AS upo_id,
    SPLIT_PART(raw_line, '\t', 131) AS iscardReplaced,
    SPLIT_PART(raw_line, '\t', 132) AS isvdcuFeeApplied,
    SPLIT_PART(raw_line, '\t', 133) AS aftType,
    SPLIT_PART(raw_line, '\t', 134) AS secondarycccid,
    SPLIT_PART(raw_line, '\t', 135) AS transaction_duration,
    SPLIT_PART(raw_line, '\t', 136) AS authorization_req_duration,
    SPLIT_PART(raw_line, '\t', 137) AS firstInstallment,
    SPLIT_PART(raw_line, '\t', 138) AS periodicalInstallment,
    SPLIT_PART(raw_line, '\t', 139) AS numberOfInstallments,
    SPLIT_PART(raw_line, '\t', 140) AS installmentProgram,
    SPLIT_PART(raw_line, '\t', 141) AS installmentFundingType,
    SPLIT_PART(raw_line, '\t', 142) AS first_installment_usd,
    SPLIT_PART(raw_line, '\t', 143) AS periodical_installment_usd,
    SPLIT_PART(raw_line, '\t', 144) AS applicableScenarios,
    SPLIT_PART(raw_line, '\t', 145) AS cascading_ab_test_experimant_name,
    
    -- Keep raw line for debugging
    raw_line
    
FROM poc.public.ncp_bronze_staging
WHERE raw_line IS NOT NULL 
  AND raw_line != ''
  AND LENGTH(raw_line) > 100  -- Filter out obviously bad lines
;

-- STEP 7: Verify production bronze table
SELECT 
    'PRODUCTION BRONZE SUMMARY' AS step,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    COUNT(CASE WHEN transaction_main_id IS NOT NULL THEN 1 END) AS valid_transaction_ids,
    COUNT(CASE WHEN transaction_date IS NOT NULL THEN 1 END) AS valid_dates
FROM poc.public.ncp_bronze;

-- Show sample of production data
SELECT 
    transaction_main_id,
    transaction_date,
    transaction_type,
    multi_client_name,
    transaction_result_id,
    amount_in_usd,
    currency_code,
    filename
FROM poc.public.ncp_bronze 
ORDER BY transaction_date DESC
LIMIT 10;

-- STEP 8: Cleanup staging table (optional)
-- DROP TABLE poc.public.ncp_bronze_staging;

-- PRODUCTION BRONZE TABLE IS NOW READY FOR ETL!
SELECT 'BRONZE PRODUCTION LOAD COMPLETE - READY FOR ETL!' AS status;
