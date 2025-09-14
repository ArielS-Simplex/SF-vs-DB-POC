-- BRONZE TABLE CREATION FROM STAGING DATA
-- This script processes staging data into bronze table with proper column types
-- RUN AFTER: 00_staging_data_loader.sql

-- STEP 2: Clear current bronze table and recreate with production data
-- Drop existing bronze table V2
DROP TABLE IF EXISTS poc.public.ncp_bronze_v2;

-- STEP 3: Recreate bronze table V2 with full 185-column structure using parsed production data - OPTIMIZED VERSION
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
    
    -- Parse all 185 columns using array indexing (0-based) - MUCH FASTER!
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

-- STEP 4: Verify production bronze table V2
SELECT 
    'PRODUCTION BRONZE V2 SUMMARY' AS step,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    COUNT(CASE WHEN transaction_main_id IS NOT NULL THEN 1 END) AS valid_transaction_ids,
    COUNT(CASE WHEN transaction_date IS NOT NULL THEN 1 END) AS valid_dates
FROM poc.public.ncp_bronze_v2;

-- Show sample of production data V2
SELECT 
    transaction_main_id,
    transaction_date,
    transaction_type,
    multi_client_name,
    transaction_result_id,
    amount_in_usd,
    currency_code,
    filename
FROM poc.public.ncp_bronze_v2 
ORDER BY transaction_date DESC
LIMIT 10;

-- STEP 5: Cleanup staging table (optional)
-- DROP TABLE poc.public.ncp_bronze_staging_v2;

-- PRODUCTION BRONZE V2 TABLE IS NOW READY FOR ETL!
SELECT 'BRONZE V2 PRODUCTION LOAD COMPLETE - READY FOR ETL!' AS status;
