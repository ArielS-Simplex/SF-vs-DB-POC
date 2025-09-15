-- ==============================================================================
-- PHASE 2: INCREMENTAL PROCESSING ETL
-- Based on: 02_checkpoint_management_fixed.sql + Databricks incremental logic
-- Added: Only process records newer than last checkpoint (like Databricks)
-- Critical: Reduces processing from ALL data to only NEW data since last run
-- ==============================================================================

-- ==============================================================================
-- 1. CREATE METADATA TABLE FOR CHECKPOINT MANAGEMENT
-- ==============================================================================

CREATE TABLE IF NOT EXISTS POC.PUBLIC.etl_metadata (
    table_name VARCHAR(100) PRIMARY KEY,
    checkpoint_time TIMESTAMP,
    last_run_timestamp TIMESTAMP,
    last_run_status VARCHAR(50),
    records_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ==============================================================================
-- 2. INITIALIZE CHECKPOINT (If not exists)
-- ==============================================================================

MERGE INTO POC.PUBLIC.etl_metadata AS target
USING (
    SELECT 'NCP_SILVER_V4' AS table_name,
           '1900-01-01 00:00:00'::TIMESTAMP AS checkpoint_time,
           CURRENT_TIMESTAMP() AS last_run_timestamp,
           'INITIALIZING' AS last_run_status,
           0 AS records_processed
) AS source
ON target.table_name = source.table_name
WHEN NOT MATCHED THEN
    INSERT (table_name, checkpoint_time, last_run_timestamp, last_run_status, records_processed)
    VALUES (source.table_name, source.checkpoint_time, source.last_run_timestamp, source.last_run_status, source.records_processed);

-- ==============================================================================
-- 3. GET CURRENT CHECKPOINT & SET VARIABLES
-- ==============================================================================

SET checkpoint_time = (
    SELECT checkpoint_time 
    FROM POC.PUBLIC.etl_metadata 
    WHERE table_name = 'NCP_SILVER_V4'
);

-- Variables for this run
SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';  -- Source is V2
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V4';  -- Target is V4
SET run_timestamp = CURRENT_TIMESTAMP();

-- ==============================================================================
-- 4. UPDATE CHECKPOINT STATUS - STARTING
-- ==============================================================================

UPDATE POC.PUBLIC.etl_metadata 
SET last_run_timestamp = $run_timestamp,
    last_run_status = 'RUNNING',
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V4';

-- ==============================================================================
-- 5. INCREMENTAL PROCESSING LOGIC (CRITICAL DATABRICKS FEATURE)
-- ==============================================================================

-- Check how many new records since checkpoint
SET new_records_count = (
    SELECT COUNT(*) 
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $checkpoint_time
      AND DATE(transaction_date) BETWEEN $DATE_RANGE_START AND $DATE_RANGE_END
);

SELECT 'INCREMENTAL PROCESSING CHECK' AS status,
       $checkpoint_time AS current_checkpoint,
       $new_records_count AS new_records_to_process,
       CASE WHEN $new_records_count > 0 THEN 'PROCESSING NEW DATA' ELSE 'NO NEW DATA' END AS action;

-- ==============================================================================
-- 6. MAIN ETL LOGIC - INCREMENTAL PROCESSING
-- ==============================================================================

-- Create or replace target table (for Phase 2 we still recreate, Phase 3 will add MERGE)
DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
WITH 
-- Step 1: Filter to only NEW records (CRITICAL DATABRICKS EQUIVALENT)
bronze_filtered AS (
    SELECT *
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $checkpoint_time  -- ⭐ INCREMENTAL FILTER - Only process NEW data
      AND DATE(transaction_date) BETWEEN $DATE_RANGE_START AND $DATE_RANGE_END
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 'test_multi', 'testmulti', 'testclient', 'test client',
        'test', 'qa', 'staging', 'demo', 'sandbox', 'dev', 'development'
      )
),

-- Step 2: Status flag calculations (same as before)
status_flags_calculated AS (
    SELECT *,
        -- Transaction result status flags for each transaction type
        CASE 
            WHEN UPPER(transaction_type) = 'INIT' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'INIT' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS init_status,
        
        CASE 
            WHEN UPPER(transaction_type) = 'AUTH3D' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'AUTH3D' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS auth_3d_status,
        
        CASE 
            WHEN UPPER(transaction_type) = 'SALE' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'SALE' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS sale_status,
        
        CASE 
            WHEN UPPER(transaction_type) = 'AUTH' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'AUTH' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS auth_status,
        
        CASE 
            WHEN UPPER(transaction_type) = 'SETTLE' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'SETTLE' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS settle_status,
        
        CASE 
            WHEN UPPER(transaction_type) = 'VERIFY_AUTH_3D' AND UPPER(status) = 'APPROVED' THEN 'approved'
            WHEN UPPER(transaction_type) = 'VERIFY_AUTH_3D' AND UPPER(status) = 'DECLINED' THEN 'declined'
            ELSE NULL
        END AS verify_auth_3d_status
        
    FROM bronze_filtered
)

SELECT 
    -- Core transaction fields
    transaction_id,
    transaction_main_id,
    transaction_date,
    UPPER(TRIM(transaction_type)) AS transaction_type,
    UPPER(TRIM(status)) AS status,
    
    -- Status flags (derived columns)
    init_status,
    auth_3d_status,
    sale_status,
    auth_status,
    settle_status,
    verify_auth_3d_status,
    
    -- Conditional copies (only for auth3d transactions)
    CASE WHEN UPPER(transaction_type) = 'AUTH3D' THEN is_sale_3d END AS is_sale_3d_auth_3d,
    CASE WHEN UPPER(transaction_type) = 'AUTH3D' THEN manage_3d_decision END AS manage_3d_decision_auth_3d,
    
    -- 3D Secure success analysis
    CASE 
        WHEN UPPER(TRIM(COALESCE(three_d_flow_status, ''))) = 'CHALLENGE' 
             AND UPPER(TRIM(COALESCE(status, ''))) = 'APPROVED' THEN TRUE
        ELSE FALSE
    END AS is_successful_challenge,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(three_d_flow_status, ''))) = 'EXEMPTION' 
             AND UPPER(TRIM(COALESCE(status, ''))) = 'APPROVED' THEN TRUE
        ELSE FALSE
    END AS is_successful_exemption,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(three_d_flow_status, ''))) = 'FRICTIONLESS' 
             AND UPPER(TRIM(COALESCE(status, ''))) = 'APPROVED' THEN TRUE
        ELSE FALSE
    END AS is_successful_frictionless,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(challenge_preference, ''))) = 'Y_REQUESTED_BY_ACQUIRER' 
             AND UPPER(TRIM(COALESCE(status, ''))) = 'APPROVED' THEN TRUE
        ELSE FALSE
    END AS is_successful_authentication,
    
    -- High-level approval/decline logic using status flags as references
    CASE 
        WHEN init_status = 'approved' OR auth_3d_status = 'approved' OR 
             sale_status = 'approved' OR auth_status = 'approved' OR 
             settle_status = 'approved' OR verify_auth_3d_status = 'approved' THEN TRUE
        ELSE FALSE
    END AS is_approved,
    
    CASE 
        WHEN init_status = 'declined' OR auth_3d_status = 'declined' OR 
             sale_status = 'declined' OR auth_status = 'declined' OR 
             settle_status = 'declined' OR verify_auth_3d_status = 'declined' THEN TRUE
        ELSE FALSE
    END AS is_declined,
    
    -- Boolean conversions with exact Databricks mapping
    CASE WHEN LOWER(TRIM(COALESCE(liability_shift, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS liability_shift,
    CASE WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_sale_3d,
    CASE WHEN LOWER(TRIM(COALESCE(is_dcc, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_dcc,
    CASE WHEN LOWER(TRIM(COALESCE(is_exemption, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_exemption,
    CASE WHEN LOWER(TRIM(COALESCE(is_recurring, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_recurring,
    CASE WHEN LOWER(TRIM(COALESCE(is_subsequent, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_subsequent,
    CASE WHEN LOWER(TRIM(COALESCE(is_mit, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_mit,
    CASE WHEN LOWER(TRIM(COALESCE(enable_partial_approval, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS enable_partial_approval,
    CASE WHEN LOWER(TRIM(COALESCE(THREED_SECURE_V_2_ISSUER_ENROLLED, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS threed_secure_v_2_issuer_enrolled,
    CASE WHEN LOWER(TRIM(COALESCE(create_payment_method, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS create_payment_method,
    CASE WHEN LOWER(TRIM(COALESCE(is_tokenization, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_tokenization,
    CASE WHEN LOWER(TRIM(COALESCE(is_3d_secure, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_3d_secure,
    CASE WHEN LOWER(TRIM(COALESCE(is_soft_decline, ''))) IN ('true', '1', 'yes', '1.0') THEN TRUE ELSE FALSE END AS is_soft_decline,
    
    -- All other columns (string fields kept as-is)
    acquirer_name, acquiring_name, action, auth_code, avs_response, browser_accept_header,
    browser_java_enabled, browser_javascript_enabled, browser_language,
    browser_screen_color_depth, browser_screen_height, browser_screen_width,
    browser_time_zone, browser_user_agent, card_bin, card_scheme, card_type,
    cc_request_type_id, challenge_preference, client_name, credit_card_id,
    cvv_response, gateway_id, manage_3d_decision, multi_client_name,
    notification_url, original_currency, payment_method_type, processor_name,
    redirect_url, response_message, risk_email_id, risk_score,
    scheme_token_fetching_result, service_name, source_ip,
    three_d_flow_status, three_d_secure_reason_description,
    three_d_secure_reason_id, three_ds_server_trans_id, upo_id,
    website_id, CCCID, ACQUIRERBIN, BIN, CARD_SCHEME, ACQUIRERBINCOUNTRYID,
    CARD_TYPE, CONSUMER_ID, CVV_RESPONSE, EXTERNALTOKENTRASACTIONTYPE,
    EXTERNALTOKEN_ECI, GATEWAY_ID, ISAIRLINE, ISDCC, ISMIT, ISNOCVV,
    ISONLINEREFUND, ISPSD2, ISRECURRING, ISSCASCOPE, ISSUBSEQUENT,
    ISSUPPORTEDOCT, ISSUER_BANK_NAME, MCMERCHANTADVICECODE, MERCHANT_COUNTRY,
    MERCHANT_ID, MERCHANTCOUNTRYCODENUM, NOTIFICATION_URL, ORIGINAL_CURRENCY,
    PAYMENT_METHOD_TYPE, PROCESSOR_NAME, REDIRECT_URL, REQUESTEDCCCID,
    RESPONSE_MESSAGE, RISK_EMAIL_ID, RISK_SCORE, SCHEME_TOKEN_FETCHING_RESULT,
    SERVICE_NAME, SOURCE_IP, SUBSCRIPTIONTYPE, THREE_D_FLOW_STATUS,
    THREE_D_SECURE_REASON_DESCRIPTION, THREE_D_SECURE_REASON_ID,
    THREE_DS_SERVER_TRANS_ID, UPO_ID, WEBSITE_ID,
    BROWSER_ACCEPT_HEADER, BROWSER_JAVA_ENABLED, BROWSER_JAVASCRIPT_ENABLED,
    BROWSER_LANGUAGE, BROWSER_SCREEN_COLOR_DEPTH, BROWSER_SCREEN_HEIGHT,
    BROWSER_SCREEN_WIDTH, BROWSER_TIME_ZONE, BROWSER_USER_AGENT,
    
    -- Numeric fields with proper casting
    COALESCE(TRY_CAST(amount_in_usd AS DECIMAL(18,2)), 0) AS amount_in_usd,
    COALESCE(TRY_CAST(approved_amount_in_usd AS DECIMAL(18,2)), 0) AS approved_amount_in_usd,
    COALESCE(TRY_CAST(original_currency_amount AS DECIMAL(18,2)), 0) AS original_currency_amount,
    
    -- ETL Processing metadata
    $run_timestamp AS etl_processed_at,
    
    -- Metadata (keep at the end)
    inserted_at
    
FROM status_flags_calculated
ORDER BY transaction_date, transaction_main_id;

-- ==============================================================================
-- 7. UPDATE CHECKPOINT STATUS - SUCCESS WITH NEW CHECKPOINT TIME
-- ==============================================================================

SET records_processed = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));

-- Get the latest inserted_at timestamp for next checkpoint
SET new_checkpoint_time = (
    SELECT COALESCE(MAX(inserted_at), $checkpoint_time)
    FROM IDENTIFIER($TARGET_TABLE)
);

UPDATE POC.PUBLIC.etl_metadata 
SET checkpoint_time = $new_checkpoint_time,  -- ⭐ CRITICAL: Advance checkpoint for next run
    last_run_status = 'SUCCESS',
    records_processed = $records_processed,
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V4';

-- ==============================================================================
-- 8. INCREMENTAL PROCESSING VERIFICATION
-- ==============================================================================

SELECT 'INCREMENTAL PROCESSING COMPLETE' AS status,
       table_name,
       checkpoint_time AS new_checkpoint,
       last_run_status,
       records_processed,
       'Phase 2: Incremental Processing Complete' AS phase
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';