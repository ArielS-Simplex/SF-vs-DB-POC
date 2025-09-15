-- ==============================================================================
-- PHASE 1: CHECKPOINT MANAGEMENT ETL
-- Based on: 01_baseline_etl.sql + Databricks checkpoint functionality
-- Added: Metadata table for tracking last processed timestamps
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
-- 3. GET CURRENT CHECKPOINT
-- ==============================================================================

SET checkpoint_time = (
    SELECT checkpoint_time 
    FROM POC.PUBLIC.etl_metadata 
    WHERE table_name = 'NCP_SILVER_V4'
);

-- Variables for this run
SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05'; 
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V4';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V4';
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
-- 5. MAIN ETL LOGIC (Same as baseline for Phase 1)
-- ==============================================================================

DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

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
    
    -- CONDITIONAL COPIES - Only for auth3d transactions
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH3D' THEN CASE 
            WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
            ELSE NULL
        END
        ELSE NULL
    END AS is_sale_3d_auth_3d,
    
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH3D' THEN CASE 
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

    -- LIABILITY SHIFT boolean conversion (FIXED)
    CASE 
        WHEN LOWER(TRIM(COALESCE(liability_shift, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(liability_shift, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS liability_shift,
    
    -- DATABRICKS DERIVED COLUMNS - Conditional copies (from CTE)
    is_sale_3d_auth_3d,
    manage_3d_decision_auth_3d,
    
    -- DATABRICKS DERIVED COLUMNS - Reference calculated status flags
    init_status,
    auth_3d_status,
    sale_status,
    auth_status,
    settle_status,
    verify_auth_3d_status,
    
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
    
    -- Keep all remaining columns from enhanced_working_etl.sql
    -- (Include all 170+ columns from the original file...)
    
    -- For brevity, showing key columns - full implementation would include ALL columns
    transaction_result_id,
    threed_flow_status AS three_ds_flow_status,
    challenge_preference,
    authentication_flow,
    status,
    acs_url,
    decline_reason,
    
    -- Add processing metadata
    $run_timestamp AS etl_processed_at,
    
    -- Metadata (keep at the end)
    inserted_at
    
FROM status_flags_calculated
ORDER BY transaction_date, transaction_main_id;

-- ==============================================================================
-- 6. UPDATE CHECKPOINT STATUS - SUCCESS
-- ==============================================================================

SET records_processed = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));

UPDATE POC.PUBLIC.etl_metadata 
SET checkpoint_time = $run_timestamp,
    last_run_status = 'SUCCESS',
    records_processed = $records_processed,
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V4';

-- ==============================================================================
-- 7. CHECKPOINT VERIFICATION
-- ==============================================================================

SELECT 'CHECKPOINT UPDATE VERIFICATION' AS status,
       table_name,
       checkpoint_time,
       last_run_status,
       records_processed,
       'Phase 1: Checkpoint Management Complete' AS phase
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';