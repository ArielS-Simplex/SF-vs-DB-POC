-- ==============================================================================
-- UNIFIED INCREMENTAL ETL - Handles both first run and subsequent runs
-- True Databricks pattern: CREATE TABLE IF NOT EXISTS + MERGE for new data
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
           '2025-09-09 00:00:00'::TIMESTAMP AS checkpoint_time,
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
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V4';
SET run_timestamp = CURRENT_TIMESTAMP();

-- ==============================================================================
-- 4. CHECK TABLE STATUS & NEW RECORDS
-- ==============================================================================

SET table_exists = (
    SELECT COUNT(*) > 0
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE table_name = 'NCP_SILVER_V4'
      AND table_schema = 'PUBLIC'
      AND table_catalog = 'POC'
);

SET new_records_count = (
    SELECT COUNT(*) 
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $checkpoint_time
      AND DATE(transaction_date) BETWEEN $DATE_RANGE_START AND $DATE_RANGE_END
);

-- ==============================================================================
-- 5. PROCESSING DECISION
-- ==============================================================================

SELECT 'PROCESSING DECISION' AS status,
       $table_exists AS table_exists,
       $checkpoint_time AS current_checkpoint,
       $new_records_count AS new_records_to_process,
       CASE 
         WHEN $table_exists = FALSE THEN 'FIRST RUN - CREATE TABLE'
         WHEN $new_records_count = 0 THEN 'NO NEW DATA - SKIP PROCESSING'
         ELSE 'INCREMENTAL DATA - MERGE REQUIRED'
       END AS action_required;

-- ==============================================================================
-- 6. UPDATE CHECKPOINT STATUS - STARTING
-- ==============================================================================

UPDATE POC.PUBLIC.etl_metadata 
SET last_run_timestamp = $run_timestamp,
    last_run_status = 'RUNNING',
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V4';

-- ==============================================================================
-- 7. FIRST RUN: CREATE TABLE (Only if table doesn't exist)
-- ==============================================================================

-- This will only execute if table doesn't exist
CREATE TABLE IF NOT EXISTS IDENTIFIER($TARGET_TABLE) (
    -- Minimal schema - will be populated by MERGE
    transaction_main_id VARCHAR(255),
    transaction_date TIMESTAMP,
    inserted_at TIMESTAMP,
    etl_processed_at TIMESTAMP,
    PRIMARY KEY (transaction_main_id, transaction_date)
);

-- ==============================================================================
-- 8. MERGE OPERATION (For both first run and incremental runs)
-- ==============================================================================

-- This runs whether table is new or existing
MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING (
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $checkpoint_time  -- Incremental filter
      AND DATE(transaction_date) >= $DATE_RANGE_START
      AND DATE(transaction_date) <= $DATE_RANGE_END
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
)
SELECT 
    transaction_main_id,
    transaction_date,
    inserted_at,
    $run_timestamp AS etl_processed_at,
    -- Add key derived columns for demonstration
    CASE 
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH' 
             AND TRIM(COALESCE(transaction_result_id, '')) = '1006' THEN TRUE
        WHEN UPPER(TRIM(COALESCE(transaction_type, ''))) = 'AUTH' THEN FALSE
        ELSE NULL
    END AS auth_status
FROM filtered_data
) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN
    UPDATE SET
        etl_processed_at = source.etl_processed_at,
        inserted_at = source.inserted_at,
        auth_status = source.auth_status
WHEN NOT MATCHED THEN
    INSERT (transaction_main_id, transaction_date, inserted_at, etl_processed_at, auth_status)
    VALUES (source.transaction_main_id, source.transaction_date, source.inserted_at, source.etl_processed_at, source.auth_status);

-- ==============================================================================
-- 9. UPDATE CHECKPOINT STATUS - SUCCESS
-- ==============================================================================

SET records_processed = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));

-- Get the latest inserted_at timestamp for next checkpoint
SET new_checkpoint_time = (
    SELECT COALESCE(MAX(inserted_at), $checkpoint_time)
    FROM IDENTIFIER($TARGET_TABLE)
);

UPDATE POC.PUBLIC.etl_metadata 
SET checkpoint_time = $new_checkpoint_time,
    last_run_status = 'SUCCESS',
    records_processed = $records_processed,
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V4';

-- ==============================================================================
-- 10. FINAL VERIFICATION
-- ==============================================================================

SELECT 'UNIFIED INCREMENTAL ETL COMPLETE' AS status,
       table_name,
       checkpoint_time AS new_checkpoint,
       last_run_status,
       records_processed,
       'True Databricks Incremental Pattern' AS processing_type
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';