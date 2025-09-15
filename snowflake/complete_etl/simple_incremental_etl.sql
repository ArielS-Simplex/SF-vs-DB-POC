-- ==============================================================================
-- SIMPLE INCREMENTAL ETL - Based on Enhanced Working ETL + MERGE Operations
-- Preserves 100% working business logic + adds simple incremental processing
-- ==============================================================================

-- ==============================================================================
-- 1. SIMPLE CHECKPOINT MANAGEMENT
-- ==============================================================================

-- Create metadata table (one-time setup)
CREATE TABLE IF NOT EXISTS POC.PUBLIC.etl_checkpoint (
    table_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Initialize checkpoint if not exists
INSERT INTO POC.PUBLIC.etl_checkpoint (table_name, last_processed_timestamp)
SELECT 'NCP_SILVER_V2', '2025-09-01 00:00:00'::TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM POC.PUBLIC.etl_checkpoint WHERE table_name = 'NCP_SILVER_V2');

-- ==============================================================================
-- 2. GET LAST CHECKPOINT
-- ==============================================================================

SET last_checkpoint = (
    SELECT COALESCE(last_processed_timestamp, '2025-09-01 00:00:00'::TIMESTAMP)
    FROM POC.PUBLIC.etl_checkpoint 
    WHERE table_name = 'NCP_SILVER_V2'
);

-- Variables for this run
SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V2';
SET run_timestamp = CURRENT_TIMESTAMP();

-- ==============================================================================
-- 3. CHECK FOR NEW DATA
-- ==============================================================================

SET new_records_count = (
    SELECT COUNT(*) 
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $last_checkpoint
      AND DATE(transaction_date) BETWEEN $DATE_RANGE_START AND $DATE_RANGE_END
);

SELECT 
    'INCREMENTAL CHECK' AS status,
    $last_checkpoint AS last_checkpoint,
    $new_records_count AS new_records_to_process;

-- ==============================================================================
-- 4. CREATE TARGET TABLE IF NOT EXISTS (First Run)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER($TARGET_TABLE) (
    transaction_main_id VARCHAR(255),
    transaction_date DATE,
    -- Add all your 174 columns here with proper types
    -- This is just the key columns for demonstration
    inserted_at TIMESTAMP,
    etl_processed_at TIMESTAMP
);

-- ==============================================================================
-- 5. PROCESS NEW DATA USING MERGE (Your Proven Business Logic)
-- ==============================================================================

MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING (
    -- ⭐ YOUR WORKING ENHANCED ETL LOGIC (EXACT SAME TRANSFORMATIONS)
    WITH deduped_bronze AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
                ORDER BY inserted_at DESC
            ) AS rn
        FROM IDENTIFIER($SOURCE_TABLE)
        WHERE inserted_at > $last_checkpoint  -- ⭐ ONLY NEW DATA
          AND DATE(transaction_date) >= $DATE_RANGE_START
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
        
        -- ⭐ ALL YOUR WORKING DERIVED COLUMNS (Copy from enhanced_working_etl.sql)
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
        
        -- ... (copy all other derived columns from your enhanced ETL)
        
        CASE 
            WHEN LOWER(TRIM(COALESCE(transaction_type, ''))) = 'auth3d' THEN CASE 
                WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
                WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
                ELSE NULL
            END
            ELSE NULL
        END AS is_sale_3d_auth_3d

    FROM filtered_data
    )
    
    SELECT 
        -- ⭐ ALL YOUR WORKING COLUMN SELECTIONS (Copy from enhanced_working_etl.sql)
        transaction_main_id,
        transaction_date,
        
        -- Boolean normalization (your proven logic)
        CASE 
            WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN FALSE
            ELSE NULL
        END AS is_void,
        
        -- ... (copy all other 174 columns from your enhanced ETL)
        
        -- ETL metadata
        $run_timestamp AS etl_processed_at,
        inserted_at
        
    FROM status_flags_calculated
    
) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date

WHEN MATCHED THEN 
    UPDATE SET 
        -- Update all columns with latest values
        etl_processed_at = source.etl_processed_at,
        inserted_at = source.inserted_at
        -- ... (update all other columns)

WHEN NOT MATCHED THEN 
    INSERT (
        transaction_main_id,
        transaction_date,
        -- ... (all your 174 columns)
        etl_processed_at,
        inserted_at
    )
    VALUES (
        source.transaction_main_id,
        source.transaction_date,
        -- ... (all your 174 columns)
        source.etl_processed_at,
        source.inserted_at
    );

-- ==============================================================================
-- 6. UPDATE CHECKPOINT
-- ==============================================================================

SET max_inserted_at = (
    SELECT COALESCE(MAX(inserted_at), $last_checkpoint)
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE inserted_at > $last_checkpoint
      AND DATE(transaction_date) BETWEEN $DATE_RANGE_START AND $DATE_RANGE_END
);

UPDATE POC.PUBLIC.etl_checkpoint 
SET last_processed_timestamp = $max_inserted_at,
    updated_at = CURRENT_TIMESTAMP()
WHERE table_name = 'NCP_SILVER_V2';

-- ==============================================================================
-- 7. VERIFICATION
-- ==============================================================================

SELECT 
    'ETL COMPLETE' AS status,
    (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE)) AS total_records,
    $new_records_count AS new_records_processed,
    $max_inserted_at AS new_checkpoint;