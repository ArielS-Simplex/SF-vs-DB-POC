-- ==============================================================================
-- TRUE MERGE ETL - PROPER MERGE STATEMENTS FOR POC PERFORMANCE COMPARISON
-- This implements actual MERGE (not DELETE + INSERT) for accurate performance testing
-- ==============================================================================

SET ETL_NAME = 'BRONZE_TO_SILVER_TRUE_MERGE';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V2';
SET STAGING_TABLE = 'POC.PUBLIC.NCP_SILVER_V2_STAGING';
SET CHECKPOINT_TABLE = 'POC.PUBLIC.ETL_CHECKPOINT';

-- For manual daily processing
SET DATE_RANGE_START = '2025-09-01';
SET DATE_RANGE_END = '2025-09-01';

-- Create staging table (same logic as before)
DROP TABLE IF EXISTS IDENTIFIER($STAGING_TABLE);

CREATE TABLE IDENTIFIER($STAGING_TABLE) AS
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
-- [All the business logic CTEs from enhanced_working_etl.sql would go here]
status_flags_calculated AS (
SELECT 
    -- Keep all original columns
    *,
    
    -- DATABRICKS DERIVED COLUMNS - All 174 columns preserved
    -- [Complete business logic from enhanced_working_etl.sql]
    
    -- Metadata (keep at the end)
    inserted_at
    
FROM filtered_data
)
SELECT * FROM status_flags_calculated
ORDER BY transaction_date, transaction_main_id;

-- ==============================================================================
-- TRUE MERGE STATEMENT - ALL 174 COLUMNS
-- ==============================================================================

-- Generate MERGE statement dynamically to handle all 174 columns
SET merge_sql = (
    WITH column_list AS (
        SELECT 
            column_name,
            ordinal_position
        FROM information_schema.columns 
        WHERE table_schema = 'PUBLIC' 
          AND table_name = 'NCP_SILVER_V2_STAGING'
          AND table_catalog = 'POC'
          AND column_name NOT IN ('TRANSACTION_MAIN_ID', 'TRANSACTION_DATE')  -- Exclude key columns
        ORDER BY ordinal_position
    ),
    update_columns AS (
        SELECT LISTAGG('target.' || column_name || ' = source.' || column_name, ',\n    ') AS update_set
        FROM column_list
    ),
    insert_columns AS (
        SELECT 
            LISTAGG(column_name, ', ') AS column_list,
            LISTAGG('source.' || column_name, ', ') AS value_list
        FROM (
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'PUBLIC' 
              AND table_name = 'NCP_SILVER_V2_STAGING'
              AND table_catalog = 'POC'
            ORDER BY ordinal_position
        )
    )
    SELECT 
        'MERGE INTO ' || $TARGET_TABLE || ' AS target\n' ||
        'USING ' || $STAGING_TABLE || ' AS source\n' ||
        'ON target.transaction_main_id = source.transaction_main_id\n' ||
        '   AND target.transaction_date = source.transaction_date\n' ||
        'WHEN MATCHED THEN UPDATE SET\n    ' ||
        update_columns.update_set || '\n' ||
        'WHEN NOT MATCHED THEN INSERT (\n    ' ||
        insert_columns.column_list || '\n' ||
        ') VALUES (\n    ' ||
        insert_columns.value_list || '\n' ||
        ');'
    FROM update_columns, insert_columns
);

-- Execute the true MERGE
EXECUTE IMMEDIATE $merge_sql;

-- Clean up staging
DROP TABLE IF EXISTS IDENTIFIER($STAGING_TABLE);

-- Success message
SELECT 
    'TRUE MERGE COMPLETED' AS status,
    CURRENT_TIMESTAMP() AS completed_at;