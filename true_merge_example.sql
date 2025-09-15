-- ==============================================================================
-- TRUE MERGE IMPLEMENTATION - CHALLENGE WITH 174 COLUMNS
-- ==============================================================================

-- OPTION 1: MANUAL MERGE (174 columns - very error prone!)
MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING IDENTIFIER($STAGING_TABLE) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN UPDATE SET
    target.transaction_main_id = source.transaction_main_id,
    target.transaction_date = source.transaction_date,
    target.transaction_type = source.transaction_type,
    target.transaction_status = source.transaction_status,
    target.init_status = source.init_status,
    target.auth_3d_status = source.auth_3d_status,
    target.sale_status = source.sale_status,
    target.auth_status = source.auth_status,
    target.settle_status = source.settle_status,
    target.verify_auth_3d_status = source.verify_auth_3d_status,
    -- ... need to list ALL 174 columns here! 
    -- RISK: Easy to miss columns, typos, maintenance nightmare
    target.inserted_at = source.inserted_at
WHEN NOT MATCHED THEN INSERT (
    transaction_main_id, transaction_date, transaction_type, /* ... all 174 columns ... */
) VALUES (
    source.transaction_main_id, source.transaction_date, source.transaction_type, /* ... all 174 values ... */
);

-- ==============================================================================
-- OPTION 2: DYNAMIC MERGE USING SNOWFLAKE METADATA (RECOMMENDED!)
-- ==============================================================================

-- Step 1: Generate the MERGE statement dynamically
SET merge_statement = (
    SELECT 
        'MERGE INTO ' || $TARGET_TABLE || ' AS target ' ||
        'USING ' || $STAGING_TABLE || ' AS source ' ||
        'ON target.transaction_main_id = source.transaction_main_id AND target.transaction_date = source.transaction_date ' ||
        'WHEN MATCHED THEN UPDATE SET ' ||
        LISTAGG('target.' || column_name || ' = source.' || column_name, ', ') ||
        ' WHEN NOT MATCHED THEN INSERT (' ||
        LISTAGG(column_name, ', ') ||
        ') VALUES (' ||
        LISTAGG('source.' || column_name, ', ') || ');'
    FROM information_schema.columns 
    WHERE table_schema = 'PUBLIC' 
      AND table_name = 'NCP_SILVER_V2'
      AND table_catalog = 'POC'
    ORDER BY ordinal_position
);

-- Step 2: Execute the dynamic MERGE
EXECUTE IMMEDIATE $merge_statement;

-- ==============================================================================
-- OPTION 3: HYBRID APPROACH - MERGE with SELECT *
-- ==============================================================================

-- This is a compromise - uses MERGE logic but with simpler syntax
MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING IDENTIFIER($STAGING_TABLE) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN DELETE  -- Delete the matched record
WHEN NOT MATCHED THEN INSERT SELECT * FROM source;  -- Insert from staging

-- Then insert the updated records
INSERT INTO IDENTIFIER($TARGET_TABLE)
SELECT * FROM IDENTIFIER($STAGING_TABLE) s
WHERE EXISTS (
    SELECT 1 FROM IDENTIFIER($TARGET_TABLE) t 
    WHERE t.transaction_main_id = s.transaction_main_id 
    AND t.transaction_date = s.transaction_date
);