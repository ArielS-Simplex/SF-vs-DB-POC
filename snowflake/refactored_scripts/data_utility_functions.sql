-- ================================================
-- Snowflake Utility Functions - Metadata Management
-- Equivalent to: data_utility_modules.ipynb (SchemaManager class)
-- ================================================

-- ================================================
-- METADATA TABLE MANAGEMENT
-- ================================================

-- Create metadata table (equivalent to SchemaManager._create_metadata_table_if_not_exists)
CREATE TABLE IF NOT EXISTS NUVEI_DWH.NCP.METADATA_TABLE (
    table_name STRING,
    schema_json STRING,
    checkpoint TIMESTAMP_NTZ,
    source_table STRING,
    table_keys STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ================================================
-- STORED PROCEDURES FOR METADATA MANAGEMENT
-- ================================================

-- Update metadata procedure (equivalent to SchemaManager.update_metadata)
CREATE OR REPLACE PROCEDURE UPDATE_METADATA(
    table_name_param STRING,
    field_name_param STRING,
    field_value_param VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    valid_fields ARRAY := ['schema_json', 'checkpoint', 'source_table', 'table_keys'];
    field_exists BOOLEAN := FALSE;
    i INTEGER := 0;
    sql_stmt STRING;
BEGIN
    -- Validate field name
    FOR i IN 0 TO ARRAY_SIZE(valid_fields) - 1 DO
        IF valid_fields[i] = field_name_param THEN
            field_exists := TRUE;
            BREAK;
        END IF;
    END FOR;
    
    IF NOT field_exists THEN
        RETURN 'ERROR: Invalid metadata field: ' || field_name_param || '. Must be one of schema_json, checkpoint, source_table, table_keys.';
    END IF;
    
    -- Build dynamic SQL for merge
    sql_stmt := 'MERGE INTO NUVEI_DWH.NCP.METADATA_TABLE AS target ' ||
                'USING (SELECT ''' || table_name_param || ''' AS table_name, ' ||
                'PARSE_JSON(''' || TO_JSON(field_value_param) || ''') AS ' || field_name_param || ', ' ||
                'CURRENT_TIMESTAMP() AS updated_at) AS source ' ||
                'ON target.table_name = source.table_name ' ||
                'WHEN MATCHED THEN UPDATE SET ' ||
                'target.' || field_name_param || ' = source.' || field_name_param || ', ' ||
                'target.updated_at = source.updated_at ' ||
                'WHEN NOT MATCHED THEN INSERT (table_name, ' || field_name_param || ', updated_at) ' ||
                'VALUES (source.table_name, source.' || field_name_param || ', source.updated_at)';
                
    EXECUTE IMMEDIATE sql_stmt;
    
    RETURN 'SUCCESS: Updated ' || field_name_param || ' for table ' || table_name_param;
END;
$$;

-- Get metadata value procedure (equivalent to SchemaManager.get_metadata)
CREATE OR REPLACE FUNCTION GET_METADATA(
    table_name_param STRING,
    field_name_param STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT 
        CASE 
            WHEN field_name_param = 'checkpoint' THEN TO_VARIANT(checkpoint)
            WHEN field_name_param = 'source_table' THEN TO_VARIANT(source_table)
            WHEN field_name_param = 'table_keys' THEN TO_VARIANT(table_keys)
            WHEN field_name_param = 'schema_json' THEN PARSE_JSON(schema_json)
            ELSE TO_VARIANT('Invalid field name')
        END
    FROM NUVEI_DWH.NCP.METADATA_TABLE 
    WHERE table_name = table_name_param
$$;

-- Add schema procedure (equivalent to SchemaManager.add_schema)
CREATE OR REPLACE PROCEDURE ADD_SCHEMA(
    table_name_param STRING,
    schema_json_param STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL UPDATE_METADATA(table_name_param, 'schema_json', PARSE_JSON(schema_json_param));
    RETURN 'SUCCESS: Added schema for table ' || table_name_param;
END;
$$;

-- List all schemas (equivalent to SchemaManager.list_schemas)
CREATE OR REPLACE FUNCTION LIST_SCHEMAS()
RETURNS TABLE(table_name STRING)
LANGUAGE SQL
AS
$$
    SELECT table_name
    FROM NUVEI_DWH.NCP.METADATA_TABLE
    WHERE schema_json IS NOT NULL
    ORDER BY table_name
$$;

-- ================================================
-- SCHEMA VALIDATION FUNCTIONS
-- ================================================

-- Validate table schema matches expected schema
CREATE OR REPLACE FUNCTION VALIDATE_TABLE_SCHEMA(
    table_name_param STRING,
    schema_name_param STRING DEFAULT 'NCP',
    database_name_param STRING DEFAULT 'NUVEI_DWH'
)
RETURNS TABLE(
    column_name STRING,
    expected_type STRING,
    actual_type STRING,
    is_match BOOLEAN
)
LANGUAGE SQL
AS
$$
    WITH expected_schema AS (
        SELECT 
            key AS column_name,
            value::STRING AS expected_type
        FROM TABLE(FLATTEN(GET_METADATA(table_name_param, 'schema_json')))
    ),
    actual_schema AS (
        SELECT 
            COLUMN_NAME AS column_name,
            DATA_TYPE AS actual_type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = database_name_param
          AND TABLE_SCHEMA = schema_name_param
          AND TABLE_NAME = UPPER(table_name_param)
    )
    SELECT 
        COALESCE(e.column_name, a.column_name) AS column_name,
        e.expected_type,
        a.actual_type,
        (e.expected_type = a.actual_type) AS is_match
    FROM expected_schema e
    FULL OUTER JOIN actual_schema a ON e.column_name = a.column_name
    ORDER BY column_name
$$;

-- ================================================
-- DATA TYPE CONVERSION UTILITIES
-- ================================================

-- Convert boolean-like strings to proper booleans
CREATE OR REPLACE FUNCTION NORMALIZE_BOOLEAN(input_value STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE 
        WHEN LOWER(TRIM(input_value)) IN ('true', '1', 'yes', '1.0') THEN TRUE
        WHEN LOWER(TRIM(input_value)) IN ('false', '0', 'no', '0.0') THEN FALSE
        ELSE NULL
    END
$$;

-- Clean string values (equivalent to string cleaning in fixing_dtypes)
CREATE OR REPLACE FUNCTION CLEAN_STRING(input_value STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN LOWER(TRIM(input_value)) IN ('<na>', 'na', 'nan', 'none', '', ' ', '\\x00', 'deprecated') THEN NULL
        WHEN REGEXP_LIKE(input_value, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(input_value, '(\\d+)', 1, 1)
        ELSE LOWER(TRIM(input_value))
    END
$$;

-- ================================================
-- TABLE OPERATIONS
-- ================================================

-- Check if table exists
CREATE OR REPLACE FUNCTION TABLE_EXISTS(
    table_name_param STRING,
    schema_name_param STRING DEFAULT 'NCP',
    database_name_param STRING DEFAULT 'NUVEI_DWH'
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT COUNT(*) > 0
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = database_name_param
      AND TABLE_SCHEMA = schema_name_param
      AND TABLE_NAME = UPPER(table_name_param)
$$;

-- Get table row count
CREATE OR REPLACE FUNCTION GET_TABLE_ROW_COUNT(
    table_name_param STRING,
    schema_name_param STRING DEFAULT 'NCP',
    database_name_param STRING DEFAULT 'NUVEI_DWH'
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    CALL RESULT_SCAN(LAST_QUERY_ID()) -- This would be implemented with dynamic SQL in practice
$$;

-- ================================================
-- CHECKPOINT MANAGEMENT
-- ================================================

-- Get last checkpoint for a table
CREATE OR REPLACE FUNCTION GET_LAST_CHECKPOINT(table_name_param STRING)
RETURNS TIMESTAMP_NTZ
LANGUAGE SQL
AS
$$
    SELECT checkpoint
    FROM NUVEI_DWH.NCP.METADATA_TABLE
    WHERE table_name = table_name_param
$$;

-- Update checkpoint for a table
CREATE OR REPLACE PROCEDURE UPDATE_CHECKPOINT(
    table_name_param STRING,
    checkpoint_value TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL UPDATE_METADATA(table_name_param, 'checkpoint', TO_VARIANT(checkpoint_value));
    RETURN 'SUCCESS: Updated checkpoint for ' || table_name_param || ' to ' || checkpoint_value;
END;
$$;

-- ================================================
-- SCHEMA EVOLUTION HELPERS
-- ================================================

-- Compare source and target schemas to identify new columns
CREATE OR REPLACE FUNCTION IDENTIFY_NEW_COLUMNS(
    source_table STRING,
    target_table STRING
)
RETURNS TABLE(
    column_name STRING,
    data_type STRING,
    is_new BOOLEAN
)
LANGUAGE SQL
AS
$$
    WITH source_cols AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = UPPER(SPLIT_PART(source_table, '.', -1))
          AND TABLE_SCHEMA = UPPER(SPLIT_PART(source_table, '.', -2))
          AND TABLE_CATALOG = UPPER(SPLIT_PART(source_table, '.', -3))
    ),
    target_cols AS (
        SELECT COLUMN_NAME, DATA_TYPE  
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = UPPER(SPLIT_PART(target_table, '.', -1))
          AND TABLE_SCHEMA = UPPER(SPLIT_PART(target_table, '.', -2))
          AND TABLE_CATALOG = UPPER(SPLIT_PART(target_table, '.', -3))
    )
    SELECT 
        s.COLUMN_NAME,
        s.DATA_TYPE,
        (t.COLUMN_NAME IS NULL) AS is_new
    FROM source_cols s
    LEFT JOIN target_cols t ON s.COLUMN_NAME = t.COLUMN_NAME
$$;

-- ================================================
-- TESTING AND VALIDATION FUNCTIONS
-- ================================================

-- Test metadata table functionality
CREATE OR REPLACE PROCEDURE TEST_METADATA_FUNCTIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    test_table STRING := 'test_transactions_silver';
    test_schema STRING := '{"transaction_id": "string", "amount": "decimal(18,2)", "created_at": "timestamp"}';
    result STRING := '';
BEGIN
    -- Test add schema
    CALL ADD_SCHEMA(test_table, test_schema);
    result := result || 'Schema added. ';
    
    -- Test update metadata
    CALL UPDATE_METADATA(test_table, 'source_table', 'test_transactions_bronze');
    result := result || 'Source table updated. ';
    
    CALL UPDATE_METADATA(test_table, 'table_keys', 'transaction_id,created_at');
    result := result || 'Table keys updated. ';
    
    -- Test get metadata
    LET checkpoint_val := GET_METADATA(test_table, 'checkpoint');
    LET source_val := GET_METADATA(test_table, 'source_table');
    
    result := result || 'Source table: ' || source_val || '. ';
    
    -- Cleanup test data
    DELETE FROM NUVEI_DWH.NCP.METADATA_TABLE WHERE table_name = test_table;
    result := result || 'Test data cleaned up.';
    
    RETURN 'TEST PASSED: ' || result;
END;
$$;

-- ================================================
-- USAGE EXAMPLES
-- ================================================

-- Example 1: Set up metadata for a new table
/*
CALL ADD_SCHEMA(
    'NUVEI_DWH.NCP.TRANSACTIONS_SILVER',
    '{"transaction_main_id": "string", "transaction_date": "timestamp", "amount_in_usd": "decimal(18,2)"}'
);

CALL UPDATE_METADATA('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', 'source_table', 'NUVEI_DWH.BRONZE.TRANSACTIONS');
CALL UPDATE_METADATA('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', 'table_keys', 'transaction_main_id,transaction_id_life_cycle');
*/

-- Example 2: Get metadata values
/*
SELECT GET_METADATA('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', 'checkpoint') AS last_checkpoint;
SELECT GET_METADATA('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', 'source_table') AS source_table;
SELECT GET_METADATA('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', 'table_keys') AS table_keys;
*/

-- Example 3: Update checkpoint after successful ETL
/*
CALL UPDATE_CHECKPOINT('NUVEI_DWH.NCP.TRANSACTIONS_SILVER', CURRENT_TIMESTAMP());
*/

-- ================================================
-- End of Utility Functions
-- ================================================
