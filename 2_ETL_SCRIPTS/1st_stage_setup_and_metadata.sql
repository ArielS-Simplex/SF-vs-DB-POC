-- ==============================================================================
-- 1ST STAGE: SETUP AND METADATA
-- Environment setup, variables, and metadata table creation
-- ==============================================================================

-- ENVIRONMENT DETECTION: Mimic Databricks cloud provider detection
SET CLOUD_PROVIDER = 'Snowflake'; -- In Databricks this would be Azure/AWS/GCP detection

-- DATABRICKS CONSTANTS: Replicate exact test client list and boolean mappings
SET TEST_CLIENTS = 'test multi,davidh test2 multi,ice demo multi,monitoring client pod2 multi';

-- FORCE NULL COLUMNS: Replicate Databricks column forcing logic
-- These columns are force-nulled in Databricks for data consistency
SET FORCE_NULL_COLUMNS = 'user_agent_3d,authentication_request,authentication_response,authorization_req_duration';

-- Parameters (Snowflake session variables)
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET curr_timestamp = CURRENT_TIMESTAMP();

-- Create metadata table
CREATE TABLE IF NOT EXISTS POC.PUBLIC.metadata_table (
    table_name STRING,
    schema_json STRING,
    checkpoint TIMESTAMP_TZ,
    source_table STRING,
    table_keys STRING
);

-- Initialize metadata
MERGE INTO POC.PUBLIC.metadata_table AS target
USING (SELECT $TARGET_TABLE AS table_name) AS source
ON target.table_name = source.table_name
WHEN NOT MATCHED THEN 
    INSERT (table_name, source_table, table_keys) 
    VALUES ($TARGET_TABLE, $SOURCE_TABLE, 'TRANSACTION_MAIN_ID,TRANSACTION_DATE');

-- Get checkpoint
SET checkpoint_time = (SELECT checkpoint FROM POC.PUBLIC.metadata_table WHERE table_name = $TARGET_TABLE);

-- DYNAMIC SCHEMA EVOLUTION: Check if target table exists and compare schemas
SET target_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'TRANSACTIONS_SILVER');

-- If target table exists, check for new columns in source and add them dynamically
-- This mimics Databricks' mergeSchema functionality
CREATE OR REPLACE TEMPORARY TABLE schema_comparison AS (
    WITH source_columns AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'NCP_BRONZE'
    ),
    target_columns AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'TRANSACTIONS_SILVER'
        AND $target_exists > 0
    ),
    new_columns AS (
        SELECT s.COLUMN_NAME, s.DATA_TYPE
        FROM source_columns s
        LEFT JOIN target_columns t ON s.COLUMN_NAME = t.COLUMN_NAME
        WHERE t.COLUMN_NAME IS NULL AND $target_exists > 0
    )
    SELECT * FROM new_columns
);

-- Add new columns to target table if they exist (Dynamic Schema Evolution)
CREATE OR REPLACE TEMPORARY TABLE add_columns_sql AS (
    SELECT 'ALTER TABLE POC.PUBLIC.transactions_silver ADD COLUMN ' || 
           COLUMN_NAME || ' ' || DATA_TYPE AS ddl_statement
    FROM schema_comparison
    WHERE $target_exists > 0
);

-- Execute dynamic column addition (simulating Databricks auto-schema evolution)
-- Note: In production, you would execute these DDL statements dynamically

SELECT 'STAGE 1 COMPLETED: Setup and metadata tables created' AS status;
