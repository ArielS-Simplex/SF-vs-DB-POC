-- ==============================================================================
-- 3RD STAGE: TABLE CREATION AND DATA LOADING
-- Create target table and load processed data
-- ==============================================================================

-- REQUIRED VARIABLES (if not already set in previous stages)
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET curr_timestamp = CURRENT_TIMESTAMP();

-- Count total rows for checkpoint logic
SET total_rows = (SELECT COUNT(*) FROM processed_data);

-- SMART TABLE CREATION: Drop and recreate target table to match current schema
-- This ensures the target table always has the correct schema matching our SELECT
DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
SELECT * FROM processed_data WHERE 1=0;

-- UPSERT LOGIC: Implement Databricks-style MERGE operation
-- Check if we should use INSERT or MERGE based on existing data
SET existing_rows = (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE));

-- Execute the appropriate operation based on table state
-- For new tables (no existing rows), use simple INSERT - ALWAYS INSERT FOR TESTING
INSERT INTO IDENTIFIER($TARGET_TABLE)
SELECT * FROM processed_data;
-- WHERE $existing_rows = 0;  -- Remove this condition for testing

-- For existing tables with data, we would execute MERGE in production
-- MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
-- USING processed_data AS source
-- ON target.transaction_main_id = source.transaction_main_id 
--    AND target.transaction_date = source.transaction_date
-- WHEN MATCHED THEN UPDATE SET *
-- WHEN NOT MATCHED THEN INSERT *;

-- Note: For POC purposes, we're using INSERT for new tables only
-- In production, implement proper MERGE logic for incremental updates

-- Update checkpoint only if rows were processed (matches Databricks logic)
MERGE INTO POC.PUBLIC.metadata_table AS target
USING (SELECT $TARGET_TABLE AS table_name, $total_rows AS row_count) AS source
ON target.table_name = source.table_name
WHEN MATCHED AND source.row_count > 0 THEN 
    UPDATE SET checkpoint = $curr_timestamp;

-- OPTIMIZATION: Table maintenance (mimics Databricks OPTIMIZE command)
-- In Databricks this happens automatically, in Snowflake we can do clustering
-- ALTER TABLE IDENTIFIER($TARGET_TABLE) CLUSTER BY (transaction_date, transaction_main_id);

SELECT 'STAGE 3 COMPLETED: Target table created and data loaded' AS status;
SELECT $total_rows AS rows_processed;
