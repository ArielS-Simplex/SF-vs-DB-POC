-- ==============================================================================
-- MANUAL CLEANUP BEFORE RE-RUNNING ETL (OPTIONAL)
-- The ETL already drops/recreates the table, but this gives you explicit control
-- ==============================================================================

-- Option 1: Drop the table completely (recommended)
DROP TABLE IF EXISTS POC.PUBLIC.transactions_silver;

-- Option 2: Just truncate if you want to keep the structure
-- TRUNCATE TABLE POC.PUBLIC.transactions_silver;

-- Verify the table is gone/empty
SELECT COUNT(*) AS remaining_rows 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'TRANSACTIONS_SILVER';

-- This should return 0 if table is dropped, or you can check row count if truncated
-- SELECT COUNT(*) FROM POC.PUBLIC.transactions_silver; -- Only if truncated
