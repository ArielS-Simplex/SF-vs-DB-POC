-- Simple debug script to check what's happening

-- 1. Check source table row count
SELECT 'Source table total rows' AS check_type, COUNT(*) AS count FROM POC.PUBLIC.NCP_BRONZE;

-- 2. Check date range in source
SELECT 
    'Date range in source' AS check_type,
    MIN(TRANSACTION_DATE) AS min_date,
    MAX(TRANSACTION_DATE) AS max_date,
    COUNT(*) AS total_rows
FROM POC.PUBLIC.NCP_BRONZE;

-- 3. Check last 3 days specifically
SELECT 
    'Last 3 days count' AS check_type,
    COUNT(*) AS count
FROM POC.PUBLIC.NCP_BRONZE
WHERE TRANSACTION_DATE >= CURRENT_DATE() - INTERVAL '3 days';

-- 4. Check if transactions_silver exists and its count
SELECT 'Target table count' AS check_type, COUNT(*) AS count FROM POC.PUBLIC.transactions_silver;

-- 5. Simple insert test (just 10 rows)
CREATE OR REPLACE TABLE POC.PUBLIC.test_insert AS
SELECT TOP 10 * FROM POC.PUBLIC.NCP_BRONZE;

SELECT 'Test insert count' AS check_type, COUNT(*) AS count FROM POC.PUBLIC.test_insert;
