-- SIMPLIFIED ETL TEST - Remove all filters to see if ANY data processes
-- Run this to test if the basic logic works

-- Drop and recreate table
DROP TABLE IF EXISTS POC.PUBLIC.transactions_silver;

-- Create target table with simple structure
CREATE TABLE POC.PUBLIC.transactions_silver AS
WITH simple_data AS (
    SELECT 
        transaction_main_id,
        transaction_date,
        transaction_type,
        multi_client_name,
        transaction_result_id,
        -- Add a few derived columns to test business logic
        CASE WHEN transaction_result_id = '1006' THEN true ELSE false END AS is_approved,
        inserted_at
    FROM POC.PUBLIC.NCP_BRONZE
    -- DATE FILTER - last 1 year to catch February 2025 data
    WHERE transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND TRANSACTION_DATE >= CURRENT_DATE() - INTERVAL '1 year'
    -- Remove test client filter temporarily
    -- AND multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
    LIMIT 1000  -- Just get first 1000 records for testing
)
SELECT * FROM simple_data;

-- Check results
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN is_approved = true THEN 1 END) as approved_count,
    MIN(transaction_date) as min_date,
    MAX(transaction_date) as max_date
FROM POC.PUBLIC.transactions_silver;

-- Show sample data
SELECT * FROM POC.PUBLIC.transactions_silver LIMIT 5;
