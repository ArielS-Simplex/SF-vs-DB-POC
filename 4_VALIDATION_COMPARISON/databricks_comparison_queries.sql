-- ==============================================================================
-- DATABRICKS COMPARISON QUERIES
-- Run these on Databricks to identify the exact 4,479 row difference
-- ==============================================================================

-- INSTRUCTIONS FOR DATABRICKS:
-- Replace 'your_databricks_silver_table' with your actual table name
-- Use the same date: 2025-09-06
-- Copy results back for comparison

-- ==============================================================================
-- QUERY 1: BASIC ROW COUNT VALIDATION
-- ==============================================================================

SELECT 
    'DATABRICKS_ROW_COUNT' AS platform,
    '2025-09-06' AS validation_date,
    COUNT(*) AS total_row_count
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06';

-- ==============================================================================
-- QUERY 2: ROW COUNT BY TRANSACTION TYPE
-- Compare with our Snowflake breakdown
-- ==============================================================================

SELECT 
    'DATABRICKS_TRANSACTION_BREAKDOWN' AS platform,
    transaction_type,
    transaction_result_id,
    final_transaction_status,
    COUNT(*) AS count_in_databricks
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY transaction_type, transaction_result_id, final_transaction_status
ORDER BY count_in_databricks DESC;

-- ==============================================================================
-- QUERY 3: TEST CLIENT FILTERING CHECK
-- Verify what test clients Databricks actually filters
-- ==============================================================================

-- First check what test clients exist in bronze before filtering
SELECT 
    'DATABRICKS_TEST_CLIENTS_IN_BRONZE' AS check_type,
    multi_client_name,
    COUNT(*) AS count_in_bronze
FROM your_databricks_bronze_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND (LOWER(multi_client_name) LIKE '%test%' 
       OR LOWER(multi_client_name) LIKE '%demo%'
       OR LOWER(multi_client_name) LIKE '%monitor%')
GROUP BY multi_client_name
ORDER BY count_in_bronze DESC;

-- Then check what remains in silver after filtering
SELECT 
    'DATABRICKS_TEST_CLIENTS_IN_SILVER' AS check_type,
    multi_client_name,
    COUNT(*) AS count_in_silver
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND (LOWER(multi_client_name) LIKE '%test%' 
       OR LOWER(multi_client_name) LIKE '%demo%'
       OR LOWER(multi_client_name) LIKE '%monitor%')
GROUP BY multi_client_name
ORDER BY count_in_silver DESC;

-- ==============================================================================
-- QUERY 4: DEDUPLICATION CHECK
-- Verify Databricks deduplication logic
-- ==============================================================================

SELECT 
    'DATABRICKS_DEDUPLICATION_CHECK' AS check_type,
    COUNT(*) AS total_records_after_dedup,
    COUNT(DISTINCT CONCAT(transaction_main_id, '|', DATE(transaction_date))) AS unique_id_date_combinations,
    COUNT(*) - COUNT(DISTINCT CONCAT(transaction_main_id, '|', DATE(transaction_date))) AS potential_duplicates_kept,
    COUNT(DISTINCT transaction_main_id) AS unique_transaction_ids
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06';

-- ==============================================================================
-- QUERY 5: TIME BOUNDARY ANALYSIS
-- Check hour 0 and hour 23 distribution
-- ==============================================================================

SELECT 
    'DATABRICKS_TIME_BOUNDARY' AS check_type,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    MIN(transaction_date) AS earliest_in_hour,
    MAX(transaction_date) AS latest_in_hour
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND (HOUR(transaction_date) = 0 OR HOUR(transaction_date) = 23)
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;

-- ==============================================================================
-- QUERY 6: CLIENT FILTERING DETAILED CHECK
-- See exactly which clients Databricks includes vs excludes
-- ==============================================================================

SELECT 
    'DATABRICKS_CLIENT_SAMPLE' AS check_type,
    multi_client_name,
    COUNT(*) AS client_count
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY multi_client_name
HAVING COUNT(*) BETWEEN 1 AND 1000  -- Focus on low-volume clients
ORDER BY client_count DESC
LIMIT 50;

-- ==============================================================================
-- QUERY 7: DATA QUALITY FLAGS CHECK
-- Compare data quality distribution
-- ==============================================================================

SELECT 
    'DATABRICKS_DATA_QUALITY' AS check_type,
    -- If you have data_quality_flag column
    -- data_quality_flag,
    -- date_quality_flag,
    COUNT(*) AS record_count
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
-- GROUP BY data_quality_flag, date_quality_flag
-- ORDER BY record_count DESC;

-- ==============================================================================
-- QUERY 8: SPECIFIC MISSING RECORDS INVESTIGATION
-- Find patterns in what Databricks includes that we might be missing
-- ==============================================================================

SELECT 
    'DATABRICKS_EDGE_CASES' AS check_type,
    processor_name,
    currency_code,
    bin_country,
    COUNT(*) AS count_in_databricks
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY processor_name, currency_code, bin_country
HAVING COUNT(*) < 10  -- Focus on low-volume edge cases
ORDER BY count_in_databricks ASC
LIMIT 20;

-- ==============================================================================
-- QUERY 9: SAMPLE OF ACTUAL TRANSACTION IDs
-- Get sample IDs to cross-reference
-- ==============================================================================

SELECT 
    'DATABRICKS_SAMPLE_IDS' AS check_type,
    transaction_main_id,
    transaction_date,
    transaction_type,
    multi_client_name,
    transaction_result_id
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
ORDER BY RANDOM()  -- or RAND() depending on Databricks dialect
LIMIT 100;

-- ==============================================================================
-- QUERY 10: AUTH3D SPECIFIC CHECK
-- Since we found differences in Auth3D logic
-- ==============================================================================

SELECT 
    'DATABRICKS_AUTH3D_CHECK' AS check_type,
    transaction_type,
    COUNT(*) AS count_by_type,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND transaction_type = 'Auth3D'
GROUP BY transaction_type;
