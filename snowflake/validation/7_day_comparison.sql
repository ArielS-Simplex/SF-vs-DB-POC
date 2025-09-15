-- 7-DAY COMPARISON: Snowflake vs Databricks Row Counts by Date
-- Run this to validate data volume parity across the date range

-- ========================================
-- SNOWFLAKE SIDE - Daily Counts
-- ========================================

-- Count by individual date in Snowflake Silver (7-day table)
SELECT 
    'SF Silver 7-Day' AS source,
    DATE(transaction_date) AS transaction_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    MIN(transaction_date) AS earliest_time,
    MAX(transaction_date) AS latest_time
FROM POC.PUBLIC.NCP_SILVER_V2_7_DAYS
GROUP BY DATE(transaction_date)
ORDER BY transaction_date;

-- Total summary for Snowflake
SELECT 
    'SF Silver 7-Day TOTAL' AS source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_main_id) AS total_unique_transactions,
    COUNT(DISTINCT DATE(transaction_date)) AS unique_dates,
    MIN(DATE(transaction_date)) AS start_date,
    MAX(DATE(transaction_date)) AS end_date
FROM POC.PUBLIC.NCP_SILVER_V2_7_DAYS;

-- ========================================  
-- SNOWFLAKE BRONZE COMPARISON
-- ========================================

-- Count by date in Snowflake Bronze (filtered same way as ETL)
SELECT 
    'SF Bronze Filtered' AS source,
    DATE(transaction_date) AS transaction_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) BETWEEN '2025-09-02' AND '2025-09-08'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  )
GROUP BY DATE(transaction_date)
ORDER BY transaction_date;

-- Bronze total
SELECT 
    'SF Bronze Filtered TOTAL' AS source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_main_id) AS total_unique_transactions,
    COUNT(DISTINCT DATE(transaction_date)) AS unique_dates
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) BETWEEN '2025-09-02' AND '2025-09-08'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );

-- ========================================
-- DATABRICKS COMPARISON TEMPLATE
-- ========================================

-- Run these queries in Databricks for comparison:
/*
%sql
SELECT 
    'DB Silver 7-Day' AS source,
    DATE(transaction_date) AS transaction_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    MIN(transaction_date) AS earliest_time,
    MAX(transaction_date) AS latest_time
FROM ncp.silver
WHERE DATE(transaction_date) BETWEEN '2025-09-02' AND '2025-09-08'
GROUP BY DATE(transaction_date)
ORDER BY transaction_date;

%sql
SELECT 
    'DB Silver 7-Day TOTAL' AS source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_main_id) AS total_unique_transactions,
    COUNT(DISTINCT DATE(transaction_date)) AS unique_dates,
    MIN(DATE(transaction_date)) AS start_date,
    MAX(DATE(transaction_date)) AS end_date
FROM ncp.silver
WHERE DATE(transaction_date) BETWEEN '2025-09-02' AND '2025-09-08';
*/

-- ========================================
-- COMPARISON ANALYSIS
-- ========================================

-- After running both sides, compare:
-- 1. Daily row counts should match exactly
-- 2. Total row counts should match exactly  
-- 3. Date ranges should be identical
-- 4. Unique transaction counts should match

-- Expected pattern (based on Sept 5 single day = 12,686,818):
-- Each day should have 10M+ to 15M+ transactions
-- 7 days total should be 70M+ to 100M+ transactions
