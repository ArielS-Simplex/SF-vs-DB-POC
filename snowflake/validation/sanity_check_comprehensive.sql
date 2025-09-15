-- COMPREHENSIVE SANITY CHECK - Pre-Meeting Validation
-- Run this to catch any obvious issues before stakeholder review

-- ========================================
-- 1. BASIC SCHEMA COMPARISON
-- ========================================

-- Column count verification
SELECT 'SF - Column Count' AS test_type, COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'NCP_SILVER_V2' 
  AND TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_CATALOG = 'POC';

-- Expected: Should be 174 columns

-- ========================================  
-- 2. ROW COUNT AND DATE RANGE VERIFICATION
-- ========================================

-- Row count and date range
SELECT 'SF - Basic Stats' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT transaction_main_id) AS unique_transactions,
       MIN(DATE(transaction_date)) AS min_date,
       MAX(DATE(transaction_date)) AS max_date,
       COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_dates,
       COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_transaction_ids
FROM POC.PUBLIC.NCP_SILVER_V2;

-- Expected: 12,686,818 rows, date range 2025-09-05 to 2025-09-05

-- ========================================
-- 3. KEY BUSINESS METRICS SPOT CHECK  
-- ========================================

-- Transaction type distribution
SELECT 'SF - Transaction Types' AS test_type,
       transaction_type,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM POC.PUBLIC.NCP_SILVER_V2), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
GROUP BY transaction_type
ORDER BY count DESC;

-- Result status distribution  
SELECT 'SF - Result Status' AS test_type,
       transaction_result_id,
       COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2
GROUP BY transaction_result_id
ORDER BY count DESC;

-- ========================================
-- 4. DERIVED COLUMNS SPOT CHECK
-- ========================================

-- Status flags population
SELECT 'SF - Status Flags' AS test_type,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_populated,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_populated,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_populated,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth3d_populated
FROM POC.PUBLIC.NCP_SILVER_V2;

-- Boolean conversion spot check
SELECT 'SF - Boolean Conversions' AS test_type,
       COUNT(CASE WHEN is_approved = TRUE THEN 1 END) AS approved_true,
       COUNT(CASE WHEN is_declined = TRUE THEN 1 END) AS declined_true,
       COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) AS liability_shift_true,
       COUNT(CASE WHEN is_3d = TRUE THEN 1 END) AS is_3d_true
FROM POC.PUBLIC.NCP_SILVER_V2;

-- Expected: liability_shift_true should be 2,402,585

-- ========================================
-- 5. DATA TYPE VALIDATION
-- ========================================

-- Check critical column data types
SELECT 'SF - Data Types' AS test_type,
       COLUMN_NAME,
       DATA_TYPE,
       IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'NCP_SILVER_V2' 
  AND COLUMN_NAME IN ('TRANSACTION_MAIN_ID', 'TRANSACTION_DATE', 'AMOUNT_IN_USD', 'LIABILITY_SHIFT', 'IS_APPROVED', 'TRANSACTION_TYPE')
ORDER BY COLUMN_NAME;

-- ========================================
-- 6. NULL PATTERN CHECK
-- ========================================

-- NULL patterns in key fields
SELECT 'SF - NULL Patterns' AS test_type,
       COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_transaction_id,
       COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_date,
       COUNT(CASE WHEN transaction_type IS NULL THEN 1 END) AS null_type,
       COUNT(CASE WHEN amount_in_usd IS NULL OR amount_in_usd = 0 THEN 1 END) AS null_or_zero_amount
FROM POC.PUBLIC.NCP_SILVER_V2;

-- ========================================
-- 7. SAMPLE RECORDS FOR VISUAL INSPECTION
-- ========================================

-- Sample records across different transaction types
SELECT 'SF - Sample Records' AS test_type,
       transaction_main_id,
       transaction_type,
       transaction_result_id,
       is_approved,
       is_declined,
       liability_shift,
       auth_status,
       sale_status,
       ROUND(amount_in_usd, 2) AS amount_usd
FROM POC.PUBLIC.NCP_SILVER_V2 
WHERE transaction_type IN ('AUTH', 'SALE', 'AUTH3D')
ORDER BY transaction_type, transaction_main_id
LIMIT 10;


