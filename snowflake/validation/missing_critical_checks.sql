-- 🚨 MISSING CRITICAL CHECKS - Run these NOW!

-- 1. COLUMN COUNT (Most Critical!)
SELECT 'SF - Column Count' AS test_type, COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'NCP_SILVER_V2' 
  AND TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_CATALOG = 'POC';
-- MUST BE 174!

-- 2. ROW COUNT
SELECT 'SF - Basic Stats' AS test_type,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT transaction_main_id) AS unique_transactions,
       MIN(DATE(transaction_date)) AS min_date,
       MAX(DATE(transaction_date)) AS max_date
FROM POC.PUBLIC.NCP_SILVER_V2;
-- MUST BE 12,686,818 rows and 2025-09-05 date!

-- 3. LIABILITY_SHIFT CHECK (Your Final Fix!)
SELECT 'SF - Boolean Conversions' AS test_type,
       COUNT(CASE WHEN is_approved = TRUE THEN 1 END) AS approved_true,
       COUNT(CASE WHEN is_declined = TRUE THEN 1 END) AS declined_true,
       COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) AS liability_shift_true,
       COUNT(CASE WHEN is_3d = TRUE THEN 1 END) AS is_3d_true
FROM POC.PUBLIC.NCP_SILVER_V2;
-- LIABILITY_SHIFT_TRUE MUST BE 2,402,585!

-- 4. STATUS FLAGS CHECK
SELECT 'SF - Status Flags' AS test_type,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_populated,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_populated,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_populated,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth3d_populated
FROM POC.PUBLIC.NCP_SILVER_V2;
-- All should be millions of records, not zero!
