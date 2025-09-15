-- 🚨 QUICK 30-SECOND SPOT CHECK - Run this first!

-- Critical numbers that MUST match:
SELECT 
  'CRITICAL VERIFICATION' AS check_type,
  (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'NCP_SILVER_V2') AS column_count,  -- Should be 174
  (SELECT COUNT(*) FROM POC.PUBLIC.NCP_SILVER_V2) AS row_count,  -- Should be 12,686,818
  (SELECT COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) FROM POC.PUBLIC.NCP_SILVER_V2) AS liability_shift_true,  -- Should be 2,402,585
  (SELECT COUNT(DISTINCT DATE(transaction_date)) FROM POC.PUBLIC.NCP_SILVER_V2) AS date_range_count,  -- Should be 1 (single day)
  (SELECT MIN(DATE(transaction_date)) FROM POC.PUBLIC.NCP_SILVER_V2) AS processing_date;  -- Should be 2025-09-05

-- If ANY of these numbers are wrong, STOP and investigate!
