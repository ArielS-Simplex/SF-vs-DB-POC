-- URGENT: Check what columns actually exist in bronze data
-- This will help us fix the 'invalid identifier' error

-- 1. Check bronze table columns
SELECT 
    'BRONZE_COLUMNS' AS check_type,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_BRONZE_V2'
  AND TABLE_CATALOG = 'POC'
ORDER BY ORDINAL_POSITION;

-- 2. Quick sample to see actual data structure
SELECT TOP 5 * 
FROM POC.PUBLIC.NCP_BRONZE_V2 
WHERE DATE(transaction_date) = '2025-09-05';
