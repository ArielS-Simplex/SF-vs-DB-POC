-- ==============================================================================
-- CHECK CURRENT TABLE COUNTS TO DEBUG THE DISCREPANCY
-- ==============================================================================

-- 1. Check what's currently in the target table
SELECT 'Current NCP_SILVER_V2 Count' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 2. Check what the original enhanced_working_etl.sql would produce (raw count)
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM POC.PUBLIC.NCP_BRONZE_V2
    WHERE DATE(transaction_date) >= '2025-09-05'
      AND DATE(transaction_date) <= '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
)
SELECT 'Enhanced Working ETL Logic Count' AS test_type, COUNT(*) AS count
FROM deduped_bronze 
WHERE rn = 1;

-- 3. Check if NCP_BRONZE_V2 data has changed
SELECT 'NCP_BRONZE_V2 Sept 5 Records' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05';

-- 4. Check the exact same filtering as enhanced_working_etl.sql (before deduplication)
SELECT 'Before Deduplication Count' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) >= '2025-09-05'
  AND DATE(transaction_date) <= '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );