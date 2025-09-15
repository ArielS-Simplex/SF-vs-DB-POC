-- ==============================================================================
-- DEBUG ROW COUNT DIFFERENCE
-- Compare original enhanced_working_etl.sql vs incremental version
-- ==============================================================================

-- Test the exact same filtering logic from both versions

SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';

-- 1. Count raw records with original filtering
SELECT 'Raw Bronze Records' AS test_type, COUNT(*) AS count
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) >= $DATE_RANGE_START
  AND DATE(transaction_date) <= $DATE_RANGE_END
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );

-- 2. Count after deduplication (exactly like original)
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) >= $DATE_RANGE_START
      AND DATE(transaction_date) <= $DATE_RANGE_END
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
)
SELECT 'After Deduplication' AS test_type, COUNT(*) AS count
FROM deduped_bronze 
WHERE rn = 1;

-- 3. Check what the original enhanced_working_etl.sql produced
SELECT 'Original Enhanced Working ETL' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2;

-- 4. Check what the incremental version produced
SELECT 'Incremental Version' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2;  -- Same table, but should show current count