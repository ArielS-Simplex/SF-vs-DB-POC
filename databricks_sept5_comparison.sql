-- ==============================================================================
-- DATABRICKS COMPARISON - SEPTEMBER 5, 2025
-- Run this on Databricks to compare with Snowflake results
-- Expected Snowflake result: 12,686,818 records
-- ==============================================================================

-- ==============================================================================
-- 1. EXACT SAME ETL LOGIC AS SNOWFLAKE
-- ==============================================================================

WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
)

SELECT 
    'DATABRICKS_SEPT5_FINAL_COUNT' AS metric, 
    COUNT(*) AS value,
    'Compare with Snowflake: 12,686,818' AS snowflake_target
FROM filtered_data;

-- ==============================================================================
-- 2. DETAILED BREAKDOWN FOR COMPARISON
-- ==============================================================================

-- Bronze total for Sept 5th
SELECT 'DATABRICKS_BRONZE_TOTAL' AS metric, COUNT(*) AS value
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05'

UNION ALL

-- After basic filters
SELECT 'DATABRICKS_AFTER_FILTERS' AS metric, COUNT(*) AS value
FROM ncp.silver
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );

-- ==============================================================================
-- 3. HOUR 23 VALIDATION
-- ==============================================================================

WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
)

SELECT 
    'DATABRICKS_HOUR23_VALIDATION' AS analysis_type,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    MIN(MINUTE(transaction_date)) AS earliest_minute,
    MAX(MINUTE(transaction_date)) AS latest_minute,
    COUNT(DISTINCT MINUTE(transaction_date)) AS unique_minutes,
    'Snowflake had 648,425 transactions' AS snowflake_comparison
FROM filtered_data
WHERE HOUR(transaction_date) = 23
GROUP BY HOUR(transaction_date);

-- ==============================================================================
-- 4. POC CONCLUSION COMPARISON
-- ==============================================================================

WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
),
bronze_total AS (
    SELECT COUNT(*) AS total FROM ncp.silver 
    WHERE DATE(transaction_date) = '2025-09-05'
),
final_count AS (
    SELECT COUNT(*) AS total FROM filtered_data
)

SELECT 
    'POC_FINAL_COMPARISON' AS report_type,
    'September 5, 2025 - Complete Data Test' AS description,
    final_count.total AS databricks_final_count,
    12686818 AS snowflake_final_count,
    final_count.total - 12686818 AS difference,
    CASE 
        WHEN final_count.total = 12686818 THEN 'PERFECT 1:1 PARITY ACHIEVED'
        WHEN ABS(final_count.total - 12686818) < 100 THEN 'NEAR PERFECT PARITY'
        ELSE 'INVESTIGATION NEEDED'
    END AS parity_status,
    ROUND((final_count.total * 100.0) / bronze_total.total, 2) AS databricks_retention_pct,
    96.39 AS snowflake_retention_pct
FROM final_count, bronze_total;

-- ==============================================================================
-- INSTRUCTIONS FOR DATABRICKS USER:
-- ==============================================================================

/*
STEPS TO RUN ON DATABRICKS:

1. Table name is now set to: ncp.silver
2. Ensure September 5, 2025 data is available in your bronze table
3. Run all queries above
4. Compare results with Snowflake:
   - Target final count: 12,686,818
   - Target hour 23 count: 648,425
   - Target retention rate: 96.39%

EXPECTED OUTCOME:
If ETL logic is identical, Databricks should produce exactly 12,686,818 records
for September 5, 2025, proving perfect platform parity.

SNOWFLAKE RESULTS TO MATCH:
- Bronze Total: 13,162,623
- After Filters: 13,105,852  
- Final Count: 12,686,818
- Duplicates Removed: 419,034
- Hour 23 Records: 648,425 (minutes 0-59 all present)
*/
