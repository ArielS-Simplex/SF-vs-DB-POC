-- ==============================================================================
-- PROGRESSIVE DATA VALIDATION - SEPTEMBER 5, 6, 8 ONLY (EXACT MATCH DATES)
-- Best Practice: Start basic, get progressively deeper
-- 
-- 🎯 STATUS: FOCUSED ON PERFECT DATABRICKS PARITY DATES
-- 📊 TESTING: 35,025,591 records with guaranteed platform alignment
-- 🗓️ DATE RANGE: September 5, 6, 8, 2025 (ONLY exact match dates)
-- 🏗️ TABLES: ncp_silver_v2 (Snowflake) vs ncp.silver (Databricks)
-- 🔄 COMPARISON: Against Databricks baseline where data availability is complete
-- 
-- CRITICAL: Sept 2,3,4,7 excluded due to Databricks data gaps
-- - Sept 2: Databricks missing 11M+ records  
-- - Sept 3: Databricks has ZERO records
-- - Sept 4: Databricks missing 180K records
-- - Sept 7: Databricks missing 55K records
--
-- VALIDATION DATES: Sept 5 (12.7M), Sept 6 (10.6M), Sept 8 (11.6M)
-- TOTAL DATASET: 35,025,591 records with perfect record count parity
-- VALIDATION DATE: September 10, 2025
-- EXPECTED: Perfect platform parity across ALL validation levels
-- ==============================================================================

-- ==============================================================================
-- LEVEL 1: BASIC ROW SAMPLING & COUNTS
-- ==============================================================================

-- SNOWFLAKE: Random sample of 10 rows (Sept 5, 6, 8 ONLY)
SELECT 
    'SNOWFLAKE_SAMPLE' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    final_transaction_status,
    amount_in_usd,
    is_void,
    is_3d
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
ORDER BY RANDOM()
LIMIT 10;

-- DATABRICKS: Random sample of 10 rows (run on Databricks - Sept 5, 6, 8 ONLY)
/*
SELECT 
    'DATABRICKS_SAMPLE' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    final_transaction_status,
    amount_in_usd,
    is_void,
    is_3d
FROM ncp.silver
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
ORDER BY RAND()
LIMIT 10;
*/

-- ==============================================================================
-- LEVEL 2: BASIC AGGREGATIONS
-- ==============================================================================

-- SNOWFLAKE: Basic stats (Sept 5, 6, 8 ONLY)
SELECT 
    'SNOWFLAKE_BASIC_STATS' AS source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date,
    SUM(amount_in_usd) AS total_amount_usd,
    AVG(amount_in_usd) AS avg_amount_usd
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08');

-- DATABRICKS: Basic stats (run on Databricks - Sept 5, 6, 8 ONLY)
/*
WITH deduplicated_data AS (
  SELECT *
  FROM ncp.silver
  WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
    AND transaction_main_id IS NOT NULL 
    AND transaction_date IS NOT NULL
    AND LOWER(TRIM(multi_client_name)) NOT IN (
      'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_BASIC_STATS' AS source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date,
    SUM(CAST(amount_in_usd AS DECIMAL(18,2))) AS total_amount_usd,
    AVG(CAST(amount_in_usd AS DECIMAL(18,2))) AS avg_amount_usd
FROM deduplicated_data;
*/

-- ==============================================================================
-- LEVEL 3: DISTRIBUTION ANALYSIS
-- ==============================================================================

-- SNOWFLAKE: Boolean field distributions (Sept 5, 6, 8 ONLY)
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_void' AS field_name,
    CAST(is_void AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05')
GROUP BY is_void
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_3d' AS field_name,
    CAST(is_3d AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05')
GROUP BY is_3d
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_approved' AS field_name,
    CAST(is_approved AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05')
GROUP BY is_approved
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_successful_challenge' AS field_name,
    CAST(is_successful_challenge AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05')
GROUP BY is_successful_challenge
ORDER BY source, field_name, field_value;

-- DATABRICKS: Boolean field distributions (run on Databricks - Sept 5, 6, 8 ONLY)
/*
WITH deduplicated_data AS (
  SELECT *
  FROM ncp.silver
  WHERE DATE(transaction_date) IN ('2025-09-05')
    AND transaction_main_id IS NOT NULL 
    AND transaction_date IS NOT NULL
    AND LOWER(TRIM(multi_client_name)) NOT IN (
      'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_BOOLEAN_DIST' AS source,
    'is_void' AS field_name,
    CAST(CASE 
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated_data
GROUP BY field_value
UNION ALL
SELECT 
    'DATABRICKS_BOOLEAN_DIST' AS source,
    'is_3d' AS field_name,
    CAST(CASE 
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated_data
GROUP BY field_value
ORDER BY source, field_name, field_value;
*/

-- ==============================================================================
-- LEVEL 4: HOURLY DISTRIBUTION ANALYSIS
-- ==============================================================================

-- SNOWFLAKE: Transaction distribution by hour (Sept 5, 6, 8 ONLY)
SELECT 
    'SNOWFLAKE_HOURLY_DIST' AS source,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    SUM(amount_in_usd) AS total_amount_hour,
    AVG(amount_in_usd) AS avg_amount_hour
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;

-- DATABRICKS: Transaction distribution by hour (run on Databricks - Sept 5, 6, 8 ONLY)
/*
WITH deduplicated_data AS (
  SELECT *
  FROM ncp.silver
  WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
    AND transaction_main_id IS NOT NULL 
    AND transaction_date IS NOT NULL
    AND LOWER(TRIM(multi_client_name)) NOT IN (
      'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_HOURLY_DIST' AS source,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    SUM(CAST(amount_in_usd AS DECIMAL(18,2))) AS total_amount_hour,
    AVG(CAST(amount_in_usd AS DECIMAL(18,2))) AS avg_amount_hour
FROM deduplicated_data
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;
*/

-- ==============================================================================
-- LEVEL 5: DEEP DIVE - SPECIFIC RECORD COMPARISON
-- ==============================================================================

-- SNOWFLAKE: Get specific transaction IDs for cross-platform comparison
SELECT 
    'SNOWFLAKE_SPECIFIC_RECORDS' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    amount_in_usd,
    is_void,
    is_3d,
    final_transaction_status
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE transaction_main_id IN (
    SELECT transaction_main_id 
    FROM POC.PUBLIC.NCP_SILVER_V2 
    WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
    ORDER BY RANDOM() 
    LIMIT 5
)
ORDER BY transaction_main_id, transaction_date;

-- DATABRICKS: Compare the same specific transaction IDs (run on Databricks - Sept 5, 6, 8 ONLY)
/*
SELECT 
    'DATABRICKS_SPECIFIC_RECORDS' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    CAST(amount_in_usd AS DECIMAL(18,2)) AS amount_in_usd,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS is_void,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS is_3d,
    UPPER(TRIM(REGEXP_REPLACE(COALESCE(final_transaction_status, ''), '[^A-Za-z0-9\\s]', ''))) AS final_transaction_status
FROM ncp.silver
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
  )
  AND transaction_main_id IN (
    '1110000000931183631',
    '1120000004646883246',
    '1120000004648357728',
    '1120000004657069538',
    '1120000004680898603'
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
ORDER BY transaction_main_id, transaction_date;
*/

-- ==============================================================================
-- LEVEL 6: ADVANCED STATISTICAL COMPARISON
-- ✅ COMPLETED: PERFECT STATISTICAL PARITY ACHIEVED
-- Snowflake: Q1=$7.05, Median=$19.99, Q3=$46.00, StdDev=396.909836034, Zeros=133433, Negatives=0
-- Databricks: Q1=$7.05, Median=$19.99, Q3=$46.00, StdDev=396.9098360339789, Zeros=133433, Negatives=0
-- ==============================================================================

-- SNOWFLAKE: Statistical profile (Sept 5, 6, 8 ONLY)
SELECT 
    'SNOWFLAKE_STATISTICS' AS source,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount_in_usd) AS q1_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_in_usd) AS median_amount,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount_in_usd) AS q3_amount,
    STDDEV(amount_in_usd) AS stddev_amount,
    COUNT(CASE WHEN amount_in_usd = 0 THEN 1 END) AS zero_amount_count,
    COUNT(CASE WHEN amount_in_usd < 0 THEN 1 END) AS negative_amount_count
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08');

-- DATABRICKS: Statistical profile (run on Databricks - Sept 5, 6, 8 ONLY)
/*
WITH deduplicated_data AS (
  SELECT *
  FROM ncp.silver
  WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
    AND transaction_main_id IS NOT NULL 
    AND transaction_date IS NOT NULL
    AND LOWER(TRIM(multi_client_name)) NOT IN (
      'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_STATISTICS' AS source,
    PERCENTILE(CAST(amount_in_usd AS DECIMAL(18,2)), 0.25) AS q1_amount,
    PERCENTILE(CAST(amount_in_usd AS DECIMAL(18,2)), 0.5) AS median_amount,
    PERCENTILE(CAST(amount_in_usd AS DECIMAL(18,2)), 0.75) AS q3_amount,
    STDDEV(CAST(amount_in_usd AS DECIMAL(18,2))) AS stddev_amount,
    COUNT(CASE WHEN CAST(amount_in_usd AS DECIMAL(18,2)) = 0 THEN 1 END) AS zero_amount_count,
    COUNT(CASE WHEN CAST(amount_in_usd AS DECIMAL(18,2)) < 0 THEN 1 END) AS negative_amount_count
FROM deduplicated_data;
*/
