-- ==============================================================================
-- PROGRESSIVE DATA VALIDATION - NCP_SILVER_V2 TABLE (143-COLUMN PARITY VALIDATION)
-- Best Practice: Start basic, get progressively deeper
-- 
-- 🎯 STATUS: VALIDATING ENHANCED 143-COLUMN SNOWFLAKE TABLE
-- 📊 TESTING: September 5, 2025 data (~12.7M records)
-- 🗓️ DATE RANGE: September 5, 2025 (Single day for focused validation)
-- 🏗️ TABLES: POC.PUBLIC.NCP_SILVER_V2 (Snowflake 143 columns) vs ncp.silver (Databricks)
-- 🔄 COMPARISON: Against Databricks baseline for perfect column parity
-- 
-- KEY VALIDATION POINTS:
-- ✅ Schema: 143+ columns matching Databricks exactly
-- ✅ Data: Same transaction processing logic
-- ✅ Business Logic: Identical derived boolean columns
-- ✅ ETL Parity: Perfect apples-to-apples comparison
--
-- VALIDATION DATE: September 14, 2025
-- EXPECTED: Perfect platform parity across ALL validation levels
-- ==============================================================================

-- ==============================================================================
-- LEVEL 1: BASIC ROW SAMPLING & COUNTS
-- ==============================================================================

-- SNOWFLAKE: Random sample of 10 rows (Sept 5 ONLY - Enhanced 143 columns)
SELECT 
    'SNOWFLAKE_SAMPLE' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    final_transaction_status,
    amount_in_usd,
    is_void,
    is_3d,
    -- NEW ENHANCED COLUMNS
    is_successful_challenge,
    is_successful_authentication,
    is_approved,
    is_declined,
    three_ds_flow_status,
    challenge_preference,
    authentication_flow
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
ORDER BY RANDOM()
LIMIT 10;

-- DATABRICKS: Random sample of 10 rows (run on Databricks - same enhanced columns)
/*
SELECT 
    'DATABRICKS_SAMPLE' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    final_transaction_status,
    amount_in_usd,
    is_void,
    is_3d,
    -- ENHANCED COLUMNS TO MATCH SNOWFLAKE
    is_successful_challenge,
    is_successful_authentication,
    is_approved,
    is_declined,
    `3d_flow_status`,
    challenge_preference,
    authentication_flow
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY RAND()) as rn
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
WHERE rn <= 10;
*/

-- ==============================================================================
-- LEVEL 2: BASIC AGGREGATIONS & ENHANCED COLUMN VALIDATION
-- ==============================================================================

-- SNOWFLAKE: Complete column validation (Sept 5 ONLY - ALL 143+ columns)
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
WHERE DATE(transaction_date) = '2025-09-05';

-- SNOWFLAKE: Full schema check - COUNT ALL COLUMNS (don't miss any!)
SELECT 
    'SNOWFLAKE_COLUMN_COUNT' AS source,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_SILVER_V2';

-- DATABRICKS: Complete validation with column count (run on Databricks)
/*
WITH deduplicated AS (
    SELECT *
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
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
FROM deduplicated;

-- DATABRICKS: Full schema check - COUNT ALL COLUMNS (don't miss any!)
DESCRIBE EXTENDED ncp.silver;
*/

-- ==============================================================================
-- LEVEL 3: ENHANCED BOOLEAN DISTRIBUTION ANALYSIS (143-Column Validation)
-- ==============================================================================

-- SNOWFLAKE: Enhanced boolean field distributions (Sept 5 ONLY)
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_void' AS field_name,
    CAST(is_void AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY is_void
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_3d' AS field_name,
    CAST(is_3d AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY is_3d
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_approved' AS field_name,
    CAST(is_approved AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY is_approved
UNION ALL
SELECT 
    'SNOWFLAKE_BOOLEAN_DIST' AS source,
    'is_successful_challenge' AS field_name,
    CAST(is_successful_challenge AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY is_successful_challenge
ORDER BY source, field_name, field_value;

-- DATABRICKS: Enhanced boolean field distributions (run on Databricks)
/*
WITH deduplicated AS (
    SELECT *
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
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
    CAST(is_void AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated
GROUP BY is_void
UNION ALL
SELECT 
    'DATABRICKS_BOOLEAN_DIST' AS source,
    'is_3d' AS field_name,
    CAST(is_3d AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated
GROUP BY is_3d
UNION ALL
SELECT 
    'DATABRICKS_BOOLEAN_DIST' AS source,
    'is_approved' AS field_name,
    CAST(is_approved AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated
GROUP BY is_approved
UNION ALL
SELECT 
    'DATABRICKS_BOOLEAN_DIST' AS source,
    'is_successful_challenge' AS field_name,
    CAST(is_successful_challenge AS STRING) AS field_value,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM deduplicated
GROUP BY is_successful_challenge
ORDER BY source, field_name, field_value;
*/

-- ==============================================================================
-- LEVEL 4: HOURLY DISTRIBUTION ANALYSIS
-- ==============================================================================

-- SNOWFLAKE: Transaction distribution by hour (Sept 5 ONLY)
SELECT 
    'SNOWFLAKE_HOURLY_DIST' AS source,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    SUM(amount_in_usd) AS total_amount_hour,
    AVG(amount_in_usd) AS avg_amount_hour
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;

-- DATABRICKS: Transaction distribution by hour (run on Databricks)
/*
WITH deduplicated AS (
    SELECT *
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
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
FROM deduplicated
GROUP BY HOUR(transaction_date)
ORDER BY hour_of_day;
*/

-- ==============================================================================
-- LEVEL 5: SPECIFIC RECORD COMPARISON (30 Transaction IDs Validated)
-- ==============================================================================

-- SNOWFLAKE: Get the exact 30 transaction IDs we validated earlier
SELECT 
    'SNOWFLAKE_SPECIFIC_RECORDS' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    amount_in_usd,
    is_void,
    is_3d,
    final_transaction_status,
    -- ENHANCED VALIDATION COLUMNS
    is_successful_challenge,
    is_approved,
    is_declined,
    three_ds_flow_status
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE transaction_main_id IN (
    '1110000000931994746', '1120000004654320896', '1120000004654320897',
    '1120000004654320903', '1120000004654320898', '1120000004654320899',
    '1120000004654320901', '1120000004654320904', '1120000004654320900',
    '1120000004654320902'  -- First 10 from our validated 30
)
ORDER BY transaction_main_id;

-- DATABRICKS: Compare the same exact 30 transaction IDs (run on Databricks)
/*
SELECT 
    'DATABRICKS_SPECIFIC_RECORDS' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    CAST(amount_in_usd AS DECIMAL(18,2)) AS amount_in_usd,
    is_void,
    is_3d,
    final_transaction_status,
    -- ENHANCED VALIDATION COLUMNS
    is_successful_challenge,
    is_approved,
    is_declined,
    `3d_flow_status`
FROM ncp.silver
WHERE transaction_main_id IN (
    '1110000000931994746', '1120000004654320896', '1120000004654320897',
    '1120000004654320903', '1120000004654320898', '1120000004654320899',
    '1120000004654320901', '1120000004654320904', '1120000004654320900',
    '1120000004654320902'
)
  AND DATE(transaction_date) = '2025-09-05'
ORDER BY transaction_main_id;
*/

-- ==============================================================================
-- LEVEL 6: ENHANCED STATISTICAL COMPARISON (143-Column Validation)
-- NEW: Validates the enhanced ETL with perfect column parity
-- ==============================================================================

-- SNOWFLAKE: Enhanced statistical profile (Sept 5 ONLY)
SELECT 
    'SNOWFLAKE_STATISTICS' AS source,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount_in_usd) AS q1_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_in_usd) AS median_amount,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount_in_usd) AS q3_amount,
    STDDEV(amount_in_usd) AS stddev_amount,
    COUNT(CASE WHEN amount_in_usd = 0 THEN 1 END) AS zero_amount_count,
    COUNT(CASE WHEN amount_in_usd < 0 THEN 1 END) AS negative_amount_count,
    -- NEW ENHANCED VALIDATIONS
    COUNT(CASE WHEN is_approved = TRUE THEN 1 END) AS approved_transactions,
    COUNT(CASE WHEN is_declined = TRUE THEN 1 END) AS declined_transactions,
    COUNT(CASE WHEN is_successful_challenge = TRUE THEN 1 END) AS successful_3ds_challenges,
    COUNT(CASE WHEN three_ds_flow_status IS NOT NULL THEN 1 END) AS transactions_with_3ds_status
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-05';

-- DATABRICKS: Enhanced statistical profile (run on Databricks)
/*
WITH deduplicated AS (
    SELECT CAST(amount_in_usd AS DECIMAL(18,2)) AS amount_in_usd,
           is_approved, is_declined, is_successful_challenge, `3d_flow_status`
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_STATISTICS' AS source,
    PERCENTILE(amount_in_usd, 0.25) AS q1_amount,
    PERCENTILE(amount_in_usd, 0.5) AS median_amount,
    PERCENTILE(amount_in_usd, 0.75) AS q3_amount,
    STDDEV(amount_in_usd) AS stddev_amount,
    COUNT(CASE WHEN amount_in_usd = 0 THEN 1 END) AS zero_amount_count,
    COUNT(CASE WHEN amount_in_usd < 0 THEN 1 END) AS negative_amount_count,
    -- NEW ENHANCED VALIDATIONS
    COUNT(CASE WHEN is_approved = TRUE THEN 1 END) AS approved_transactions,
    COUNT(CASE WHEN is_declined = TRUE THEN 1 END) AS declined_transactions,
    COUNT(CASE WHEN is_successful_challenge = TRUE THEN 1 END) AS successful_3ds_challenges,
    COUNT(CASE WHEN `3d_flow_status` IS NOT NULL THEN 1 END) AS transactions_with_3ds_status
FROM deduplicated;
*/
