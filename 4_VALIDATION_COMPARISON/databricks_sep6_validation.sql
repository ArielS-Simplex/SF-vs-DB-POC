-- ==============================================================================
-- DATABRICKS VALIDATION QUERIES - September 6, 2025
-- Compare against Snowflake silver table for same date
-- ==============================================================================

-- Note: Adjust the database and table names according to your Databricks setup
-- SET VALIDATION_DATE = '2025-09-06';
-- SET TARGET_TABLE = 'your_database.your_schema.transactions_silver';

-- 1. BASIC ROW COUNT AND DATE VALIDATION
SELECT 
    'DATABRICKS_ROW_COUNT_SEP6' AS check_type,
    COUNT(*) AS total_rows,
    MIN(transaction_date) AS min_date,
    MAX(transaction_date) AS max_date,
    COUNT(DISTINCT DATE(transaction_date)) AS unique_dates
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';

-- 2. BUSINESS LOGIC SUMMARY FOR SEP 6
SELECT 
    'DATABRICKS_BUSINESS_LOGIC_SEP6' AS check_type,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_count,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_count,
    COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) AS successful_auth_count,
    COUNT(CASE WHEN is_successful_frictionless = true THEN 1 END) AS frictionless_success_count,
    COUNT(CASE WHEN is_successful_challenge = true THEN 1 END) AS challenge_success_count,
    ROUND(COUNT(CASE WHEN is_approved = true THEN 1 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct,
    ROUND(COUNT(CASE WHEN is_declined = true THEN 1 END) * 100.0 / COUNT(*), 2) AS decline_rate_pct
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06';

-- 3. TRANSACTION TYPE BREAKDOWN FOR SEP 6
SELECT 
    'DATABRICKS_TRANSACTION_TYPES_SEP6' AS check_type,
    transaction_type,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) AS success_count,
    COUNT(CASE WHEN transaction_result_id = '1008' THEN 1 END) AS decline_count,
    ROUND(COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY transaction_type
ORDER BY total_count DESC;

-- 4. CLIENT BREAKDOWN FOR SEP 6
SELECT 
    'DATABRICKS_CLIENT_BREAKDOWN_SEP6' AS check_type,
    multi_client_name,
    COUNT(*) AS transaction_count,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_count,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_count,
    ROUND(COUNT(CASE WHEN is_approved = true THEN 1 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY multi_client_name
ORDER BY transaction_count DESC
LIMIT 10;

-- 5. AUTHENTICATION FLOW BREAKDOWN FOR SEP 6
SELECT 
    'DATABRICKS_AUTH_FLOW_SEP6' AS check_type,
    authentication_flow,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) AS success_count,
    COUNT(CASE WHEN is_successful_authentication = false THEN 1 END) AS failure_count,
    ROUND(COUNT(CASE WHEN is_successful_authentication = true THEN 1 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
  AND authentication_flow IS NOT NULL
GROUP BY authentication_flow
ORDER BY total_count DESC;

-- 6. CURRENCY AND AMOUNT SUMMARY FOR SEP 6
SELECT 
    'DATABRICKS_CURRENCY_AMOUNTS_SEP6' AS check_type,
    currency_code,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount_in_usd), 2) AS total_amount_usd,
    ROUND(AVG(amount_in_usd), 2) AS avg_amount_usd,
    ROUND(MIN(amount_in_usd), 2) AS min_amount_usd,
    ROUND(MAX(amount_in_usd), 2) AS max_amount_usd
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
  AND amount_in_usd IS NOT NULL
  AND currency_code IS NOT NULL
GROUP BY currency_code
ORDER BY transaction_count DESC
LIMIT 10;

-- 7. DATA QUALITY CHECK FOR SEP 6
SELECT 
    'DATABRICKS_DATA_QUALITY_SEP6' AS check_type,
    data_quality_flag,
    date_quality_flag,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM transactions_silver
WHERE DATE(transaction_date) = '2025-09-06'
GROUP BY data_quality_flag, date_quality_flag
ORDER BY record_count DESC;
