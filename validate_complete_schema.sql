-- COMPLETE SCHEMA VALIDATION - 143 COLUMN VERIFICATION
-- This validates that our enhanced ETL now produces the full Databricks schema

-- 1. Count columns in enhanced silver table
SELECT 
    'ENHANCED_SNOWFLAKE_COLUMN_COUNT' AS validation,
    COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_SILVER_V2'
  AND TABLE_CATALOG = 'POC';

-- 2. List all columns to verify 143 columns exist
SELECT 
    'ENHANCED_SNOWFLAKE_COLUMNS' AS validation,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC' 
  AND TABLE_NAME = 'NCP_SILVER_V2'
  AND TABLE_CATALOG = 'POC'
ORDER BY ORDINAL_POSITION;

-- 3. Sample data validation - test specific records
SELECT 
    'ENHANCED_SCHEMA_SAMPLE' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_main_id) AS unique_transactions,
    -- Test core columns work
    COUNT(CASE WHEN is_approved = TRUE THEN 1 END) AS approved_transactions,
    COUNT(CASE WHEN is_declined = TRUE THEN 1 END) AS declined_transactions,
    -- Test new columns exist and aren't all NULL
    COUNT(CASE WHEN transaction_type_id IS NOT NULL THEN 1 END) AS has_transaction_type_id,
    COUNT(CASE WHEN device_type IS NOT NULL THEN 1 END) AS has_device_type,
    COUNT(CASE WHEN bin_country IS NOT NULL THEN 1 END) AS has_bin_country,
    COUNT(CASE WHEN processor_name IS NOT NULL THEN 1 END) AS has_processor_name,
    COUNT(CASE WHEN merchant_country IS NOT NULL THEN 1 END) AS has_merchant_country
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) IN ('2025-09-05', '2025-09-06', '2025-09-08')
LIMIT 1000;

-- 4. Compare against expected Databricks schema count
SELECT 
    'SCHEMA_PARITY_CHECK' AS validation,
    CASE 
        WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'NCP_SILVER_V2' AND TABLE_CATALOG = 'POC') = 143 
        THEN '✅ PERFECT PARITY - 143 COLUMNS ACHIEVED'
        ELSE '❌ SCHEMA MISMATCH - MISSING COLUMNS: ' || 
             (143 - (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'NCP_SILVER_V2' AND TABLE_CATALOG = 'POC'))
    END AS parity_status;
