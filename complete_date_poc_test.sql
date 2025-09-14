-- ==============================================================================
-- COMPLETE DATE POC TEST - SEPTEMBER 5, 2025
-- Testing ETL with complete 24-hour data coverage
-- Expected: Perfect 1:1 parity with Databricks logic
-- ==============================================================================

SET VALIDATION_DATE = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_SEPT5_TEST';

-- ==============================================================================
-- 1. CLEAN SLATE - DROP AND RECREATE TARGET TABLE
-- ==============================================================================

DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

-- ==============================================================================
-- 2. EXACT DATABRICKS PARITY ETL - SEPTEMBER 5TH
-- ==============================================================================

CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) = $VALIDATION_DATE
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
    -- Core transaction fields
    transaction_main_id,
    transaction_date,
    
    -- Boolean normalization - EXACT Databricks logic using actual columns
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_void,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_sale_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_sale_3d,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_external_mpi, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_external_mpi, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_external_mpi,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_prepaid, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_prepaid, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_prepaid,
    
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('yes', 'true', '1') THEN TRUE
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('no', 'false', '0', '') THEN FALSE
        ELSE NULL
    END AS is_3d,
    
    -- String cleaning - exact Databricks approach using actual columns
    CASE 
        WHEN TRIM(COALESCE(transaction_type, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(transaction_type, ''), '[^A-Za-z0-9\\s]', '')))
    END AS transaction_type,
    
    CASE 
        WHEN TRIM(COALESCE(multi_client_name, '')) = '' THEN NULL
        ELSE TRIM(REGEXP_REPLACE(COALESCE(multi_client_name, ''), '[^A-Za-z0-9\\s]', ''))
    END AS multi_client_name,
    
    CASE 
        WHEN TRIM(COALESCE(final_transaction_status, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(final_transaction_status, ''), '[^A-Za-z0-9\\s]', '')))
    END AS final_transaction_status,
    
    CASE 
        WHEN TRIM(COALESCE(card_scheme, '')) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(COALESCE(card_scheme, ''), '[^A-Za-z0-9\\s]', '')))
    END AS card_scheme,
    
    -- Numeric fields using actual columns
    COALESCE(TRY_CAST(amount_in_usd AS DECIMAL(18,2)), 0) AS amount_in_usd,
    COALESCE(TRY_CAST(approved_amount_in_usd AS DECIMAL(18,2)), 0) AS approved_amount_in_usd,
    COALESCE(TRY_CAST(original_currency_amount AS DECIMAL(18,2)), 0) AS original_currency_amount,
    
    -- Forced NULL columns (Databricks specific)
    NULL AS processing_status,
    NULL AS validation_flag,
    NULL AS databricks_specific_field1,
    NULL AS databricks_specific_field2,
    
    -- Additional Databricks columns
    'SNOWFLAKE_PROCESSED' AS data_source,
    CURRENT_TIMESTAMP() AS snowflake_processed_at,
    
    -- Metadata
    inserted_at
    
FROM filtered_data
ORDER BY transaction_date, transaction_main_id;

-- ==============================================================================
-- 3. COMPREHENSIVE VALIDATION - SEPTEMBER 5TH
-- ==============================================================================

SELECT 'SEPT5_FINAL_COUNT' AS metric, COUNT(*) AS value FROM IDENTIFIER($TARGET_TABLE)
UNION ALL
SELECT 'SEPT5_BRONZE_TOTAL' AS metric, COUNT(*) AS value FROM IDENTIFIER($SOURCE_TABLE) WHERE DATE(transaction_date) = $VALIDATION_DATE
UNION ALL
SELECT 'SEPT5_AFTER_FILTERS' AS metric, COUNT(*) AS value FROM IDENTIFIER($SOURCE_TABLE) 
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')
UNION ALL
SELECT 'SEPT5_DUPLICATES_REMOVED' AS metric, 
    (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE) 
     WHERE DATE(transaction_date) = $VALIDATION_DATE
       AND transaction_main_id IS NOT NULL 
       AND transaction_date IS NOT NULL
       AND LOWER(TRIM(multi_client_name)) NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi')) - 
    (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE)) AS value
FROM DUAL;

-- ==============================================================================
-- 4. DATA QUALITY VALIDATION
-- ==============================================================================

SELECT 
    'SEPT5_BOOLEAN_DISTRIBUTION' AS analysis_type,
    'is_void' AS field_name,
    CAST(is_void AS STRING) AS value,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY is_void
UNION ALL
SELECT 
    'SEPT5_BOOLEAN_DISTRIBUTION' AS analysis_type,
    'is_sale_3d' AS field_name,
    CAST(is_sale_3d AS STRING) AS value,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY is_sale_3d
UNION ALL
SELECT 
    'SEPT5_BOOLEAN_DISTRIBUTION' AS analysis_type,
    'is_3d' AS field_name,
    CAST(is_3d AS STRING) AS value,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY is_3d
ORDER BY analysis_type, field_name, value;

-- ==============================================================================
-- 5. HOUR 23 VALIDATION - COMPLETE COVERAGE
-- ==============================================================================

SELECT 
    'SEPT5_HOUR23_VALIDATION' AS analysis_type,
    HOUR(transaction_date) AS hour_of_day,
    COUNT(*) AS transaction_count,
    MIN(MINUTE(transaction_date)) AS earliest_minute,
    MAX(MINUTE(transaction_date)) AS latest_minute,
    COUNT(DISTINCT MINUTE(transaction_date)) AS unique_minutes
FROM IDENTIFIER($TARGET_TABLE)
WHERE HOUR(transaction_date) = 23
GROUP BY HOUR(transaction_date);

-- ==============================================================================
-- 6. SUMMARY REPORT
-- ==============================================================================

SELECT 
    'SEPT5_POC_SUMMARY' AS report_type,
    'September 5, 2025 - Complete 24-hour data test' AS description,
    (SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE)) AS final_silver_count,
    (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE) WHERE DATE(transaction_date) = $VALIDATION_DATE) AS bronze_total,
    ROUND(
        ((SELECT COUNT(*) FROM IDENTIFIER($TARGET_TABLE)) * 100.0) / 
        (SELECT COUNT(*) FROM IDENTIFIER($SOURCE_TABLE) WHERE DATE(transaction_date) = $VALIDATION_DATE), 2
    ) AS retention_percentage,
    'Ready for Databricks comparison' AS status;
