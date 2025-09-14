-- ==============================================================================
-- FOCUSED DIFFERENCE ANALYSIS - SPECIFIC PATTERN INVESTIGATION
-- Based on results from remaining_4k_difference_investigation.sql
-- ==============================================================================

-- FINDINGS FROM INITIAL ANALYSIS:
-- 1. No deduplication issues (10,584,798 unique records)
-- 2. Time boundary data looks normal (578K at hour 0, 283K at hour 23)
-- 3. Found potential "dev/test" clients that might be filtered differently
-- 4. Found very low-volume edge cases that might be handled differently

SET VALIDATION_DATE = '2025-09-06';
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- INVESTIGATION 1: DEV/TEST CLIENT DETAILED ANALYSIS
-- These clients might be filtered differently between platforms
SELECT 
    'DEV_TEST_CLIENTS_DETAILED' AS analysis_type,
    multi_client_name,
    transaction_type,
    final_transaction_status,
    COUNT(*) AS transaction_count,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND multi_client_name IN (
    'DEVELUX LIMITED Multi',
    'GM APPDEV LIMITED Multi', 
    'Atrius Development Group Corporation Multi',
    'Jeux devasion Fox in a Box Sherbrooke Ecomm Multi',
    'MP Developers s.r.o. Multi',
    'Prod Testing Multi'
  )
GROUP BY multi_client_name, transaction_type, final_transaction_status
ORDER BY multi_client_name, transaction_count DESC;

-- INVESTIGATION 2: EDGE CASE PROCESSOR/CURRENCY COMBINATIONS
-- These single-occurrence combinations might be handled differently
SELECT 
    'EDGE_CASE_PROCESSORS' AS analysis_type,
    processor_name,
    currency_code,
    bin_country,
    transaction_type,
    final_transaction_status,
    transaction_date,
    transaction_main_id
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND CONCAT(processor_name, '|', currency_code, '|', bin_country) IN (
    'Nuvei Acquirer - Visa|GBP|tj',
    'Nuvei Acquirer SG USD - Visa|KRW|pr',
    'Nuvei Acquirer - Visa|GBP|am',
    'Nuvei Acquirer - MasterCard|ZAR|au',
    'WorldPay Direct|DKK|it',
    'AIBMS-Direct Payment|USD|ch',
    'Nuvei Acquirer - MasterCard|BRL|ch',
    'Nuvei NA Hub Citizens US PI|USD|zw',
    'Nuvei Acquirer - Visa|AED|cy',
    'Nuvei Acquirer - Visa|CZK|iq'
  )
ORDER BY processor_name, currency_code, bin_country;

-- INVESTIGATION 3: TIME ZONE / TIMESTAMP PRECISION ANALYSIS
-- Check if there are millisecond-level differences at day boundaries
SELECT 
    'TIMESTAMP_PRECISION_ANALYSIS' AS analysis_type,
    'EXACTLY_MIDNIGHT' AS boundary_type,
    COUNT(*) AS exact_midnight_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_date = '2025-09-06 00:00:00.000'

UNION ALL

SELECT 
    'TIMESTAMP_PRECISION_ANALYSIS' AS analysis_type,
    'LAST_MILLISECOND' AS boundary_type,
    COUNT(*) AS last_millisecond_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE transaction_date >= '2025-09-06 23:59:59.000' 
  AND transaction_date < '2025-09-07 00:00:00.000';

-- INVESTIGATION 4: TRANSACTION_RESULT_ID PATTERNS
-- Check if there are subtle differences in how result IDs are mapped
SELECT 
    'RESULT_ID_MAPPING_CHECK' AS analysis_type,
    transaction_result_id,
    final_transaction_status,
    transaction_type,
    COUNT(*) AS count_in_snowflake,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 6) AS precise_percentage
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND transaction_result_id IN (1000, 1004, 1008, 1011)  -- Focus on non-1006 results
GROUP BY transaction_result_id, final_transaction_status, transaction_type
ORDER BY transaction_result_id, count_in_snowflake DESC;

-- INVESTIGATION 5: POTENTIAL NULL/EMPTY VALUE HANDLING
-- Check for any subtle differences in how NULL values are handled
SELECT 
    'NULL_VALUE_ANALYSIS' AS analysis_type,
    'PROCESSOR_NAME_NULLS' AS field_name,
    COUNT(*) AS null_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (processor_name IS NULL OR processor_name = '')

UNION ALL

SELECT 
    'NULL_VALUE_ANALYSIS' AS analysis_type,
    'CURRENCY_CODE_NULLS' AS field_name,
    COUNT(*) AS null_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (currency_code IS NULL OR currency_code = '')

UNION ALL

SELECT 
    'NULL_VALUE_ANALYSIS' AS analysis_type,
    'BIN_COUNTRY_NULLS' AS field_name,
    COUNT(*) AS null_count
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND (bin_country IS NULL OR bin_country = '');

-- INVESTIGATION 6: DATABRICKS COMPARISON QUERY TEMPLATE
-- Use this template to run the EXACT same queries on Databricks
/*
-- RUN THIS ON DATABRICKS TO COMPARE:

-- 1. DEV/TEST CLIENTS CHECK
SELECT 
    'DEV_TEST_CLIENTS_DETAILED' AS analysis_type,
    multi_client_name,
    transaction_type,
    final_transaction_status,
    COUNT(*) AS transaction_count,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND multi_client_name IN (
    'DEVELUX LIMITED Multi',
    'GM APPDEV LIMITED Multi', 
    'Atrius Development Group Corporation Multi',
    'Jeux devasion Fox in a Box Sherbrooke Ecomm Multi',
    'MP Developers s.r.o. Multi',
    'Prod Testing Multi'
  )
GROUP BY multi_client_name, transaction_type, final_transaction_status
ORDER BY multi_client_name, transaction_count DESC;

-- 2. TOTAL COUNT CHECK FOR THESE CLIENTS
SELECT COUNT(*) as total_dev_test_clients
FROM your_databricks_silver_table
WHERE DATE(transaction_date) = '2025-09-06'
  AND multi_client_name IN (
    'DEVELUX LIMITED Multi',
    'GM APPDEV LIMITED Multi', 
    'Atrius Development Group Corporation Multi',
    'Jeux devasion Fox in a Box Sherbrooke Ecomm Multi',
    'MP Developers s.r.o. Multi',
    'Prod Testing Multi'
  );
*/

-- SUMMARY COUNTS FOR QUICK COMPARISON
SELECT 
    'SUMMARY_COMPARISON' AS analysis_type,
    'TOTAL_RECORDS' AS metric,
    COUNT(*) AS snowflake_value
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE

UNION ALL

SELECT 
    'SUMMARY_COMPARISON' AS analysis_type,
    'DEV_TEST_CLIENTS_TOTAL' AS metric,
    COUNT(*) AS snowflake_value
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND multi_client_name IN (
    'DEVELUX LIMITED Multi',
    'GM APPDEV LIMITED Multi', 
    'Atrius Development Group Corporation Multi',
    'Jeux devasion Fox in a Box Sherbrooke Ecomm Multi',
    'MP Developers s.r.o. Multi',
    'Prod Testing Multi'
  )

UNION ALL

SELECT 
    'SUMMARY_COMPARISON' AS analysis_type,
    'EDGE_CASE_PROCESSORS_TOTAL' AS metric,
    COUNT(*) AS snowflake_value
FROM IDENTIFIER($TARGET_TABLE)
WHERE DATE(transaction_date) = $VALIDATION_DATE
  AND CONCAT(processor_name, '|', currency_code, '|', bin_country) IN (
    'Nuvei Acquirer - Visa|GBP|tj',
    'Nuvei Acquirer SG USD - Visa|KRW|pr',
    'Nuvei Acquirer - Visa|GBP|am',
    'Nuvei Acquirer - MasterCard|ZAR|au',
    'WorldPay Direct|DKK|it',
    'AIBMS-Direct Payment|USD|ch',
    'Nuvei Acquirer - MasterCard|BRL|ch',
    'Nuvei NA Hub Citizens US PI|USD|zw',
    'Nuvei Acquirer - Visa|AED|cy',
    'Nuvei Acquirer - Visa|CZK|iq'
  );
