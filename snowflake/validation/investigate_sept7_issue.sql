-- SEPTEMBER 7TH INVESTIGATION - Why is Snowflake missing 10.3M records?

-- Step 1: Check if Sept 7 data exists in Bronze at all
SELECT 
    'Sept 7 Bronze Availability' AS check_type,
    COUNT(*) AS total_records,
    MIN(transaction_date) AS earliest,
    MAX(transaction_date) AS latest,
    COUNT(DISTINCT HOUR(transaction_date)) AS hours_covered
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-07';




CHECK_TYPE	TOTAL_RECORDS	EARLIEST	LATEST	HOURS_COVERED
Sept 7 Bronze Availability	273566	2025-09-07 00:00:00.000	2025-09-07 23:59:59.993	2
-- Step 2: Check filtering impact on Sept 7
SELECT 
    'Sept 7 Filtering Impact' AS check_type,
    COUNT(*) AS total_bronze,
    COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) AS null_tx_id,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_date,
    COUNT(CASE WHEN LOWER(TRIM(multi_client_name)) IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS test_clients,
    COUNT(*) - COUNT(CASE WHEN transaction_main_id IS NULL THEN 1 END) - COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) - COUNT(CASE WHEN LOWER(TRIM(multi_client_name)) IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS should_process
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-07';

CHECK_TYPE	TOTAL_BRONZE	NULL_TX_ID	NULL_DATE	TEST_CLIENTS	SHOULD_PROCESS
Sept 7 Filtering Impact	273566	0	0	1166	272400

-- Step 3: Compare what actually made it to Silver
SELECT 
    'Sept 7 ETL Result' AS check_type,
    COUNT(*) AS silver_records,
    MIN(transaction_date) AS earliest,
    MAX(transaction_date) AS latest
FROM POC.PUBLIC.NCP_SILVER_V2_7_DAYS
WHERE DATE(transaction_date) = '2025-09-07';


CHECK_TYPE	SILVER_RECORDS	EARLIEST	LATEST
Sept 7 ETL Result	268144	2025-09-07 00:00:00.000	2025-09-07 23:59:59.993
-- Step 4: Check for boundary date issues (maybe data is in Sept 6 or 8?)
SELECT 
    'Date Boundary Check' AS check_type,
    DATE(transaction_date) AS date,
    COUNT(*) AS count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) BETWEEN '2025-09-06' AND '2025-09-08'
GROUP BY DATE(transaction_date)
ORDER BY date;
CHECK_TYPE	DATE	COUNT
Date Boundary Check	2025-09-06	10984929
Date Boundary Check	2025-09-07	273566
Date Boundary Check	2025-09-08	12028699