-- DEBUG SCRIPT - Find out why no data is being inserted
-- Run this to check each step of the filtering process

-- Step 1: Check total records in bronze table
SELECT 'BRONZE_TOTAL' as step, COUNT(*) as record_count
FROM POC.PUBLIC.NCP_BRONZE;

-- Step 2: Check date range in bronze
SELECT 
    'BRONZE_DATE_RANGE' as step,
    MIN(TRANSACTION_DATE) as min_date,
    MAX(TRANSACTION_DATE) as max_date,
    COUNT(*) as total_records
FROM POC.PUBLIC.NCP_BRONZE;

-- Step 3: Check how many records match the 30-day filter
SELECT 'DATE_FILTER_30_DAYS' as step, COUNT(*) as record_count
FROM POC.PUBLIC.NCP_BRONZE
WHERE TRANSACTION_DATE >= CURRENT_DATE() - INTERVAL '30 days';

-- Step 4: Check how many records match the 1-year filter
SELECT 'DATE_FILTER_1_YEAR' as step, COUNT(*) as record_count
FROM POC.PUBLIC.NCP_BRONZE
WHERE TRANSACTION_DATE >= CURRENT_DATE() - INTERVAL '1 year';

-- Step 5: Check records with valid transaction_main_id and transaction_date
SELECT 'VALID_RECORDS' as step, COUNT(*) as record_count
FROM POC.PUBLIC.NCP_BRONZE
WHERE transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL;

-- Step 6: Check what happens after test client filtering
SELECT 'AFTER_TEST_CLIENT_FILTER' as step, COUNT(*) as record_count
FROM POC.PUBLIC.NCP_BRONZE
WHERE transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND multi_client_name NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi');

-- Step 7: Sample data to see what we're working with
SELECT 
    'SAMPLE_DATA' as step,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    transaction_type
FROM POC.PUBLIC.NCP_BRONZE
LIMIT 5;
