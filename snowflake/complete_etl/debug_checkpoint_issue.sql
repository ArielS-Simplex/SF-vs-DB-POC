-- DEBUG: Why is the table empty?
-- Check 1: What's the current checkpoint time?
SELECT 'Current Checkpoint' AS check_type,
       checkpoint_time,
       last_run_status,
       records_processed
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';


CHECK_TYPE	CHECKPOINT_TIME	LAST_RUN_STATUS	RECORDS_PROCESSED
Current Checkpoint	2025-09-14 22:31:17.015	SUCCESS	0
-- Check 2: What are the inserted_at values in bronze?
SELECT 'Bronze inserted_at Range' AS check_type,
       MIN(inserted_at) AS min_inserted_at,
       MAX(inserted_at) AS max_inserted_at,
       COUNT(*) AS total_records,
       COUNT(CASE WHEN inserted_at IS NULL THEN 1 END) AS null_inserted_at
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05';


CHECK_TYPE	MIN_INSERTED_AT	MAX_INSERTED_AT	TOTAL_RECORDS	NULL_INSERTED_AT
Bronze inserted_at Range	2025-09-10 08:19:10.159	2025-09-10 08:19:10.159	13162623	0
-- Check 3: How many records would be processed with current checkpoint?
SELECT 'Records After Checkpoint' AS check_type,
       checkpoint_time,
       COUNT(*) AS records_to_process
FROM POC.PUBLIC.etl_metadata m
CROSS JOIN POC.PUBLIC.NCP_BRONZE_V2 b
WHERE m.table_name = 'NCP_SILVER_V4'
  AND b.inserted_at > m.checkpoint_time
  AND DATE(b.transaction_date) = '2025-09-05';
no results
-- Check 4: What if we ignore the checkpoint filter?
SELECT 'Records Without Checkpoint Filter' AS check_type,
       COUNT(*) AS total_records_available
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) = '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );
  CHECK_TYPE	TOTAL_RECORDS_AVAILABLE
Records Without Checkpoint Filter	13105852