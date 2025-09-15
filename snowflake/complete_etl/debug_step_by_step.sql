-- STEP BY STEP DEBUG - Why is table still empty?

-- Step 1: Check current checkpoint after reset
SELECT 'Step 1: Current Checkpoint' AS step,
       checkpoint_time,
       last_run_status,
       records_processed
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';

STEP	CHECKPOINT_TIME	LAST_RUN_STATUS	RECORDS_PROCESSED
Step 1: Current Checkpoint	2025-09-09 00:00:00.000	SUCCESS	0

-- Step 2: Test the exact filter being used
SELECT 'Step 2: Test Filter Logic' AS step,
       COUNT(*) AS records_matching_filter
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE inserted_at > '2025-09-09 00:00:00'::TIMESTAMP
  AND DATE(transaction_date) >= '2025-09-05'
  AND DATE(transaction_date) <= '2025-09-05'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );


STEP	RECORDS_MATCHING_FILTER
Step 2: Test Filter Logic	13105852

-- Step 3: Check if target table exists and what's in it
SELECT 'Step 3: Target Table Status' AS step,
       COUNT(*) AS record_count
FROM POC.PUBLIC.NCP_SILVER_V4;

STEP	RECORD_COUNT
Step 3: Target Table Status	0

-- Step 4: Check if there are any errors in the ETL variables
-- Simulate the variable setting
SET checkpoint_time_test = (
    SELECT checkpoint_time 
    FROM POC.PUBLIC.etl_metadata 
    WHERE table_name = 'NCP_SILVER_V4'
);

SELECT 'Step 4: Variable Check' AS step,
       $checkpoint_time_test AS checkpoint_variable;


status
Statement executed successfully.

STEP	CHECKPOINT_VARIABLE
Step 4: Variable Check	2025-09-09 00:00:00.000

-- Step 5: Test the deduplication step
WITH deduped_bronze AS (
    SELECT 
        transaction_main_id,
        transaction_date,
        inserted_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM POC.PUBLIC.NCP_BRONZE_V2
    WHERE inserted_at > '2025-09-09 00:00:00'::TIMESTAMP
      AND DATE(transaction_date) >= '2025-09-05'
      AND DATE(transaction_date) <= '2025-09-05'
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
SELECT 'Step 5: After Deduplication' AS step,
       COUNT(*) AS deduplicated_count
FROM filtered_data;


STEP	DEDUPLICATED_COUNT
Step 5: After Deduplication	12686818