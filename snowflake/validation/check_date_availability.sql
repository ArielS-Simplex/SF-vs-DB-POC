-- QUICK DATE AVAILABILITY CHECK
-- Run this FIRST to see what dates are actually available

-- Check available dates in Bronze
SELECT 
    'Available in Bronze' AS check_type,
    DATE(transaction_date) AS date,
    COUNT(*) AS raw_count,
    COUNT(CASE WHEN LOWER(TRIM(multi_client_name)) NOT IN ('test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi') THEN 1 END) AS filtered_count
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) BETWEEN '2025-09-01' AND '2025-09-10'  -- Wider range to see what's available
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
GROUP BY DATE(transaction_date)
ORDER BY date;

-- Check if we have the target 7-day range
SELECT 
    'Target Date Range Check' AS check_type,
    CASE 
        WHEN COUNT(DISTINCT DATE(transaction_date)) = 7 THEN 'ALL 7 DAYS AVAILABLE'
        ELSE CONCAT('ONLY ', COUNT(DISTINCT DATE(transaction_date)), ' DAYS AVAILABLE')
    END AS availability_status,
    MIN(DATE(transaction_date)) AS first_date,
    MAX(DATE(transaction_date)) AS last_date,
    COUNT(*) AS total_filtered_records
FROM POC.PUBLIC.NCP_BRONZE_V2
WHERE DATE(transaction_date) BETWEEN '2025-09-02' AND '2025-09-08'
  AND transaction_main_id IS NOT NULL 
  AND transaction_date IS NOT NULL
  AND LOWER(TRIM(multi_client_name)) NOT IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );
