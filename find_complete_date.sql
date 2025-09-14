-- ==============================================================================
-- FIND COMPLETE DATE FOR TESTING
-- Finding a date with complete 24-hour data coverage
-- ==============================================================================

SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE';

-- ==============================================================================
-- 1. AVAILABLE DATES IN BRONZE DATA
-- ==============================================================================

SELECT 
    'AVAILABLE_DATES' AS analysis_type,
    DATE(transaction_date) AS available_date,
    COUNT(*) AS total_transactions,
    MIN(transaction_date) AS earliest_time,
    MAX(transaction_date) AS latest_time,
    COUNT(DISTINCT HOUR(transaction_date)) AS unique_hours,
    MAX(HOUR(transaction_date)) AS latest_hour,
    MAX(MINUTE(transaction_date)) AS latest_minute
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) >= '2025-09-01'
  AND DATE(transaction_date) <= '2025-09-09'
GROUP BY DATE(transaction_date)
ORDER BY available_date DESC;

-- ==============================================================================
-- 2. FIND DATES WITH COMPLETE 24-HOUR COVERAGE
-- ==============================================================================

-- Check which dates have all 24 hours and reach minute 59 in hour 23
SELECT 
    'COMPLETE_COVERAGE_CHECK' AS analysis_type,
    DATE(transaction_date) AS check_date,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT HOUR(transaction_date)) AS unique_hours,
    MAX(HOUR(transaction_date)) AS latest_hour,
    MAX(CASE WHEN HOUR(transaction_date) = 23 THEN MINUTE(transaction_date) END) AS latest_minute_hour23,
    CASE 
        WHEN COUNT(DISTINCT HOUR(transaction_date)) = 24 
         AND MAX(HOUR(transaction_date)) = 23 
         AND MAX(CASE WHEN HOUR(transaction_date) = 23 THEN MINUTE(transaction_date) END) >= 55
        THEN 'LIKELY_COMPLETE'
        ELSE 'INCOMPLETE'
    END AS completeness_status
FROM IDENTIFIER($SOURCE_TABLE)
WHERE DATE(transaction_date) >= '2025-09-01'
  AND DATE(transaction_date) <= '2025-09-09'
GROUP BY DATE(transaction_date)
HAVING COUNT(*) > 1000000  -- Only consider dates with substantial data
ORDER BY check_date DESC;

-- ==============================================================================
-- 3. DETAILED ANALYSIS OF MOST RECENT COMPLETE DATE
-- ==============================================================================

-- Get the most recent date that appears to have complete coverage
WITH complete_dates AS (
    SELECT 
        DATE(transaction_date) AS check_date,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT HOUR(transaction_date)) AS unique_hours,
        MAX(CASE WHEN HOUR(transaction_date) = 23 THEN MINUTE(transaction_date) END) AS latest_minute_hour23
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) >= '2025-09-01'
      AND DATE(transaction_date) <= '2025-09-09'
    GROUP BY DATE(transaction_date)
    HAVING COUNT(DISTINCT HOUR(transaction_date)) = 24 
       AND MAX(HOUR(transaction_date)) = 23 
       AND MAX(CASE WHEN HOUR(transaction_date) = 23 THEN MINUTE(transaction_date) END) >= 55
       AND COUNT(*) > 1000000
    ORDER BY check_date DESC
    LIMIT 1
)

SELECT 
    'RECOMMENDED_TEST_DATE' AS analysis_type,
    complete_dates.check_date AS recommended_date,
    complete_dates.total_transactions,
    complete_dates.unique_hours,
    complete_dates.latest_minute_hour23,
    'Use this date for complete POC testing' AS recommendation
FROM complete_dates;

-- ==============================================================================
-- 4. HOUR 23 MINUTE DISTRIBUTION FOR RECOMMENDED DATE
-- ==============================================================================

-- Check minute distribution in hour 23 for the recommended date
WITH complete_dates AS (
    SELECT 
        DATE(transaction_date) AS check_date
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) >= '2025-09-01'
      AND DATE(transaction_date) <= '2025-09-09'
    GROUP BY DATE(transaction_date)
    HAVING COUNT(DISTINCT HOUR(transaction_date)) = 24 
       AND MAX(HOUR(transaction_date)) = 23 
       AND MAX(CASE WHEN HOUR(transaction_date) = 23 THEN MINUTE(transaction_date) END) >= 55
       AND COUNT(*) > 1000000
    ORDER BY check_date DESC
    LIMIT 1
)

SELECT 
    'HOUR_23_MINUTES_CHECK' AS analysis_type,
    complete_dates.check_date,
    MINUTE(transaction_date) AS minute_of_hour,
    COUNT(*) AS transaction_count
FROM IDENTIFIER($SOURCE_TABLE) bronze
JOIN complete_dates ON DATE(bronze.transaction_date) = complete_dates.check_date
WHERE HOUR(bronze.transaction_date) = 23
GROUP BY complete_dates.check_date, MINUTE(transaction_date)
ORDER BY minute_of_hour;
