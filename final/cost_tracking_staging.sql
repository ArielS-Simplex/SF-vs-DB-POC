-- ==============================================================================
-- COST TRACKING FOR STAGING OPERATION
-- Run this AFTER completing the staging data load
-- ==============================================================================

-- 1. WAREHOUSE USAGE FOR LAST HOUR
SELECT 
    'RECENT_WAREHOUSE_USAGE' AS metric_type,
    WAREHOUSE_NAME,
    ROUND(SUM(CREDITS_USED), 4) AS credits_used_last_hour,
    ROUND(SUM(CREDITS_USED) * 2.00, 2) AS estimated_cost_usd,  -- Adjust $2/credit for your pricing
    COUNT(*) AS queries_executed,
    MAX(END_TIME) AS last_activity
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
WHERE START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP)
  AND WAREHOUSE_NAME = CURRENT_WAREHOUSE()
GROUP BY WAREHOUSE_NAME;

-- 2. RECENT QUERY COSTS
SELECT 
    'STAGING_QUERIES' AS metric_type,
    QUERY_TYPE,
    COUNT(*) AS query_count,
    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) AS total_credits,
    ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_duration_seconds,
    MAX(END_TIME) AS last_query_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP)
  AND USER_NAME = CURRENT_USER()
  AND WAREHOUSE_NAME = CURRENT_WAREHOUSE()
GROUP BY QUERY_TYPE
ORDER BY total_credits DESC;

-- 3. TABLE STORAGE COSTS
SELECT 
    'STORAGE_USAGE' AS metric_type,
    TABLE_NAME,
    ROUND(BYTES / (1024*1024*1024), 2) AS storage_gb,
    ROW_COUNT,
    ROUND((BYTES / (1024*1024*1024)) * 0.023, 4) AS monthly_storage_cost_usd  -- $23/TB/month
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
  AND TABLE_CATALOG = 'POC'
  AND TABLE_NAME LIKE '%STAGING%'
ORDER BY storage_gb DESC;

-- 4. COST SUMMARY
SELECT 
    'COST_SUMMARY' AS metric_type,
    CURRENT_WAREHOUSE() AS warehouse_used,
    'Staging operation completed - 71,876,386 records loaded' AS operation,
    'Dataset is 5.5x larger than baseline (13M)' AS note,
    CURRENT_TIMESTAMP AS report_time;
