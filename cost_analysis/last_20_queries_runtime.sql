-- Runtime and cost for last 20 queries
SELECT 
    query_id,
    warehouse_name,
    query_text,
    execution_time / 1000 AS runtime_seconds,
    ROUND(execution_time / 1000 / 60.0, 2) AS runtime_minutes,
    CASE WHEN credits_used_cloud_services < 0.000001 THEN '0.000000' 
         ELSE TRIM(TO_CHAR(credits_used_cloud_services, '999999990.000000')) END AS credits_used,
    CASE WHEN credits_used_cloud_services < 0.000001 THEN '0.000000' 
         ELSE TRIM(TO_CHAR(credits_used_cloud_services * 3.00, '999999990.000000')) END AS cost_usd,
    start_time,
    execution_status
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
ORDER BY start_time DESC
LIMIT 20;



-- Aggregated warehouse costs for X_SMALL_2_GEN (last 20 queries)
SELECT 
    warehouse_name,
    COUNT(*) AS total_queries,
    SUM(execution_time) / 1000 AS total_runtime_seconds,
    ROUND(SUM(execution_time) / 1000 / 60.0, 2) AS total_runtime_minutes,
    ROUND(SUM(execution_time) / 1000 / 3600.0, 4) AS total_runtime_hours,
    TRIM(TO_CHAR(SUM(credits_used_cloud_services), '999999990.000000')) AS total_credits_used,
    TRIM(TO_CHAR(SUM(credits_used_cloud_services) * 3.00, '999999990.000000')) AS total_cost_usd,
    ROUND(AVG(execution_time) / 1000, 2) AS avg_query_seconds,
    MIN(start_time) AS earliest_query,
    MAX(start_time) AS latest_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
  AND start_time >= (
    SELECT start_time 
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
    WHERE warehouse_name = 'X_SMALL_2_GEN'
    ORDER BY start_time DESC 
    LIMIT 1 OFFSET 19
  )
GROUP BY warehouse_name;