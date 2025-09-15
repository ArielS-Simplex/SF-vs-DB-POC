-- Aggregated warehouse costs for X_SMALL_2_GEN (last 12 queries)
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
    LIMIT 1 OFFSET 11
  )
GROUP BY warehouse_name;