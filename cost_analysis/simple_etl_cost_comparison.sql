-- All queries on X_SMALL_2_GEN in last 10 minutes
SELECT 
    COUNT(*) AS total_queries,
    ROUND(SUM(execution_time) / 1000 / 60.0, 2) AS runtime_minutes,
    ROUND((SUM(execution_time) / 1000 / 3600.0) * 3.00, 4) AS cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
  AND start_time >= DATEADD('minute', -10, CURRENT_TIMESTAMP());