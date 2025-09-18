SELECT 
    query_tag,
    warehouse_name,
    ROUND(SUM(execution_time/1000.0/60.0), 2) AS total_runtime_minutes,
    ROUND(SUM(execution_time/1000.0/60.0)/60, 4) AS estimated_credits,
    ROUND(SUM(execution_time/1000.0/60.0)/60 * 3.00, 4) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
GROUP BY query_tag, warehouse_name
ORDER BY query_tag;