SELECT 
    query_tag,
    warehouse_name,
    ROUND(SUM(execution_time/1000.0), 2) AS total_runtime_seconds,
    ROUND(SUM(execution_time/1000.0/60.0), 2) AS total_runtime_minutes,
    CASE WHEN SUM(credits_used_cloud_services) < 0.000001 THEN '0.000000'
         ELSE TRIM(TO_CHAR(SUM(credits_used_cloud_services), '999999990.000000')) END AS total_credits,
    CASE WHEN SUM(credits_used_cloud_services) < 0.000001 THEN '0.000000'
         ELSE TRIM(TO_CHAR(SUM(credits_used_cloud_services) * 3.00, '999999990.000000')) END AS total_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
  AND query_tag LIKE '%POC_ETL_%'
    and query_tag != 'POC_ETL_15_V2_2025-09-16_03:36:04'
GROUP BY query_tag, warehouse_name
ORDER BY query_tag;