-- ETL specific query costs
SELECT 
    'ETL Queries Cost Analysis' AS analysis_type,
    COUNT(*) AS etl_queries,
    SUM(execution_time) / 1000 AS total_runtime_seconds,
    ROUND(SUM(execution_time) / 1000 / 60.0, 2) AS total_runtime_minutes,
    TRIM(TO_CHAR(SUM(credits_used_cloud_services), '999999990.000000')) AS total_credits_used,
    TRIM(TO_CHAR(SUM(credits_used_cloud_services) * 3.00, '999999990.000000')) AS total_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
  AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND (
    query_text ILIKE '%ncp_bronze%' OR
    query_text ILIKE '%ncp_silver%' OR
    query_text ILIKE '%CREATE TABLE%parsed_data%' OR
    query_text ILIKE '%COPY INTO%'
  );