-- Warehouse compute costs using WAREHOUSE_METERING_HISTORY (last 2 hours)
SELECT 
    warehouse_name,
    SUM(credits_used) AS total_compute_credits,
    TRIM(TO_CHAR(SUM(credits_used) * 3.00, '999999990.000000')) AS total_cost_usd,
    COUNT(*) AS billing_periods,
    MIN(start_time) AS period_start,
    MAX(end_time) AS period_end
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'X_SMALL_2_GEN'
  AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;