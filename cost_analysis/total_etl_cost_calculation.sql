-- Complete ETL cost calculation combining compute + cloud services
WITH etl_query_time AS (
    -- Get ETL query execution time
    SELECT 
        SUM(execution_time) / 1000 AS etl_runtime_seconds,
        SUM(credits_used_cloud_services) AS etl_cloud_services_credits,
        COUNT(*) AS etl_queries
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name = 'X_SMALL_2_GEN'
      AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
      AND (
        query_text ILIKE '%ncp_bronze%' OR
        query_text ILIKE '%ncp_silver%' OR
        query_text ILIKE '%CREATE TABLE%parsed_data%' OR
        query_text ILIKE '%COPY INTO%'
      )
),
warehouse_total AS (
    -- Get total warehouse compute credits
    SELECT 
        SUM(credits_used) AS total_compute_credits,
        SUM(credits_used) * 3.00 AS total_compute_cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name = 'X_SMALL_2_GEN'
      AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
),
total_query_time AS (
    -- Get total query time for proportion calculation
    SELECT SUM(execution_time) / 1000 AS total_runtime_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name = 'X_SMALL_2_GEN'
      AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
)
SELECT 
    'Complete ETL Cost Analysis' AS analysis_type,
    e.etl_queries,
    e.etl_runtime_seconds,
    ROUND(e.etl_runtime_seconds / 60.0, 2) AS etl_runtime_minutes,
    
    -- Cloud services cost (metadata)
    TRIM(TO_CHAR(e.etl_cloud_services_credits * 3.00, '999999990.000000')) AS cloud_services_cost_usd,
    
    -- Compute cost (proportional to runtime)
    ROUND((e.etl_runtime_seconds / t.total_runtime_seconds) * w.total_compute_credits, 6) AS estimated_compute_credits,
    TRIM(TO_CHAR((e.etl_runtime_seconds / t.total_runtime_seconds) * w.total_compute_cost, '999999990.000000')) AS compute_cost_usd,
    
    -- Total ETL cost
    TRIM(TO_CHAR(
        (e.etl_cloud_services_credits * 3.00) + 
        ((e.etl_runtime_seconds / t.total_runtime_seconds) * w.total_compute_cost), 
        '999999990.000000'
    )) AS total_etl_cost_usd,
    
    -- Context
    TRIM(TO_CHAR(w.total_compute_cost, '999999990.000000')) AS total_warehouse_cost_usd,
    ROUND((e.etl_runtime_seconds / t.total_runtime_seconds) * 100, 1) AS etl_percentage_of_warehouse_time
    
FROM etl_query_time e, warehouse_total w, total_query_time t;