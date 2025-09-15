-- SNOWFLAKE ETL COST MEASUREMENT FRAMEWORK
-- Professional cost tracking for POC comparison vs Databricks
-- Measures complete pipeline: S3 -> Staging -> Bronze -> Silver

-- ========================================
-- PART 1: QUERY HISTORY COST ANALYSIS
-- ========================================

-- Get detailed cost breakdown for last 24 hours of ETL operations
SELECT 
    'ETL Cost Breakdown' AS analysis_type,
    query_type,
    warehouse_name,
    COUNT(*) AS query_count,
    SUM(execution_time) / 1000 AS total_execution_seconds,
    SUM(queued_overload_time) / 1000 AS total_queue_seconds,
    SUM(credits_used_cloud_services) AS cloud_services_credits,
    SUM(total_elapsed_time) / 1000 AS total_elapsed_seconds,
    ROUND(AVG(credits_used_cloud_services), 4) AS avg_credits_per_query,
    MAX(total_elapsed_time) / 1000 AS max_execution_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND (
    query_text ILIKE '%ncp_bronze%' OR
    query_text ILIKE '%ncp_silver%' OR  
    query_text ILIKE '%staging%' OR
    query_text ILIKE '%@NCP/bpa.STP_BusinessAnalyticsQuery%' OR
    query_text ILIKE '%enhanced_working_etl%'
  )
  AND execution_status = 'SUCCESS'
GROUP BY query_type, warehouse_name
ORDER BY SUM(credits_used_cloud_services) DESC;

-- ========================================
-- PART 2: WAREHOUSE USAGE COST ANALYSIS  
-- ========================================

-- Warehouse credits used for ETL operations
SELECT 
    'Warehouse Usage Costs' AS analysis_type,
    warehouse_name,
    SUM(credits_used) AS total_credits_used,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_services_credits,
    ROUND(AVG(credits_used), 4) AS avg_credits_per_hour,
    COUNT(*) AS billing_periods,
    MIN(start_time) AS analysis_start,
    MAX(end_time) AS analysis_end
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND warehouse_name IN (
    SELECT DISTINCT warehouse_name 
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
      AND (query_text ILIKE '%ncp_bronze%' OR query_text ILIKE '%ncp_silver%')
  )
GROUP BY warehouse_name
ORDER BY total_credits_used DESC;

-- ========================================
-- PART 3: STORAGE COST ANALYSIS
-- ========================================

-- Storage costs for tables created
SELECT 
    'Storage Usage Costs' AS analysis_type,
    table_schema,
    table_name,
    active_bytes / POWER(1024, 3) AS active_gb,
    time_travel_bytes / POWER(1024, 3) AS time_travel_gb,
    failsafe_bytes / POWER(1024, 3) AS failsafe_gb,
    (active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024, 3) AS total_storage_gb,
    -- Estimated storage cost (adjust rate based on your region)
    ROUND((active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024, 3) * 0.023, 4) AS estimated_monthly_storage_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_name IN ('NCP_BRONZE_V2', 'NCP_SILVER_V2', 'NCP_BRONZE_STAGING_V2')
  AND table_schema = 'PUBLIC'
ORDER BY total_storage_gb DESC;

-- ========================================
-- PART 4: DATA TRANSFER COSTS (S3 to Snowflake)
-- ========================================

-- COPY command costs for S3 data loading
SELECT 
    'Data Transfer Costs' AS analysis_type,
    COUNT(*) AS copy_operations,
    SUM(bytes_scanned) / POWER(1024, 3) AS total_data_scanned_gb,
    SUM(bytes_loaded) / POWER(1024, 3) AS total_data_loaded_gb,
    SUM(credits_used_cloud_services) AS copy_cloud_services_credits,
    AVG(execution_time) / 1000 AS avg_copy_time_seconds,
    MAX(execution_time) / 1000 AS max_copy_time_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND table_name IN ('NCP_BRONZE_STAGING_V2')
  AND status = 'Loaded';

-- ========================================
-- PART 5: COMPLETE ETL PIPELINE COST SUMMARY
-- ========================================

-- Total ETL cost summary for Sept 5 POC
WITH etl_costs AS (
  -- Compute costs
  SELECT 'Compute' AS cost_category, SUM(credits_used) AS credits
  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND warehouse_name IN (
      SELECT DISTINCT warehouse_name 
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        AND (query_text ILIKE '%ncp_bronze%' OR query_text ILIKE '%ncp_silver%')
    )
  
  UNION ALL
  
  -- Cloud services costs  
  SELECT 'Cloud Services' AS cost_category, SUM(credits_used_cloud_services) AS credits
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND (query_text ILIKE '%ncp_bronze%' OR query_text ILIKE '%ncp_silver%')
    AND execution_status = 'SUCCESS'
)
SELECT 
    'ETL Pipeline Total Cost' AS summary_type,
    cost_category,
    credits,
    -- Convert to USD (adjust rate based on your Snowflake edition/region)
    ROUND(credits * 2.00, 2) AS estimated_cost_usd_standard,  -- Standard edition rate
    ROUND(credits * 3.00, 2) AS estimated_cost_usd_enterprise, -- Enterprise edition rate  
    ROUND(credits * 4.00, 2) AS estimated_cost_usd_business_critical -- Business Critical rate
FROM etl_costs
ORDER BY credits DESC;
