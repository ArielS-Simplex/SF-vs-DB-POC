-- SNOWFLAKE vs DATABRICKS COST COMPARISON TEMPLATE
-- Professional cost analysis framework for POC

-- ========================================
-- SNOWFLAKE COST BREAKDOWN (Run after ETL)
-- ========================================

-- 1. PIPELINE STAGE COSTS
SELECT 'Pipeline Stage Costs' AS metric_type,
       stage_name,
       credits_used,
       estimated_cost_usd,
       execution_time_minutes,
       data_processed_gb
FROM (
  -- Stage 1: S3 to Staging (COPY commands)
  SELECT 'S3_to_Staging' AS stage_name,
         SUM(credits_used_cloud_services) AS credits_used,
         ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS estimated_cost_usd,
         SUM(execution_time) / 60000 AS execution_time_minutes,
         SUM(bytes_loaded) / POWER(1024, 3) AS data_processed_gb
  FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND table_name = 'NCP_BRONZE_STAGING_V2'
    
  UNION ALL
  
  -- Stage 2: Staging to Bronze (CREATE TABLE AS)
  SELECT 'Staging_to_Bronze' AS stage_name,
         SUM(credits_used_cloud_services) AS credits_used,
         ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS estimated_cost_usd,
         SUM(execution_time) / 60000 AS execution_time_minutes,
         SUM(bytes_scanned) / POWER(1024, 3) AS data_processed_gb
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND query_text ILIKE '%ncp_bronze_v2%'
    AND query_text ILIKE '%CREATE TABLE%'
    
  UNION ALL
  
  -- Stage 3: Bronze to Silver (Enhanced Working ETL)
  SELECT 'Bronze_to_Silver' AS stage_name,
         SUM(credits_used_cloud_services) AS credits_used,
         ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS estimated_cost_usd,
         SUM(execution_time) / 60000 AS execution_time_minutes,
         SUM(bytes_scanned) / POWER(1024, 3) AS data_processed_gb
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND query_text ILIKE '%ncp_silver_v2%'
    AND query_text ILIKE '%enhanced%'
);

-- ========================================
-- COST PER RECORD ANALYSIS
-- ========================================

-- Calculate cost efficiency metrics
WITH record_counts AS (
  SELECT 
    (SELECT COUNT(*) FROM POC.PUBLIC.NCP_BRONZE_V2 WHERE DATE(transaction_date) = '2025-09-05') AS bronze_records,
    (SELECT COUNT(*) FROM POC.PUBLIC.NCP_SILVER_V2 WHERE DATE(transaction_date) = '2025-09-05') AS silver_records
),
total_costs AS (
  SELECT 
    SUM(credits_used_cloud_services) AS total_credits,
    ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS total_cost_usd
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND (query_text ILIKE '%ncp_bronze%' OR query_text ILIKE '%ncp_silver%')
    AND execution_status = 'SUCCESS'
)
SELECT 
  'Cost Efficiency Metrics' AS metric_type,
  r.bronze_records,
  r.silver_records,
  c.total_credits,
  c.total_cost_usd,
  ROUND(c.total_cost_usd / r.silver_records * 1000000, 4) AS cost_per_million_records_usd,
  ROUND(c.total_credits / r.silver_records * 1000000, 4) AS credits_per_million_records
FROM record_counts r, total_costs c;

-- ========================================
-- DATABRICKS COMPARISON TEMPLATE
-- ========================================

/*
DATABRICKS COST CALCULATION (Run in Databricks):

%sql
-- Calculate Databricks ETL costs for same Sept 5 data
SELECT 
  'Databricks ETL Cost Analysis' AS analysis_type,
  -- DBU consumption for cluster runtime
  cluster_runtime_hours * cluster_dbu_rate AS compute_dbus,
  -- Storage costs  
  data_processed_gb * storage_cost_per_gb AS storage_costs,
  -- Data transfer costs
  data_ingested_gb * data_transfer_rate AS transfer_costs,
  -- Total cost calculation
  (compute_dbus * dbu_cost_usd) + storage_costs + transfer_costs AS total_estimated_cost_usd
FROM (
  SELECT 
    -- Adjust these values based on your actual Databricks setup
    2.5 AS cluster_runtime_hours,  -- Estimated runtime for Sept 5 ETL
    1.0 AS cluster_dbu_rate,       -- DBUs per hour for your cluster size
    0.10 AS dbu_cost_usd,          -- Cost per DBU in USD
    15.0 AS data_processed_gb,     -- GB processed (adjust based on actual)
    0.023 AS storage_cost_per_gb,  -- Delta Lake storage cost
    0.09 AS data_transfer_rate,    -- S3 data transfer cost
    12.0 AS data_ingested_gb       -- GB ingested from S3
);
*/

-- ========================================
-- COMPARATIVE COST SUMMARY
-- ========================================

SELECT 
  'SNOWFLAKE vs DATABRICKS COMPARISON' AS comparison_summary,
  'Run queries above to get exact costs' AS instructions,
  'Key metrics to compare:' AS note_1,
  '1. Total ETL cost per day' AS metric_1,
  '2. Cost per million records processed' AS metric_2, 
  '3. Storage cost per GB' AS metric_3,
  '4. Data ingestion cost per GB' AS metric_4,
  '5. Compute cost per hour' AS metric_5;
