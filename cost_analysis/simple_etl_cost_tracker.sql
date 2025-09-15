-- SIMPLE ETL COST TRACKER
-- Run this BEFORE and AFTER your ETL pipeline to measure costs

-- ========================================
-- STEP 1: BASELINE MEASUREMENT (Run BEFORE ETL)
-- ========================================

-- Record current credit usage baseline
CREATE OR REPLACE TEMPORARY TABLE etl_cost_baseline AS
SELECT 
    CURRENT_TIMESTAMP() AS measurement_time,
    'PRE_ETL_BASELINE' AS measurement_type,
    warehouse_name,
    SUM(credits_used) AS cumulative_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;

SELECT 'BASELINE RECORDED - Ready to run ETL pipeline' AS status;

-- ========================================
-- STEP 2: POST-ETL COST MEASUREMENT (Run AFTER ETL)
-- ========================================

/*
-- After running your complete ETL pipeline, run this:

-- Simple cost summary for last 2 hours of ETL operations
SELECT 
    'SNOWFLAKE ETL COST SUMMARY' AS cost_summary,
    COUNT(*) AS total_queries,
    ROUND(SUM(credits_used_cloud_services), 4) AS total_credits_used,
    ROUND(SUM(execution_time) / 1000 / 60, 2) AS total_execution_minutes,
    -- Cost estimates (adjust rate based on your edition)
    ROUND(SUM(credits_used_cloud_services) * 2.00, 2) AS cost_standard_edition_usd,
    ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS cost_enterprise_edition_usd,
    ROUND(SUM(credits_used_cloud_services) * 4.00, 2) AS cost_business_critical_usd,
    -- Efficiency metrics
    (SELECT COUNT(*) FROM POC.PUBLIC.NCP_SILVER_V2 WHERE DATE(transaction_date) = '2025-09-05') AS records_processed,
    ROUND(SUM(credits_used_cloud_services) / (SELECT COUNT(*) FROM POC.PUBLIC.NCP_SILVER_V2 WHERE DATE(transaction_date) = '2025-09-05') * 1000000, 6) AS credits_per_million_records
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND (
    query_text ILIKE '%ncp_bronze%' OR
    query_text ILIKE '%ncp_silver%' OR  
    query_text ILIKE '%staging%' OR
    query_text ILIKE '%COPY INTO%'
  )
  AND execution_status = 'SUCCESS';
  


COST_SUMMARY	TOTAL_QUERIES	TOTAL_CREDITS_USED	TOTAL_EXECUTION_MINUTES	COST_STANDARD_EDITION_USD	COST_ENTERPRISE_EDITION_USD	COST_BUSINESS_CRITICAL_USD	RECORDS_PROCESSED	CREDITS_PER_MILLION_RECORDS
SNOWFLAKE ETL COST SUMMARY	28	0.0017	3.24	0	0.01	0.01	12686818	0.000138

-- Detailed breakdown by pipeline stage
SELECT 
    'ETL STAGE BREAKDOWN' AS breakdown_type,
    CASE 
        WHEN query_text ILIKE '%COPY INTO%' AND query_text ILIKE '%staging%' THEN '1_S3_to_Staging'
        WHEN query_text ILIKE '%CREATE TABLE%' AND query_text ILIKE '%bronze%' THEN '2_Staging_to_Bronze'
        WHEN query_text ILIKE '%CREATE TABLE%' AND query_text ILIKE '%silver%' THEN '3_Bronze_to_Silver'
        ELSE '4_Other_Operations'
    END AS pipeline_stage,
    COUNT(*) AS queries,
    ROUND(SUM(credits_used_cloud_services), 4) AS credits,
    ROUND(SUM(credits_used_cloud_services) * 3.00, 2) AS estimated_cost_usd,
    ROUND(SUM(execution_time) / 1000 / 60, 2) AS execution_minutes
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND (
    query_text ILIKE '%ncp_bronze%' OR
    query_text ILIKE '%ncp_silver%' OR  
    query_text ILIKE '%staging%' OR
    query_text ILIKE '%COPY INTO%'
  )
  AND execution_status = 'SUCCESS'
GROUP BY pipeline_stage
ORDER BY pipeline_stage;
*/

BREAKDOWN_TYPE	PIPELINE_STAGE	QUERIES	CREDITS	ESTIMATED_COST_USD	EXECUTION_MINUTES
ETL STAGE BREAKDOWN	1_S3_to_Staging	2	0.001	0	1.23
ETL STAGE BREAKDOWN	2_Staging_to_Bronze	2	0.0002	0	1.94
ETL STAGE BREAKDOWN	4_Other_Operations	24	0.0005	0	0.06