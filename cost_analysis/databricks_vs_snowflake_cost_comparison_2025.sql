-- ==============================================================================
-- DATABRICKS SERVERLESS vs SNOWFLAKE XS WAREHOUSE COST COMPARISON 2025
-- Azure Enterprise pricing for bronze and silver ETL processing
-- Processing 12.6M records daily with runtime-based cost calculations
-- ==============================================================================

-- ==============================================================================
-- STEP 1: MEASURE CURRENT SNOWFLAKE XS WAREHOUSE RUNTIME & CREDITS
-- ==============================================================================

-- First, let's get actual runtime data from the enhanced working ETL
WITH snowflake_etl_performance AS (
    SELECT 
        'Snowflake XS Warehouse Performance' AS platform,
        
        -- Silver ETL metrics (from query history)
        SUM(CASE WHEN query_text ILIKE '%enhanced_working_etl%' OR query_text ILIKE '%NCP_SILVER%' 
                 THEN execution_time / 1000.0 / 3600 ELSE 0 END) AS silver_runtime_hours,
        SUM(CASE WHEN query_text ILIKE '%enhanced_working_etl%' OR query_text ILIKE '%NCP_SILVER%' 
                 THEN credits_used_cloud_services + COALESCE(credits_used_compute, 0) ELSE 0 END) AS silver_credits,
        
        -- Bronze ETL metrics (from copy history and staging)
        SUM(CASE WHEN query_text ILIKE '%bronze%' OR query_text ILIKE '%copy%' OR query_text ILIKE '%staging%'
                 THEN execution_time / 1000.0 / 3600 ELSE 0 END) AS bronze_runtime_hours,
        SUM(CASE WHEN query_text ILIKE '%bronze%' OR query_text ILIKE '%copy%' OR query_text ILIKE '%staging%'
                 THEN credits_used_cloud_services + COALESCE(credits_used_compute, 0) ELSE 0 END) AS bronze_credits,
        
        -- Total metrics
        12686818 AS rows_processed,
        COUNT(*) AS total_queries
        
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
      AND execution_status = 'SUCCESS'
      AND (
        query_text ILIKE '%ncp_bronze%' OR 
        query_text ILIKE '%ncp_silver%' OR
        query_text ILIKE '%enhanced_working_etl%'
      )
)
SELECT * FROM snowflake_etl_performance;

-- ==============================================================================
-- STEP 2: AZURE PRICING MODELS COMPARISON TABLE
-- ==============================================================================

WITH pricing_models AS (
    SELECT 
        -- Databricks Serverless Pricing (Azure 2025)
        'Databricks Serverless' AS platform,
        'Bronze ETL' AS etl_type,
        0.70 AS cost_per_unit,  -- $0.70 per DBU (US regions)
        'DBU' AS unit_type,
        'Per-second billing, no minimum cluster' AS billing_model,
        
        -- Estimated DBU consumption for bronze loading (conservative estimate)
        2.0 AS estimated_units_bronze, -- 2 DBUs for data loading/preprocessing
        4.0 AS estimated_units_silver  -- 4 DBUs for complex transformations
        
    UNION ALL
    
    SELECT 
        'Snowflake XS Warehouse' AS platform,
        'Both ETLs' AS etl_type,
        3.00 AS cost_per_unit,  -- $3.00 per credit (Enterprise Edition, mid-range)
        'Credit' AS unit_type,
        'Per-second billing, 60-second minimum' AS billing_model,
        
        -- Actual credits from query history (will be replaced with real data)
        0.1 AS estimated_units_bronze,  -- Actual bronze credits
        0.5 AS estimated_units_silver   -- Actual silver credits
)
SELECT * FROM pricing_models;

-- ==============================================================================
-- STEP 3: COMPREHENSIVE COST COMPARISON TABLE
-- ==============================================================================

WITH cost_comparison AS (
    -- Databricks Serverless Costs
    SELECT 
        'Databricks' AS platform,
        'Serverless' AS compute_type,
        'Bronze' AS etl_stage,
        
        -- Runtime assumptions (based on typical bronze processing)
        0.25 AS estimated_runtime_hours,  -- 15 minutes for data loading
        2.0 AS compute_units,             -- 2 DBUs estimated
        0.70 AS unit_cost_usd,            -- $0.70 per DBU
        
        -- Daily cost calculation
        2.0 * 0.70 AS daily_cost_usd,
        
        -- Row processing efficiency
        12686818 / (2.0 * 0.70) AS rows_per_dollar
    
    UNION ALL
    
    SELECT 
        'Databricks' AS platform,
        'Serverless' AS compute_type,
        'Silver' AS etl_stage,
        
        -- Runtime assumptions (based on complex transformation)
        1.0 AS estimated_runtime_hours,   -- 1 hour for complex business logic
        4.0 AS compute_units,             -- 4 DBUs for complex transformations
        0.70 AS unit_cost_usd,            -- $0.70 per DBU
        
        -- Daily cost calculation
        4.0 * 0.70 AS daily_cost_usd,
        
        -- Row processing efficiency
        12686818 / (4.0 * 0.70) AS rows_per_dollar
    
    UNION ALL
    
    -- Snowflake XS Warehouse Costs (using actual performance data)
    SELECT 
        'Snowflake' AS platform,
        'X Small' AS compute_type,
        'Bronze' AS etl_stage,
        
        -- Actual performance data (to be filled from query history)
        COALESCE((SELECT bronze_runtime_hours FROM snowflake_etl_performance), 0.1) AS estimated_runtime_hours,
        COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) AS compute_units,
        3.00 AS unit_cost_usd,  -- $3.00 per credit (Enterprise Edition)
        
        -- Daily cost calculation
        COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) * 3.00 AS daily_cost_usd,
        
        -- Row processing efficiency
        12686818 / (COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) * 3.00) AS rows_per_dollar
    
    UNION ALL
    
    SELECT 
        'Snowflake' AS platform,
        'X Small' AS compute_type,
        'Silver' AS etl_stage,
        
        -- Actual performance data
        COALESCE((SELECT silver_runtime_hours FROM snowflake_etl_performance), 0.5) AS estimated_runtime_hours,
        COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) AS compute_units,
        3.00 AS unit_cost_usd,  -- $3.00 per credit (Enterprise Edition)
        
        -- Daily cost calculation  
        COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) * 3.00 AS daily_cost_usd,
        
        -- Row processing efficiency
        12686818 / (COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) * 3.00) AS rows_per_dollar
)
SELECT 
    platform,
    compute_type,
    etl_stage,
    estimated_runtime_hours,
    compute_units,
    unit_cost_usd,
    ROUND(daily_cost_usd, 2) AS daily_cost_usd,
    ROUND(rows_per_dollar, 0) AS rows_per_dollar,
    ROUND(daily_cost_usd * 30, 2) AS monthly_cost_usd,
    ROUND(daily_cost_usd * 365, 2) AS annual_cost_usd
FROM cost_comparison
ORDER BY platform, etl_stage;

-- ==============================================================================
-- STEP 4: PLATFORM TOTALS AND COST DIFFERENCES
-- ==============================================================================

WITH platform_totals AS (
    SELECT 
        platform,
        SUM(daily_cost_usd) AS total_daily_cost,
        SUM(daily_cost_usd * 30) AS total_monthly_cost,
        SUM(daily_cost_usd * 365) AS total_annual_cost,
        SUM(compute_units) AS total_compute_units,
        AVG(rows_per_dollar) AS avg_efficiency_rows_per_dollar
    FROM (
        -- Databricks totals
        SELECT 'Databricks' AS platform, (2.0 + 4.0) * 0.70 AS daily_cost_usd, 
               (2.0 + 4.0) AS compute_units, 12686818 / ((2.0 + 4.0) * 0.70) AS rows_per_dollar
        UNION ALL
        -- Snowflake totals (using actual data)
        SELECT 'Snowflake' AS platform, 
               (COALESCE((SELECT bronze_credits + silver_credits FROM snowflake_etl_performance), 0.6)) * 3.00 AS daily_cost_usd,
               (COALESCE((SELECT bronze_credits + silver_credits FROM snowflake_etl_performance), 0.6)) AS compute_units,
               12686818 / ((COALESCE((SELECT bronze_credits + silver_credits FROM snowflake_etl_performance), 0.6)) * 3.00) AS rows_per_dollar
    ) t
    GROUP BY platform
),
cost_difference AS (
    SELECT 
        d.total_daily_cost AS databricks_daily_cost,
        s.total_daily_cost AS snowflake_daily_cost,
        (d.total_daily_cost - s.total_daily_cost) AS daily_cost_difference,
        (d.total_monthly_cost - s.total_monthly_cost) AS monthly_cost_difference,
        (d.total_annual_cost - s.total_annual_cost) AS annual_cost_difference,
        
        -- Percentage difference
        ROUND(((d.total_daily_cost - s.total_daily_cost) / s.total_daily_cost * 100), 1) AS percent_difference,
        
        -- Cost per million rows
        ROUND(d.total_daily_cost / 12.686818, 2) AS databricks_cost_per_million_rows,
        ROUND(s.total_daily_cost / 12.686818, 2) AS snowflake_cost_per_million_rows
        
    FROM platform_totals d, platform_totals s
    WHERE d.platform = 'Databricks' AND s.platform = 'Snowflake'
)
SELECT 
    'Cost Comparison Summary - Databricks vs Snowflake (2025)' AS analysis_type,
    databricks_daily_cost,
    snowflake_daily_cost, 
    daily_cost_difference,
    monthly_cost_difference,
    annual_cost_difference,
    percent_difference AS percent_difference_databricks_vs_snowflake,
    databricks_cost_per_million_rows,
    snowflake_cost_per_million_rows,
    
    -- Winner determination
    CASE 
        WHEN daily_cost_difference > 0 THEN 'Snowflake is cheaper'
        WHEN daily_cost_difference < 0 THEN 'Databricks is cheaper' 
        ELSE 'Costs are equal'
    END AS cost_winner
FROM cost_difference;

-- ==============================================================================
-- STEP 5: FINAL COMPARISON TABLE (MATCHING YOUR TEMPLATE)
-- ==============================================================================

SELECT 
    'FINAL COST COMPARISON TABLE' AS table_type,
    platform,
    etl_stage,
    CASE 
        WHEN platform = 'Databricks' AND etl_stage = 'Bronze' THEN '15 min'
        WHEN platform = 'Databricks' AND etl_stage = 'Silver' THEN '60 min'
        WHEN platform = 'Snowflake' AND etl_stage = 'Bronze' THEN CONCAT(ROUND(estimated_runtime_hours * 60, 0), ' min')
        WHEN platform = 'Snowflake' AND etl_stage = 'Silver' THEN CONCAT(ROUND(estimated_runtime_hours * 60, 0), ' min')
    END AS runtime,
    
    CONCAT('$', ROUND(daily_cost_usd, 2)) AS daily_row_cost,
    
    -- Additional metrics for decision making
    CONCAT(ROUND(compute_units, 2), ' ', 
           CASE WHEN platform = 'Databricks' THEN 'DBUs' ELSE 'Credits' END) AS compute_consumption,
    CONCAT(ROUND(rows_per_dollar / 1000, 0), 'K') AS rows_per_dollar_k
    
FROM (
    -- Include the cost_comparison CTE results
    SELECT 
        'Databricks' AS platform, 'Bronze' AS etl_stage, 0.25 AS estimated_runtime_hours,
        2.0 AS compute_units, 2.0 * 0.70 AS daily_cost_usd, 12686818 / (2.0 * 0.70) AS rows_per_dollar
    UNION ALL
    SELECT 
        'Databricks' AS platform, 'Silver' AS etl_stage, 1.0 AS estimated_runtime_hours,
        4.0 AS compute_units, 4.0 * 0.70 AS daily_cost_usd, 12686818 / (4.0 * 0.70) AS rows_per_dollar
    UNION ALL
    SELECT 
        'Snowflake' AS platform, 'Bronze' AS etl_stage,
        COALESCE((SELECT bronze_runtime_hours FROM snowflake_etl_performance), 0.1) AS estimated_runtime_hours,
        COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) AS compute_units,
        COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) * 3.00 AS daily_cost_usd,
        12686818 / (COALESCE((SELECT bronze_credits FROM snowflake_etl_performance), 0.1) * 3.00) AS rows_per_dollar
    UNION ALL
    SELECT 
        'Snowflake' AS platform, 'Silver' AS etl_stage,
        COALESCE((SELECT silver_runtime_hours FROM snowflake_etl_performance), 0.5) AS estimated_runtime_hours,
        COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) AS compute_units,
        COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) * 3.00 AS daily_cost_usd,
        12686818 / (COALESCE((SELECT silver_credits FROM snowflake_etl_performance), 0.5) * 3.00) AS rows_per_dollar
) comparison_data
ORDER BY 
    CASE WHEN platform = 'Databricks' THEN 1 ELSE 2 END,
    CASE WHEN etl_stage = 'Bronze' THEN 1 ELSE 2 END;

-- ==============================================================================
-- STEP 6: RECOMMENDATIONS AND NOTES
-- ==============================================================================

SELECT 
    'COST ANALYSIS RECOMMENDATIONS' AS section,
    'Based on 12.6M daily records processing' AS scope,
    'Databricks: $0.70/DBU (Serverless) + Azure infrastructure' AS databricks_pricing,
    'Snowflake: $3.00/credit (Enterprise XS) - per-second billing' AS snowflake_pricing,
    'Consider usage patterns: Databricks better for ML/complex data eng, Snowflake better for analytics' AS recommendation,
    'Actual costs may vary based on optimization, caching, and specific workload characteristics' AS disclaimer;