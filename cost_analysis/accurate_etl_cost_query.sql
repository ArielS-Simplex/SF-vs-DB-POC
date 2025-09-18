-- COMPLETE ACCURATE ETL COST ANALYSIS FOR MANAGER PRESENTATION
-- This query combines actual compute credits + cloud services credits for precise costing

WITH etl_queries AS (
    -- Get all ETL query details
    SELECT 
        query_tag,
        warehouse_name,
        start_time,
        end_time,
        execution_time,
        credits_used_cloud_services,
        ROW_NUMBER() OVER (PARTITION BY query_tag ORDER BY start_time) as query_order
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name = 'X_SMALL_2_GEN'
      AND query_tag LIKE 'POC_ETL_%'
      AND execution_status = 'SUCCESS'
),

etl_time_windows AS (
    -- Get start and end times for each ETL run
    SELECT 
        query_tag,
        warehouse_name,
        MIN(start_time) as etl_start_time,
        MAX(end_time) as etl_end_time,
        COUNT(*) as total_queries,
        ROUND(SUM(execution_time)/1000.0/60.0, 2) AS total_runtime_minutes,
        ROUND(SUM(credits_used_cloud_services), 6) AS cloud_services_credits
    FROM etl_queries
    GROUP BY query_tag, warehouse_name
),

warehouse_compute_credits AS (
    -- Get actual compute credits used during each ETL window
    SELECT 
        etw.query_tag,
        etw.warehouse_name,
        etw.etl_start_time,
        etw.etl_end_time,
        etw.total_queries,
        etw.total_runtime_minutes,
        etw.cloud_services_credits,
        
        -- Get warehouse compute credits for the ETL time window
        COALESCE(
            (SELECT SUM(wm.credits_used) 
             FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wm
             WHERE wm.warehouse_name = etw.warehouse_name
               AND wm.start_time >= etw.etl_start_time 
               AND wm.end_time <= etw.etl_end_time), 
            -- Fallback: estimate based on runtime (1 credit = 1 hour X-Small)
            ROUND(etw.total_runtime_minutes / 60.0, 6)
        ) AS compute_credits,
        
        -- Calculate proportional credits if needed
        CASE 
            WHEN etw.total_runtime_minutes > 0 THEN
                ROUND(etw.total_runtime_minutes / 60.0, 6)
            ELSE 0
        END AS estimated_compute_credits
        
    FROM etl_time_windows etw
)

-- FINAL RESULTS: Complete cost breakdown
SELECT 
    query_tag,
    warehouse_name,
    total_queries,
    total_runtime_minutes,
    
    -- CREDIT BREAKDOWN
    cloud_services_credits,
    compute_credits AS actual_compute_credits,
    estimated_compute_credits,
    
    -- Use actual if available, otherwise use estimate
    CASE 
        WHEN compute_credits > 0 THEN compute_credits
        ELSE estimated_compute_credits
    END AS final_compute_credits,
    
    -- TOTAL CREDITS
    cloud_services_credits + 
    CASE 
        WHEN compute_credits > 0 THEN compute_credits
        ELSE estimated_compute_credits
    END AS total_credits,
    
    -- COST BREAKDOWN (Enterprise $3/credit)
    ROUND(cloud_services_credits * 3.00, 4) AS cloud_services_cost_usd,
    ROUND(
        CASE 
            WHEN compute_credits > 0 THEN compute_credits
            ELSE estimated_compute_credits
        END * 3.00, 4
    ) AS compute_cost_usd,
    
    -- TOTAL COST
    ROUND((
        cloud_services_credits + 
        CASE 
            WHEN compute_credits > 0 THEN compute_credits
            ELSE estimated_compute_credits
        END
    ) * 3.00, 4) AS total_cost_usd,
    
    -- EFFICIENCY METRICS
    ROUND((
        cloud_services_credits + 
        CASE 
            WHEN compute_credits > 0 THEN compute_credits
            ELSE estimated_compute_credits
        END
    ) / 12.686818, 6) AS credits_per_million_records,
    
    ROUND((
        cloud_services_credits + 
        CASE 
            WHEN compute_credits > 0 THEN compute_credits
            ELSE estimated_compute_credits
        END
    ) * 3.00 / 12.686818, 6) AS cost_per_million_records_usd,
    
    -- DATA SOURCE INDICATOR
    CASE 
        WHEN compute_credits > 0 THEN 'ACTUAL_WAREHOUSE_METERING'
        ELSE 'ESTIMATED_FROM_RUNTIME'
    END AS credit_calculation_method

FROM warehouse_compute_credits
ORDER BY query_tag;

-- SUMMARY FOR MANAGER
SELECT 
    'SUMMARY FOR MANAGER PRESENTATION' as analysis_type,
    COUNT(*) as total_etl_runs,
    ROUND(AVG(total_runtime_minutes), 2) as avg_runtime_minutes,
    ROUND(AVG(total_credits), 6) as avg_credits_per_run,
    ROUND(AVG(total_cost_usd), 4) as avg_cost_per_run_usd,
    ROUND(AVG(total_cost_usd) * 30, 2) as monthly_cost_daily_runs_usd,
    ROUND(AVG(total_cost_usd) * 365, 2) as annual_cost_daily_runs_usd,
    ROUND(AVG(credits_per_million_records), 6) as avg_credits_per_million_records
FROM warehouse_compute_credits;