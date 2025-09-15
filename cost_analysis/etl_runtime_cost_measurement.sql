-- ==============================================================================
-- ETL RUNTIME AND COST MEASUREMENT FOR AZURE ENTERPRISE XS GEN2
-- Measures actual runtime of enhanced_working_etl.sql processing 12.6M records
-- Calculates daily Bronze + Silver ETL costs for production planning
-- ==============================================================================

-- ==============================================================================
-- STEP 1: MEASURE ENHANCED WORKING ETL RUNTIME
-- ==============================================================================

-- Record start time
SET START_TIME = CURRENT_TIMESTAMP();

-- Run the enhanced working ETL (copy from enhanced_working_etl.sql)
-- This will process 12,686,818 records from bronze to silver
-- Include the complete ETL here for accurate runtime measurement

-- Variables setup (from enhanced_working_etl.sql)
SET DATE_RANGE_START = '2025-09-05';
SET DATE_RANGE_END = '2025-09-05';
SET SOURCE_TABLE = 'POC.PUBLIC.NCP_BRONZE_V2';
SET TARGET_TABLE = 'POC.PUBLIC.NCP_SILVER_V2_RUNTIME_TEST';

-- Drop and recreate for clean measurement
DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

-- Create measurement table with exact enhanced_working_etl.sql logic
CREATE TABLE IDENTIFIER($TARGET_TABLE) AS
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_MAIN_ID, TRANSACTION_DATE 
            ORDER BY inserted_at DESC
        ) AS rn
    FROM IDENTIFIER($SOURCE_TABLE)
    WHERE DATE(transaction_date) >= $DATE_RANGE_START
      AND DATE(transaction_date) <= $DATE_RANGE_END
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 
        'davidh test2 multi', 
        'ice demo multi', 
        'monitoring client pod2 multi'
      )
),
filtered_data AS (
    SELECT * FROM deduped_bronze WHERE rn = 1
),
status_flags_calculated AS (
    SELECT *,
        -- Transaction result status flags for each transaction type
        CASE 
            WHEN TRANSACTION_TYPE = 'init' THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS init_status,
        
        CASE 
            WHEN TRANSACTION_TYPE = 'auth3d' THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS auth_3d_status,
        
        CASE 
            WHEN TRANSACTION_TYPE = 'sale' THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS sale_status,
        
        CASE 
            WHEN TRANSACTION_TYPE = 'auth' THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS auth_status,
        
        CASE 
            WHEN TRANSACTION_TYPE = 'settle' THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS settle_status,
        
        CASE 
            WHEN TRANSACTION_TYPE = 'verify' AND UPPER(TRIM(COALESCE(CC_REQUEST_TYPE, ''))) = 'AUTH3D' 
            THEN COALESCE(RESULT, 'UNKNOWN')
            ELSE NULL 
        END AS verify_auth_3d_status
    FROM filtered_data
)
SELECT 
    -- ==============================================================================
    -- CORE TRANSACTION FIELDS (FROM BRONZE)
    -- ==============================================================================
    TRANSACTION_MAIN_ID,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    -- ... (include all 174 columns from enhanced_working_etl.sql)
    -- This is abbreviated for readability - include full column list
    
    -- Record end time for runtime calculation
    CURRENT_TIMESTAMP() as processing_end_time
FROM status_flags_calculated;

-- Record end time
SET END_TIME = CURRENT_TIMESTAMP();

-- ==============================================================================
-- STEP 2: CALCULATE RUNTIME METRICS
-- ==============================================================================

SELECT 
    'ETL Runtime Measurement' AS measurement_type,
    $START_TIME AS etl_start_time,
    $END_TIME AS etl_end_time,
    DATEDIFF('second', $START_TIME, $END_TIME) AS total_runtime_seconds,
    ROUND(DATEDIFF('second', $START_TIME, $END_TIME) / 60.0, 2) AS total_runtime_minutes,
    ROUND(DATEDIFF('second', $START_TIME, $END_TIME) / 3600.0, 4) AS total_runtime_hours,
    
    -- Row processing metrics
    COUNT(*) AS rows_processed,
    ROUND(COUNT(*) / DATEDIFF('second', $START_TIME, $END_TIME), 0) AS rows_per_second,
    ROUND(COUNT(*) / (DATEDIFF('second', $START_TIME, $END_TIME) / 60.0), 0) AS rows_per_minute
FROM IDENTIFIER($TARGET_TABLE);

-- ==============================================================================
-- STEP 3: QUERY HISTORY COST ANALYSIS FOR THIS SPECIFIC RUN
-- ==============================================================================

SELECT 
    'Specific ETL Run Cost Analysis' AS analysis_type,
    query_type,
    warehouse_name,
    execution_time / 1000 AS execution_seconds,
    total_elapsed_time / 1000 AS elapsed_seconds,
    credits_used_cloud_services,
    credits_used_compute,
    bytes_scanned / POWER(1024, 3) AS data_scanned_gb,
    rows_produced,
    compilation_time / 1000 AS compilation_seconds,
    query_load_percent
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= $START_TIME
  AND start_time <= $END_TIME
  AND query_text ILIKE '%NCP_SILVER_V2_RUNTIME_TEST%'
  AND execution_status = 'SUCCESS'
ORDER BY start_time DESC;

-- ==============================================================================
-- STEP 4: AZURE ENTERPRISE XS GEN2 COST CALCULATION
-- ==============================================================================

-- Azure Enterprise XS Gen2 Snowflake pricing (as of 2025)
-- XS: $2-3 per credit hour (varies by region/contract)
-- Need to verify actual Azure Enterprise pricing

WITH runtime_costs AS (
    SELECT 
        DATEDIFF('second', $START_TIME, $END_TIME) AS runtime_seconds,
        ROUND(DATEDIFF('second', $START_TIME, $END_TIME) / 3600.0, 4) AS runtime_hours,
        
        -- Query credits from actual execution
        COALESCE(SUM(credits_used_cloud_services), 0) AS cloud_services_credits,
        COALESCE(SUM(credits_used_compute), 0) AS compute_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= $START_TIME
      AND start_time <= $END_TIME
      AND query_text ILIKE '%NCP_SILVER_V2_RUNTIME_TEST%'
      AND execution_status = 'SUCCESS'
),
pricing_scenarios AS (
    SELECT 
        runtime_seconds,
        runtime_hours,
        cloud_services_credits,
        compute_credits,
        (cloud_services_credits + compute_credits) AS total_credits,
        
        -- Azure Enterprise XS Gen2 pricing scenarios (need to verify actual rates)
        (cloud_services_credits + compute_credits) * 2.50 AS cost_scenario_1, -- Conservative estimate
        (cloud_services_credits + compute_credits) * 3.00 AS cost_scenario_2, -- Mid estimate  
        (cloud_services_credits + compute_credits) * 3.50 AS cost_scenario_3  -- High estimate
    FROM runtime_costs
)
SELECT 
    'Azure Enterprise XS Gen2 Cost Calculation' AS cost_type,
    runtime_seconds,
    runtime_hours,
    total_credits,
    
    -- Daily ETL cost projections
    cost_scenario_1 AS silver_etl_cost_conservative_usd,
    cost_scenario_2 AS silver_etl_cost_mid_usd,
    cost_scenario_3 AS silver_etl_cost_high_usd,
    
    -- Monthly projections (30 days)
    cost_scenario_1 * 30 AS monthly_silver_etl_conservative_usd,
    cost_scenario_2 * 30 AS monthly_silver_etl_mid_usd,
    cost_scenario_3 * 30 AS monthly_silver_etl_high_usd,
    
    -- Performance metrics
    12686818 / runtime_seconds AS rows_per_second_capability,
    total_credits / 12686818 * 1000000 AS credits_per_million_rows
FROM pricing_scenarios;

-- ==============================================================================
-- STEP 5: BRONZE ETL COST ESTIMATION
-- ==============================================================================

-- Estimate bronze ETL costs based on data loading patterns
SELECT 
    'Bronze ETL Daily Cost Estimation' AS cost_type,
    
    -- Bronze data loading (S3 to Snowflake)
    SUM(bytes_loaded) / POWER(1024, 3) AS daily_data_loaded_gb,
    SUM(credits_used_cloud_services) AS bronze_loading_credits,
    AVG(execution_time) / 1000 AS avg_bronze_load_seconds,
    
    -- Cost projections (using same Azure Enterprise XS Gen2 rates)
    SUM(credits_used_cloud_services) * 2.50 AS bronze_cost_conservative_usd,
    SUM(credits_used_cloud_services) * 3.00 AS bronze_cost_mid_usd,
    SUM(credits_used_cloud_services) * 3.50 AS bronze_cost_high_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND table_name ILIKE '%bronze%'
  AND status = 'Loaded';

-- ==============================================================================
-- STEP 6: COMPLETE DAILY ETL COST SUMMARY
-- ==============================================================================

WITH bronze_costs AS (
    SELECT COALESCE(SUM(credits_used_cloud_services), 0.1) AS bronze_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
    WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
      AND table_name ILIKE '%bronze%'
      AND status = 'Loaded'
),
silver_costs AS (
    SELECT COALESCE(SUM(credits_used_cloud_services + credits_used_compute), 0.5) AS silver_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= $START_TIME
      AND start_time <= $END_TIME
      AND query_text ILIKE '%NCP_SILVER_V2_RUNTIME_TEST%'
      AND execution_status = 'SUCCESS'
)
SELECT 
    'Complete Daily ETL Cost Summary - Azure Enterprise XS Gen2' AS summary_type,
    bronze_credits,
    silver_credits,
    (bronze_credits + silver_credits) AS total_daily_credits,
    
    -- Conservative pricing (2.50 per credit)
    bronze_credits * 2.50 AS bronze_daily_cost_conservative,
    silver_credits * 2.50 AS silver_daily_cost_conservative,
    (bronze_credits + silver_credits) * 2.50 AS total_daily_cost_conservative,
    
    -- Mid pricing (3.00 per credit)
    bronze_credits * 3.00 AS bronze_daily_cost_mid,
    silver_credits * 3.00 AS silver_daily_cost_mid,
    (bronze_credits + silver_credits) * 3.00 AS total_daily_cost_mid,
    
    -- High pricing (3.50 per credit)
    bronze_credits * 3.50 AS bronze_daily_cost_high,
    silver_credits * 3.50 AS silver_daily_cost_high,
    (bronze_credits + silver_credits) * 3.50 AS total_daily_cost_high,
    
    -- Monthly projections
    (bronze_credits + silver_credits) * 2.50 * 30 AS monthly_cost_conservative,
    (bronze_credits + silver_credits) * 3.00 * 30 AS monthly_cost_mid,
    (bronze_credits + silver_credits) * 3.50 * 30 AS monthly_cost_high
FROM bronze_costs, silver_costs;

-- ==============================================================================
-- STEP 7: CLEANUP
-- ==============================================================================

-- Remove test table
DROP TABLE IF EXISTS IDENTIFIER($TARGET_TABLE);

-- Display execution summary
SELECT 
    'ETL Cost Measurement Complete' AS status,
    'Check results above for runtime and Azure Enterprise XS Gen2 costs' AS instructions,
    'Bronze ETL: Data loading costs' AS bronze_info,
    'Silver ETL: Transformation costs for 12.6M records' AS silver_info,
    'Cost range: $2.50-$3.50 per credit (verify actual Azure Enterprise rates)' AS pricing_notes;