-- ==============================================================================
-- ACTUAL STAGING BASELINE - SEPTEMBER 10, 2025
-- 71.8M records loaded with X_SMALL_2_GEN warehouse
-- ==============================================================================

-- ACTUAL STAGING OPERATION BASELINE (X_SMALL_2_GEN)
-- Date: September 10, 2025 08:17:38 -0700
-- Operation: 7-day staging data load (Sept 2-8, 2025)
-- Records: 71,876,386 (5.5x larger than original baseline)
/*
METRIC_TYPE: RECENT_WAREHOUSE_USAGE
WAREHOUSE_NAME: X_SMALL_2_GEN
CREDITS_USED_LAST_HOUR: 0.0002
ESTIMATED_COST_USD: $0.00
QUERIES_EXECUTED: 1
LAST_ACTIVITY: 2025-09-10 09:00:00.000 -0700
*/

-- ACTUAL STAGING QUERY BREAKDOWN
/*
QUERY_TYPE                  QUERY_COUNT    TOTAL_CREDITS    AVG_DURATION_SECONDS
SELECT                      4              0.0001           0.18
CREATE_TABLE_AS_SELECT      3              0                0.08
UNKNOWN                     2              0                0.03
SET                         4              0                0.05
USE                         1              0                0.05
*/

-- ACTUAL STAGING STORAGE COSTS
/*
TABLE_NAME                  STORAGE_GB    ROW_COUNT     MONTHLY_STORAGE_COST_USD
NCP_BRONZE_STAGING_V2      12.11         84,061,124    $0.2786
NCP_BRONZE_STAGING_V2      10.36         71,876,386    $0.2382  <-- Current dataset
NCP_BRONZE_STAGING         5.25          36,349,536    $0.1207
NCP_BRONZE_STAGING_V2      1.92          13,162,152    $0.0442
*/

-- ACTUAL STAGING SUMMARY
/*
Total Credits Used (Last Hour): 0.0002
Estimated Cost: $0.00
Dataset Size: 71,876,386 records (10.36 GB)
Monthly Storage Cost: $0.24 for current dataset

PERFORMANCE NOTE: Extremely efficient - Gen2 warehouse processed 71.8M records 
for virtually no compute cost (0.0002 credits)
*/

-- ==============================================================================
-- COMPARISON TEMPLATE - Run after ETL completion
-- ==============================================================================

-- Run this query after ETL to compare costs:
/*
SELECT 
    'COST_COMPARISON' AS analysis_type,
    'Baseline: 0.0346 credits ($0.07)' AS before_etl,
    'After ETL: [INSERT NEW VALUES]' AS after_etl,
    'Difference: [CALCULATE]' AS cost_difference,
    'Performance: [ASSESS]' AS performance_notes;
*/
