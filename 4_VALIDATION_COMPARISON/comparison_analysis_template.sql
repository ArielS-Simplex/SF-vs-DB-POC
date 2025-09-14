-- ==============================================================================
-- COMPARISON ANALYSIS TEMPLATE
-- Use this to compare results from Snowflake and Databricks for September 6, 2025
-- ==============================================================================

-- INSTRUCTIONS:
-- 1. Run snowflake_sep6_validation.sql in Snowflake
-- 2. Run databricks_sep6_validation.sql in Databricks  
-- 3. Copy results here and compare manually or use Excel/spreadsheet

-- EXPECTED COMPARISON POINTS:
-- ================================

-- 1. ROW COUNT COMPARISON
-- Snowflake: SNOWFLAKE_ROW_COUNT_SEP6 → total_rows should equal
-- Databricks: DATABRICKS_ROW_COUNT_SEP6 → total_rows
-- ✅ MATCH: Both should show identical row counts for Sep 6, 2025

-- 2. BUSINESS LOGIC COMPARISON  
-- Snowflake: SNOWFLAKE_BUSINESS_LOGIC_SEP6
-- Databricks: DATABRICKS_BUSINESS_LOGIC_SEP6
-- ✅ CRITICAL MATCH: 
--    - approved_count should be identical
--    - declined_count should be identical
--    - approval_rate_pct should be identical
--    - successful_auth_count should be identical

-- 3. TRANSACTION TYPE COMPARISON
-- Snowflake: SNOWFLAKE_TRANSACTION_TYPES_SEP6  
-- Databricks: DATABRICKS_TRANSACTION_TYPES_SEP6
-- ✅ MATCH: Each transaction_type should have identical:
--    - total_count
--    - success_count  
--    - decline_count
--    - success_rate_pct

-- 4. CLIENT BREAKDOWN COMPARISON
-- Snowflake: SNOWFLAKE_CLIENT_BREAKDOWN_SEP6
-- Databricks: DATABRICKS_CLIENT_BREAKDOWN_SEP6
-- ✅ MATCH: Top 10 clients should show identical transaction volumes and approval rates

-- 5. AUTHENTICATION FLOW COMPARISON
-- Snowflake: SNOWFLAKE_AUTH_FLOW_SEP6
-- Databricks: DATABRICKS_AUTH_FLOW_SEP6  
-- ✅ MATCH: Each authentication_flow should show identical success rates

-- 6. CURRENCY/AMOUNT COMPARISON
-- Snowflake: SNOWFLAKE_CURRENCY_AMOUNTS_SEP6
-- Databricks: DATABRICKS_CURRENCY_AMOUNTS_SEP6
-- ✅ MATCH: Total amounts in USD should be identical (within rounding)

-- 7. DATA QUALITY COMPARISON  
-- Snowflake: SNOWFLAKE_DATA_QUALITY_SEP6
-- Databricks: DATABRICKS_DATA_QUALITY_SEP6
-- ✅ MATCH: Should show 100% VALID/VALID_DATE for both platforms

-- VALIDATION CHECKLIST:
-- =====================
-- □ Row counts match exactly
-- □ Business logic metrics identical (approved, declined, auth success)
-- □ Transaction type breakdowns identical
-- □ Client-level metrics identical  
-- □ Authentication flow success rates identical
-- □ Currency totals match (within rounding tolerance)
-- □ Data quality flags identical
-- □ No unexpected differences in any metric

-- PERFORMANCE COMPARISON:
-- ======================
-- □ Record Snowflake query execution times
-- □ Record Databricks query execution times  
-- □ Compare ETL processing times for Sep 6 data
-- □ Document any performance differences

-- TROUBLESHOOTING IF DIFFERENCES FOUND:
-- ===================================
-- 1. Check for timezone differences in date filtering
-- 2. Verify both platforms processed exact same source data
-- 3. Check for any platform-specific data type conversion differences
-- 4. Verify business logic conditions are case-sensitive matched
-- 5. Check for any null/empty value handling differences
