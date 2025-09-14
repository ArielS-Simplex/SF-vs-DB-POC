-- ==============================================================================
-- HYPOTHESIS: 4,479 ROW DIFFERENCE ROOT CAUSE ANALYSIS
-- ==============================================================================

-- CURRENT STATE:
-- Snowflake: 10,584,798 rows
-- Databricks: 10,589,277 rows  
-- Difference: 4,479 rows (Databricks has MORE)

-- HYPOTHESIS 1: DEV/TEST CLIENT FILTERING DIFFERENCES
-- Total dev/test clients found in Snowflake: 784 transactions
-- If Databricks includes these but Snowflake filters them out, 
-- this could explain part of the difference

-- HYPOTHESIS 2: EDGE CASE PROCESSOR COMBINATIONS
-- Found 20 single-occurrence processor/currency/country combinations
-- These might be filtered differently or have different data quality rules

-- HYPOTHESIS 3: TIMESTAMP BOUNDARY HANDLING
-- Time boundaries show normal distribution, but millisecond precision
-- differences could cause records to fall on different sides of the 
-- day boundary between platforms

-- HYPOTHESIS 4: TRANSACTION RESULT ID MAPPING
-- Non-1006 result IDs (1000, 1004, 1008, 1011) represent edge cases
-- These might be mapped or filtered differently between platforms

-- ==============================================================================
-- INVESTIGATION PLAN
-- ==============================================================================

-- STEP 1: Run focused_difference_analysis.sql on Snowflake
-- STEP 2: Run the equivalent queries on Databricks  
-- STEP 3: Compare the specific counts for:
--   - Dev/test clients: Expected ~784 in Snowflake, might be 0 in Databricks
--   - Edge case processors: Expected ~20 in Snowflake, might be different in Databricks
--   - Result ID distributions: Compare exact percentages

-- STEP 4: Mathematical validation
-- If dev/test clients explain X rows and edge cases explain Y rows,
-- then X + Y should equal or be close to 4,479

-- ==============================================================================
-- EXPECTED FINDINGS
-- ==============================================================================

-- If hypothesis is correct, we should find:
-- 1. Databricks filters OUT the 784 dev/test client transactions
-- 2. Databricks handles edge case processors differently 
-- 3. Small timestamp boundary differences
-- 4. The sum of these differences = 4,479 rows

-- MATHEMATICAL CHECK:
-- 784 (dev/test clients) + ~3,695 (other factors) = 4,479 total difference

-- This would mean Databricks has MORE records because:
-- - It includes some edge cases that Snowflake filters out
-- - OR it has different time boundary handling
-- - OR it has different deduplication logic for edge cases

-- ==============================================================================
-- NEXT ACTIONS
-- ==============================================================================

-- 1. Execute focused_difference_analysis.sql
-- 2. Run equivalent queries on Databricks
-- 3. Compare specific counts and identify exact patterns
-- 4. Document the root cause
-- 5. Decide if 0.04% difference is acceptable for POC comparison
