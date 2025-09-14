-- ==============================================================================
-- ETL DIFFERENCE TEST EXECUTION GUIDE
-- How to run and interpret the test results
-- ==============================================================================

/*
PURPOSE:
This test script identifies exactly which Databricks ETL transformations we're missing
in our Snowflake implementation that could cause the 4,479 row difference.

EXECUTION STEPS:
1. Run etl_difference_test.sql in Snowflake
2. Analyze each test result
3. Identify the specific transformations causing differences
4. Update our ETL with the missing logic

INTERPRETATION GUIDE:

TEST 1 - NULL_BYTE_TEST:
- If > 0: Databricks filters these records, we keep them
- Impact: Records we include that Databricks excludes

TEST 2 - YES_NO_BOOLEAN_TEST: 
- If > 0: Records with "yes"/"no" boolean values
- Impact: Different boolean normalization could affect filtering

TEST 3 - NUMERIC_STRING_TEST:
- Shows regex extraction differences
- Impact: Different string processing results

TEST 4 - DEPRECATED_VALUES_TEST:
- If > 0: Records with "deprecated" values
- Impact: Databricks nullifies these, we might process them

TEST 5 - FORCED_NULL_COLUMNS_TEST:
- Shows records where Databricks forces columns to NULL
- Impact: Different column values could affect downstream logic

TEST 6 - AUTH3D_TRANSACTIONS_TEST:
- Shows Auth3D transactions where Databricks creates extra columns
- Impact: Additional business logic that affects processing

TEST 7 - EMPTY_STRING_TEST:
- Records with empty strings vs NULL
- Impact: Different NULL handling between platforms

TEST 8 - BOOLEAN_FIELD_ANALYSIS:
- Detailed breakdown of boolean field values
- Impact: Identifies exact values that need different handling

TEST 9 - DIFFERENCE_SIMULATION:
- Simulates Databricks vs Snowflake processing
- Impact: Direct count of processing differences

TEST 10 - MATHEMATICAL_VALIDATION:
- Sums up all potential differences
- Impact: Should approximate the 4,479 row difference

EXPECTED RESULTS:
If our hypothesis is correct, the sum of differences should explain
most or all of the 4,479 row discrepancy.

NEXT STEPS BASED ON RESULTS:
- If null byte handling explains majority → Add null byte filtering
- If boolean normalization explains majority → Fix boolean logic  
- If string processing explains majority → Update regex patterns
- If multiple factors → Update ETL with all missing transformations

RUN THIS COMMAND:
Execute etl_difference_test.sql and analyze each test result.
*/

-- Quick summary query to run first
SELECT 
    'QUICK_SUMMARY' AS summary_type,
    'Run this to get overview before detailed tests' AS description,
    COUNT(*) AS total_records_sep_6
FROM POC.PUBLIC.NCP_BRONZE 
WHERE DATE(transaction_date) = '2025-09-06';
