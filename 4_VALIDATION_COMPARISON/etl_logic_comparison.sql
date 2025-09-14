-- ==============================================================================
-- ETL LOGIC COMPARISON: DATABRICKS vs SNOWFLAKE
-- Key differences that could explain the 4,479 row difference
-- ==============================================================================

/*
MAJOR FINDING: DATABRICKS APPLIES ADDITIONAL COLUMN TRANSFORMATIONS THAT WE'RE MISSING!

After comparing the Databricks custom_etl_functions.ipynb with our Snowflake ETL,
I found several critical differences:

1. DATABRICKS CREATES ADDITIONAL COLUMNS that we don't have in Snowflake:
   - is_sale_3d_auth_3d (conditional copy)
   - manage_3d_decision_auth_3d (conditional copy)
   - Additional status flags with different logic

2. DATABRICKS BOOLEAN NORMALIZATION is more complex:
   - We handle basic true/false/1/0 
   - Databricks handles: "1", "1.0", "true", "yes" vs "0", "0.0", "false", "no"
   - We might be filtering out records that Databricks keeps

3. DATABRICKS STRING PROCESSING:
   - Converts numeric strings to just the number part
   - More aggressive NULL handling for deprecated/empty values
   - We might be processing these differently

4. DATABRICKS DTYPE FIXING:
   - Forces NULL for specific columns: user_agent_3d, authentication_request, 
     authentication_response, authorization_req_duration
   - We handle these but might have different logic

5. DATABRICKS HAS ADDITIONAL COLUMN CREATION LOGIC:
   - Creates new columns like is_sale_3d_auth_3d only when transaction_type = 'auth3d'
   - Creates conditional status flags that might affect row counts

HYPOTHESIS: The 4,479 difference could be due to:
- Records that pass Databricks boolean/string normalization but fail ours
- Records where our status flag logic differs from Databricks
- Records with edge case data that Databricks handles but we filter out
*/

-- ==============================================================================
-- SPECIFIC DIFFERENCES TO INVESTIGATE:
-- ==============================================================================

-- 1. ADDITIONAL COLUMNS DATABRICKS CREATES:
/*
Databricks creates these columns that we don't have:
- is_sale_3d_auth_3d (only for auth3d transactions)
- manage_3d_decision_auth_3d (only for auth3d transactions)

This could affect filtering or business logic downstream.
*/

-- 2. DIFFERENT BOOLEAN HANDLING:
/*
Databricks allows: ["true", "1", "yes", "1.0"] = true, ["false", "0", "no", "0.0"] = false
We only handle:   ["true", "1", "yes", "1.0"] = true, ["false", "0", "no", "0.0"] = false

But our regex and case handling might be different.
*/

-- 3. STRING NORMALIZATION DIFFERENCES:
/*
Databricks: when(expr.rlike(r"^\d+\.?\d*$"), regexp_extract(expr, r"(\d+)", 1)).otherwise(expr)
Us: CASE WHEN REGEXP_LIKE(status, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(status, '\\d+', 1, 1) ELSE TRIM(LOWER(status)) END

The regex patterns might behave differently between Spark and Snowflake.
*/

-- 4. NULL VALUE HANDLING:
/*
Databricks filters: ["<na>", "na", "nan", "none", "", " ", "\x00"]
We filter: ['<na>', 'na', 'nan', 'none', '', ' ', 'deprecated']

We don't handle "\x00" (null byte) which could be in the data.
*/

-- 5. FORCED NULL COLUMNS:
/*
Databricks forces these to NULL: user_agent_3d, authentication_request, authentication_response, authorization_req_duration
We handle them normally - this could cause different row counts if there's filtering based on these.
*/

-- ==============================================================================
-- INVESTIGATION QUERIES:
-- ==============================================================================

-- Check for records with null byte characters (that Databricks filters but we don't)
SELECT COUNT(*) as null_byte_records
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = '2025-09-06'
  AND (status LIKE '%\x00%' OR acs_url LIKE '%\x00%' OR user_agent_3d LIKE '%\x00%');

-- Check for records where our boolean logic might differ
SELECT COUNT(*) as potential_boolean_issues
FROM POC.PUBLIC.NCP_BRONZE  
WHERE DATE(transaction_date) = '2025-09-06'
  AND (is_void IN ('yes', 'no') OR liability_shift IN ('yes', 'no') OR is_sale_3d IN ('yes', 'no'));

-- Check for Auth3D transactions (where Databricks creates additional columns)
SELECT COUNT(*) as auth3d_transactions
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = '2025-09-06'
  AND transaction_type = 'Auth3D';

-- Check for numeric strings in status field
SELECT status, COUNT(*) as count
FROM POC.PUBLIC.NCP_BRONZE
WHERE DATE(transaction_date) = '2025-09-06'
  AND REGEXP_LIKE(status, '^\\d+\\.?\\d*$')
GROUP BY status
ORDER BY count DESC;
