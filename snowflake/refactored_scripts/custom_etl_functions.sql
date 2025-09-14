-- ================================================
-- Snowflake Custom ETL Functions
-- Equivalent to: custom_etl_functions.ipynb
-- ================================================

-- ================================================
-- CONSTANTS AND CONFIGURATION
-- ================================================

-- Test clients to filter out (equivalent to TEST_CLIENTS)
CREATE OR REPLACE VIEW TEST_CLIENTS AS
SELECT column1 AS client_name FROM VALUES 
    ('test multi'),
    ('davidh test2 multi'),
    ('ice demo multi'),
    ('monitoring client pod2 multi');

-- Boolean string columns (equivalent to BOOLEAN_STRING_COLUMN)
CREATE OR REPLACE VIEW BOOLEAN_STRING_COLUMNS AS
SELECT column1 AS column_name FROM VALUES
    ('is_currency_converted'),
    ('is_eea'),
    ('is_external_mpi'),
    ('is_partial_amount'),
    ('is_prepaid'),
    ('is_sale_3d'),
    ('is_void'),
    ('liability_shift'),
    ('manage_3d_decision'),
    ('mc_scheme_token_used'),
    ('partial_approval_is_void'),
    ('rebill'),
    ('is_3d');

-- ================================================
-- TRANSACTION CONVERSION FUNCTIONS
-- ================================================

-- Create conversion columns function (equivalent to create_conversions_columns)
-- Note: In Snowflake, this is implemented as a macro/view since we can't modify DataFrames
CREATE OR REPLACE VIEW TRANSACTION_CONVERSIONS_TEMPLATE AS
WITH base_data AS (
    SELECT * FROM PLACEHOLDER_TABLE -- This will be replaced with actual table in usage
),

with_conversions AS (
    SELECT *,
           -- Conditional copies
           CASE WHEN transaction_type = 'auth3d' THEN is_sale_3d END AS is_sale_3d_auth_3d,
           CASE WHEN transaction_type = 'auth3d' THEN manage_3d_decision END AS manage_3d_decision_auth_3d,
           
           -- Transaction result status flags
           CASE WHEN transaction_type = 'initauth3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS init_status,
           
           CASE WHEN transaction_type = 'auth3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS auth_3d_status,
           
           CASE WHEN transaction_type = 'sale' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS sale_status,
           
           CASE WHEN transaction_type = 'auth' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS auth_status,
           
           CASE WHEN transaction_type = 'settle' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS settle_status,
           
           CASE WHEN transaction_type = 'verify_auth_3d' 
                THEN CASE WHEN transaction_result_id = '1006' THEN 'true' ELSE 'false' END 
           END AS verify_auth_3d_status,
           
           -- Challenge success logic
           CASE 
               WHEN "3d_flow_status" = '3d_success' THEN 'true'
               WHEN "3d_flow_status" IN ('3d_failure', '3d_wasnt_completed') THEN 'false'
           END AS is_successful_challenge,
           
           -- Exemption logic
           CASE 
               WHEN authentication_flow = 'exemption' THEN 'true'
               WHEN challenge_preference = 'y_requested_by_acquirer' THEN 'false'
           END AS is_successful_exemption,
           
           -- Frictionless logic
           CASE 
               WHEN authentication_flow = 'frictionless' AND status = '40' THEN 'true'
               WHEN authentication_flow = 'frictionless' THEN 'false'
           END AS is_successful_frictionless,
           
           -- Successful authentication logic
           CASE 
               WHEN "3d_flow_status" = '3d_success' 
                    OR (authentication_flow = 'frictionless' AND status = '40') THEN 'true'
               WHEN (acs_url IS NOT NULL AND authentication_flow != 'exemption')
                    OR (authentication_flow = 'frictionless' AND status != '40') THEN 'false'
           END AS is_successful_authentication,
           
           -- Approval logic
           CASE 
               WHEN auth_status = 'true' OR sale_status = 'true' THEN 'true'
               WHEN auth_status = 'false' OR sale_status = 'false' THEN 'false'
           END AS is_approved,
           
           -- Decline logic
           CASE 
               WHEN transaction_type IN ('sale', 'auth') AND transaction_result_id = '1008' THEN 'true'
               WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN 'false'
           END AS is_declined
           
    FROM base_data
)

SELECT * FROM with_conversions;

-- ================================================
-- DATA TYPE FIXING FUNCTIONS
-- ================================================

-- Boolean normalization function
CREATE OR REPLACE FUNCTION NORMALIZE_BOOLEAN_FIELD(input_value STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE 
        WHEN LOWER(TRIM(input_value)) IN ('true', '1', 'yes', '1.0') THEN TRUE
        WHEN LOWER(TRIM(input_value)) IN ('false', '0', 'no', '0.0') THEN FALSE
        ELSE NULL
    END
$$;

-- String cleaning function
CREATE OR REPLACE FUNCTION CLEAN_STRING_FIELD(input_value STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN LOWER(TRIM(input_value)) IN ('<na>', 'na', 'nan', 'none', '', ' ', '\\x00') THEN NULL
        WHEN input_value = 'deprecated' THEN NULL
        WHEN REGEXP_LIKE(input_value, '^\\d+\\.?\\d*$') THEN REGEXP_SUBSTR(input_value, '(\\d+)', 1, 1)
        ELSE LOWER(TRIM(input_value))
    END
$$;

-- Force null for specific columns
CREATE OR REPLACE FUNCTION FORCE_NULL_COLUMNS(column_name STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    column_name IN ('user_agent_3d', 'authentication_request', 'authentication_response', 'authorization_req_duration')
$$;

-- ================================================
-- MAIN TRANSFORMATION PROCEDURES
-- ================================================

-- Apply all data type fixes (equivalent to fixing_dtypes function)
CREATE OR REPLACE PROCEDURE APPLY_DATA_TYPE_FIXES(source_table_name STRING, target_view_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
    boolean_columns ARRAY;
    all_columns CURSOR FOR 
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = UPPER(SPLIT_PART(source_table_name, '.', -1))
          AND TABLE_SCHEMA = UPPER(SPLIT_PART(source_table_name, '.', -2));
BEGIN
    -- Build dynamic SQL for data type conversions
    sql_stmt := 'CREATE OR REPLACE VIEW ' || target_view_name || ' AS SELECT ';
    
    -- Add columns with proper type conversions
    FOR record IN all_columns DO
        -- Handle forced null columns
        IF FORCE_NULL_COLUMNS(record.COLUMN_NAME) THEN
            sql_stmt := sql_stmt || 'NULL::' || record.DATA_TYPE || ' AS ' || record.COLUMN_NAME || ', ';
        
        -- Handle boolean columns
        ELSIF EXISTS (SELECT 1 FROM BOOLEAN_STRING_COLUMNS WHERE column_name = record.COLUMN_NAME) THEN
            sql_stmt := sql_stmt || 'NORMALIZE_BOOLEAN_FIELD(' || record.COLUMN_NAME || ') AS ' || record.COLUMN_NAME || ', ';
        
        -- Handle string columns
        ELSIF record.DATA_TYPE = 'TEXT' OR record.DATA_TYPE LIKE 'VARCHAR%' THEN
            sql_stmt := sql_stmt || 'CLEAN_STRING_FIELD(' || record.COLUMN_NAME || ') AS ' || record.COLUMN_NAME || ', ';
        
        -- Handle float/double columns (set NULL to NaN equivalent)
        ELSIF record.DATA_TYPE IN ('FLOAT', 'DOUBLE') THEN
            sql_stmt := sql_stmt || 'COALESCE(' || record.COLUMN_NAME || ', ''NaN''::FLOAT) AS ' || record.COLUMN_NAME || ', ';
        
        -- Default: cast to expected type
        ELSE
            sql_stmt := sql_stmt || record.COLUMN_NAME || '::' || record.DATA_TYPE || ' AS ' || record.COLUMN_NAME || ', ';
        END IF;
    END FOR;
    
    -- Remove trailing comma and add FROM clause
    sql_stmt := TRIM(sql_stmt, ', ') || ' FROM ' || source_table_name;
    
    -- Execute the dynamic SQL
    EXECUTE IMMEDIATE sql_stmt;
    
    RETURN 'SUCCESS: Created view ' || target_view_name || ' with data type fixes applied';
END;
$$;

-- Filter and transform transactions (equivalent to filter_and_transform_transactions function)
CREATE OR REPLACE PROCEDURE FILTER_AND_TRANSFORM_TRANSACTIONS(
    source_table_name STRING,
    target_view_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
BEGIN
    -- Build comprehensive transformation SQL
    sql_stmt := '
    CREATE OR REPLACE VIEW ' || target_view_name || ' AS
    WITH base_data AS (
        SELECT *
        FROM ' || source_table_name || '
        WHERE multi_client_name NOT IN (SELECT client_name FROM TEST_CLIENTS)
    ),
    
    with_conversions AS (
        SELECT *,
               -- Conditional copies
               CASE WHEN transaction_type = ''auth3d'' THEN is_sale_3d END AS is_sale_3d_auth_3d,
               CASE WHEN transaction_type = ''auth3d'' THEN manage_3d_decision END AS manage_3d_decision_auth_3d,
               
               -- Transaction result status flags
               CASE WHEN transaction_type = ''initauth3d'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS init_status,
               
               CASE WHEN transaction_type = ''auth3d'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS auth_3d_status,
               
               CASE WHEN transaction_type = ''sale'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS sale_status,
               
               CASE WHEN transaction_type = ''auth'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS auth_status,
               
               CASE WHEN transaction_type = ''settle'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS settle_status,
               
               CASE WHEN transaction_type = ''verify_auth_3d'' 
                    THEN CASE WHEN transaction_result_id = ''1006'' THEN ''true'' ELSE ''false'' END 
               END AS verify_auth_3d_status,
               
               -- Challenge success
               CASE 
                   WHEN "3d_flow_status" = ''3d_success'' THEN ''true''
                   WHEN "3d_flow_status" IN (''3d_failure'', ''3d_wasnt_completed'') THEN ''false''
               END AS is_successful_challenge,
               
               -- Exemption logic
               CASE 
                   WHEN authentication_flow = ''exemption'' THEN ''true''
                   WHEN challenge_preference = ''y_requested_by_acquirer'' THEN ''false''
               END AS is_successful_exemption,
               
               -- Frictionless logic
               CASE 
                   WHEN authentication_flow = ''frictionless'' AND status = ''40'' THEN ''true''
                   WHEN authentication_flow = ''frictionless'' THEN ''false''
               END AS is_successful_frictionless,
               
               -- Successful authentication
               CASE 
                   WHEN "3d_flow_status" = ''3d_success'' 
                        OR (authentication_flow = ''frictionless'' AND status = ''40'') THEN ''true''
                   WHEN (acs_url IS NOT NULL AND authentication_flow != ''exemption'')
                        OR (authentication_flow = ''frictionless'' AND status != ''40'') THEN ''false''
               END AS is_successful_authentication,
               
               -- Approval logic
               CASE 
                   WHEN auth_status = ''true'' OR sale_status = ''true'' THEN ''true''
                   WHEN auth_status = ''false'' OR sale_status = ''false'' THEN ''false''
               END AS is_approved,
               
               -- Decline logic
               CASE 
                   WHEN transaction_type IN (''sale'', ''auth'') AND transaction_result_id = ''1008'' THEN ''true''
                   WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN ''false''
               END AS is_declined
               
        FROM base_data
    ),
    
    with_fixed_types AS (
        SELECT 
            -- Force null for specific columns
            NULL::STRING AS user_agent_3d,
            NULL::STRING AS authentication_request,
            NULL::STRING AS authentication_response,
            NULL::NUMBER AS authorization_req_duration,
            
            -- Boolean conversions for all boolean string columns
            NORMALIZE_BOOLEAN_FIELD(is_currency_converted) AS is_currency_converted,
            NORMALIZE_BOOLEAN_FIELD(is_eea) AS is_eea,
            NORMALIZE_BOOLEAN_FIELD(is_external_mpi) AS is_external_mpi,
            NORMALIZE_BOOLEAN_FIELD(is_partial_amount) AS is_partial_amount,
            NORMALIZE_BOOLEAN_FIELD(is_prepaid) AS is_prepaid,
            NORMALIZE_BOOLEAN_FIELD(is_sale_3d) AS is_sale_3d,
            NORMALIZE_BOOLEAN_FIELD(is_void) AS is_void,
            NORMALIZE_BOOLEAN_FIELD(liability_shift) AS liability_shift,
            NORMALIZE_BOOLEAN_FIELD(manage_3d_decision) AS manage_3d_decision,
            NORMALIZE_BOOLEAN_FIELD(mc_scheme_token_used) AS mc_scheme_token_used,
            NORMALIZE_BOOLEAN_FIELD(partial_approval_is_void) AS partial_approval_is_void,
            NORMALIZE_BOOLEAN_FIELD(rebill) AS rebill,
            NORMALIZE_BOOLEAN_FIELD(is_3d) AS is_3d,
            
            -- String cleaning for key fields
            CLEAN_STRING_FIELD(transaction_main_id) AS transaction_main_id,
            CLEAN_STRING_FIELD(multi_client_name) AS multi_client_name,
            CLEAN_STRING_FIELD(client_name) AS client_name,
            
            -- Keep all other columns as-is
            * EXCLUDE (
                user_agent_3d, authentication_request, authentication_response, authorization_req_duration,
                is_currency_converted, is_eea, is_external_mpi, is_partial_amount, is_prepaid,
                is_sale_3d, is_void, liability_shift, manage_3d_decision, mc_scheme_token_used,
                partial_approval_is_void, rebill, is_3d, transaction_main_id, multi_client_name, client_name
            )
            
        FROM with_conversions
    )
    
    SELECT * FROM with_fixed_types
    ';
    
    -- Execute the transformation
    EXECUTE IMMEDIATE sql_stmt;
    
    RETURN 'SUCCESS: Created transformed view ' || target_view_name;
END;
$$;

-- ================================================
-- VALIDATION AND TESTING FUNCTIONS
-- ================================================

-- Test the transformation logic
CREATE OR REPLACE PROCEDURE TEST_TRANSFORMATION_LOGIC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING := '';
BEGIN
    -- Test boolean normalization
    LET test_true := NORMALIZE_BOOLEAN_FIELD('true');
    LET test_false := NORMALIZE_BOOLEAN_FIELD('false');
    LET test_null := NORMALIZE_BOOLEAN_FIELD('invalid');
    
    IF test_true = TRUE AND test_false = FALSE AND test_null IS NULL THEN
        result := result || 'Boolean normalization: PASSED. ';
    ELSE
        result := result || 'Boolean normalization: FAILED. ';
    END IF;
    
    -- Test string cleaning
    LET test_clean := CLEAN_STRING_FIELD('  Test String  ');
    LET test_null_clean := CLEAN_STRING_FIELD('na');
    
    IF test_clean = 'test string' AND test_null_clean IS NULL THEN
        result := result || 'String cleaning: PASSED. ';
    ELSE
        result := result || 'String cleaning: FAILED. ';
    END IF;
    
    -- Test forced null columns
    IF FORCE_NULL_COLUMNS('user_agent_3d') = TRUE AND FORCE_NULL_COLUMNS('normal_column') = FALSE THEN
        result := result || 'Forced null columns: PASSED. ';
    ELSE
        result := result || 'Forced null columns: FAILED. ';
    END IF;
    
    RETURN 'TRANSFORMATION TESTS: ' || result;
END;
$$;

-- ================================================
-- USAGE EXAMPLES AND DOCUMENTATION
-- ================================================

-- Example 1: Apply transformations to a source table
/*
CALL FILTER_AND_TRANSFORM_TRANSACTIONS(
    'NUVEI_DWH.BRONZE.TRANSACTIONS', 
    'NUVEI_DWH.STAGING.TRANSACTIONS_TRANSFORMED'
);
*/

-- Example 2: Apply just data type fixes
/*
CALL APPLY_DATA_TYPE_FIXES(
    'NUVEI_DWH.BRONZE.TRANSACTIONS',
    'NUVEI_DWH.STAGING.TRANSACTIONS_TYPED'
);
*/

-- Example 3: Test all transformation functions
/*
CALL TEST_TRANSFORMATION_LOGIC();
*/

-- Example 4: Check test clients that will be filtered
/*
SELECT * FROM TEST_CLIENTS;
*/

-- Example 5: Check boolean string columns
/*
SELECT * FROM BOOLEAN_STRING_COLUMNS;
*/

-- ================================================
-- PERFORMANCE OPTIMIZATION NOTES
-- ================================================

/*
Performance Considerations for Snowflake Implementation:

1. **Clustering**: Consider clustering large tables by transaction_date for better performance
   ALTER TABLE target_table CLUSTER BY (transaction_date, transaction_main_id);

2. **Warehousing**: Use appropriate warehouse sizes for transformation workloads
   - Small: Development/testing
   - Medium: Regular ETL jobs
   - Large/X-Large: Heavy transformation workloads

3. **Result Caching**: Snowflake automatically caches query results for 24 hours

4. **Micro-partitions**: Snowflake automatically handles partitioning, but date-based
   clustering helps with time-series data

5. **Resource Monitors**: Set up resource monitors to control costs during development

6. **Query Optimization**: 
   - Use LIMIT for testing large transformations
   - Consider SAMPLE for testing on subsets of data
   - Use EXPLAIN to analyze query plans
*/

-- ================================================
-- End of Custom ETL Functions
-- ================================================
