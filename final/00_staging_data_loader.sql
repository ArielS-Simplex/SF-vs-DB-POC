-- STEP 1: STAGING DATA LOADING - SEPTEMBER 2-9, 2025
-- This script loads raw production data into staging table
-- Run this FIRST, then run the bronze processing script

-- STEP 1: Create staging table for raw data loading - V2 VERSION
CREATE OR REPLACE TABLE poc.public.ncp_bronze_staging_v2 (
    filename STRING,
    loaded_at TIMESTAMP_NTZ,
    raw_line STRING
);

-- STEP 2: Create file format for raw line loading
CREATE OR REPLACE FILE FORMAT txt_format_raw
TYPE = 'CSV' 
FIELD_DELIMITER = NONE  -- No field delimiter - entire line is one field
SKIP_HEADER = 0 
ENCODING = 'ISO-8859-1' 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 3: Load production data - 7 DAYS (September 2-8, 2025)
-- Process full week of data for comprehensive testing - OPTIMIZED SINGLE COMMAND
COPY INTO poc.public.ncp_bronze_staging_v2 (filename, loaded_at, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-0[2-8].*'  -- Sept 2-8 (7 days)
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

-- STEP 4: Verify staging load results
SELECT 
    'STAGING LOAD SUMMARY' AS step,
    COUNT(*) AS total_rows_loaded,
    COUNT(DISTINCT filename) AS files_loaded,
    MIN(loaded_at) AS first_load_time,
    MAX(loaded_at) AS last_load_time,
    MIN(LENGTH(raw_line)) AS min_line_length,
    MAX(LENGTH(raw_line)) AS max_line_length
FROM poc.public.ncp_bronze_staging_v2;

-- Show sample of loaded data
SELECT 
    'STAGING SAMPLE DATA' AS step,
    filename,
    loaded_at,
    LEFT(raw_line, 200) AS sample_line_start,
    LENGTH(raw_line) AS line_length
FROM poc.public.ncp_bronze_staging_v2 
ORDER BY loaded_at 
LIMIT 10;

-- STAGING COMPLETE MESSAGE
SELECT 'STAGING LOAD COMPLETE - Ready for Bronze Processing!' AS status,
       'Run final/cost_tracking_staging.sql to see operation costs' AS cost_tracking_note;
