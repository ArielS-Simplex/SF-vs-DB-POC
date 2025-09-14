-- ==============================================================================
-- INITIAL STAGING SETUP (if starting from scratch)
-- This script sets up the staging environment for the first time
-- ==============================================================================

-- STEP 1: Create staging table for raw data
CREATE OR REPLACE TABLE poc.public.ncp_bronze_staging (
    filename STRING,
    DW_CREATED_AT TIMESTAMP_NTZ,
    raw_line STRING
);

-- STEP 2: Create raw file format (treat each line as one field)
CREATE OR REPLACE FILE FORMAT txt_format_raw
TYPE = 'CSV' 
FIELD_DELIMITER = NONE  -- No field delimiter - entire line is one field
SKIP_HEADER = 0 
ENCODING = 'ISO-8859-1' 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 3: Load one test file to verify setup
COPY INTO poc.public.ncp_bronze_staging (filename, DW_CREATED_AT, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-06_00-00-00.txt'  -- One file for testing
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

-- STEP 4: Verify staging setup
SELECT 
    'STAGING SETUP COMPLETE' AS status,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT filename) AS unique_files,
    MIN(LENGTH(raw_line)) AS min_line_length,
    MAX(LENGTH(raw_line)) AS max_line_length
FROM poc.public.ncp_bronze_staging;

-- Show sample data
SELECT filename, LEFT(raw_line, 100) AS sample_line 
FROM poc.public.ncp_bronze_staging 
LIMIT 3;
