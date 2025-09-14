-- ==============================================================================
-- OPTIONAL: LOAD MORE DATA TO STAGING (if needed)
-- This script loads additional data files to ncp_bronze_staging
-- Only run this if you need more data than what's already in staging
-- ==============================================================================

-- OPTION 1: Load specific files from Sept 4-6 (3 days for performance testing)
COPY INTO poc.public.ncp_bronze_staging (filename, DW_CREATED_AT, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-04*'  -- Sept 4
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

COPY INTO poc.public.ncp_bronze_staging (filename, DW_CREATED_AT, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-05*'  -- Sept 5
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

COPY INTO poc.public.ncp_bronze_staging (filename, DW_CREATED_AT, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-06*'  -- Sept 6
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

-- OPTION 2: Load just one hour for quick testing
COPY INTO poc.public.ncp_bronze_staging (filename, DW_CREATED_AT, raw_line)
FROM (
    SELECT
        METADATA$FILENAME::string,
        CURRENT_TIMESTAMP,
        $1::string
    FROM @NCP/bpa.STP_BusinessAnalyticsQuery/
)
PATTERN = 'file2-bpa.STP_BusinessAnalyticsQuery-2025-09-06_00-00-00.txt'  -- Just one file
FILE_FORMAT = (FORMAT_NAME = 'txt_format_raw') 
ON_ERROR = CONTINUE;

-- Check staging data after loading
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT filename) AS unique_files,
    MIN(DW_CREATED_AT) AS first_loaded,
    MAX(DW_CREATED_AT) AS last_loaded
FROM poc.public.ncp_bronze_staging;
