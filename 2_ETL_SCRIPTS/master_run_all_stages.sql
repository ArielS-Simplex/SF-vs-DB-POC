-- ==============================================================================
-- MASTER ETL SCRIPT - RUN ALL STAGES IN SEQUENCE
-- Execute complete ETL pipeline with proper stage separation
-- ==============================================================================

-- STAGE 1: Setup and Metadata
.read 1st_stage_setup_and_metadata.sql

-- STAGE 2: Data Transformation and Business Logic
.read 2nd_stage_data_transformation.sql

-- STAGE 3: Table Creation and Data Loading
.read 3rd_stage_table_creation_and_loading.sql

-- STAGE 4: Testing and Validation
.read 4th_stage_testing_and_validation.sql

-- STAGE 5: Cleanup
.read 5th_stage_cleanup.sql

-- FINAL SUMMARY
SELECT 
    'COMPLETE ETL PIPELINE FINISHED' AS final_message,
    CURRENT_TIMESTAMP() AS completion_time,
    'All 5 stages executed successfully' AS details;
