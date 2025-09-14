-- ==============================================================================
-- 5TH STAGE: CLEANUP
-- Clean up temporary tables and finalize ETL
-- ==============================================================================

-- REQUIRED VARIABLES (if not already set in previous stages)
SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- Cleanup temporary tables
DROP TABLE IF EXISTS processed_data;
DROP TABLE IF EXISTS schema_comparison;
DROP TABLE IF EXISTS add_columns_sql;

-- Final verification query
SELECT 
    'ETL PIPELINE COMPLETED' AS final_status,
    COUNT(*) AS final_row_count,
    MIN(transaction_date) AS earliest_transaction,
    MAX(transaction_date) AS latest_transaction,
    COUNT(DISTINCT multi_client_name) AS unique_clients,
    COUNT(CASE WHEN is_approved = true THEN 1 END) AS approved_count,
    COUNT(CASE WHEN is_declined = true THEN 1 END) AS declined_count
FROM IDENTIFIER($TARGET_TABLE);

SELECT 'STAGE 5 COMPLETED: Cleanup finished - ETL pipeline complete' AS status;
