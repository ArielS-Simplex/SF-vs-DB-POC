-- ==============================================================================
-- VALIDATE INCREMENTAL PROCESSING SUCCESS
-- ==============================================================================

-- Test 1: Verify first run results
SELECT 'Test 1: First Run Results' AS test_name,
       COUNT(*) AS record_count,
       CASE WHEN COUNT(*) = 12686818 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4;

-- Test 2: Check checkpoint advancement
SELECT 'Test 2: Checkpoint Advancement' AS test_name,
       checkpoint_time,
       last_run_status,
       records_processed,
       CASE 
         WHEN checkpoint_time > '2025-09-09 00:00:00'::TIMESTAMP THEN '✅ PASS - Checkpoint Advanced'
         ELSE '❌ FAIL - Checkpoint Not Advanced'
       END AS status
FROM POC.PUBLIC.etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';

-- Test 3: Verify key derived columns are populated
SELECT 'Test 3: Business Logic Validation' AS test_name,
       COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) AS init_status_count,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth_3d_status_count,
       COUNT(CASE WHEN is_approved IS NOT NULL THEN 1 END) AS is_approved_count,
       CASE 
         WHEN COUNT(CASE WHEN init_status IS NOT NULL THEN 1 END) > 0 THEN '✅ PASS'
         ELSE '❌ FAIL'
       END AS status
FROM POC.PUBLIC.NCP_SILVER_V4;

-- Test 4: Check for etl_processed_at timestamp
SELECT 'Test 4: ETL Metadata' AS test_name,
       COUNT(CASE WHEN etl_processed_at IS NOT NULL THEN 1 END) AS etl_timestamp_count,
       MIN(etl_processed_at) AS min_etl_time,
       MAX(etl_processed_at) AS max_etl_time,
       CASE 
         WHEN COUNT(CASE WHEN etl_processed_at IS NOT NULL THEN 1 END) = COUNT(*) THEN '✅ PASS'
         ELSE '❌ FAIL'
       END AS status
FROM POC.PUBLIC.NCP_SILVER_V4;