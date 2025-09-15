-- VALIDATION SUITE FOR INCREMENTAL ETL IMPLEMENTATION
-- Run after each phase to ensure no regressions

-- ========================================
-- 1. BASIC DATA VALIDATION
-- ========================================

-- Row count validation (should always be 12,686,818 for Sept 5)
SELECT 'Row Count Validation' AS test_name,
       COUNT(*) AS actual_count,
       12686818 AS expected_count,
       CASE WHEN COUNT(*) = 12686818 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4 
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- 2. BUSINESS LOGIC VALIDATION  
-- ========================================

-- Status flags populated correctly
SELECT 'Status Flags Validation' AS test_name,
       COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) AS auth_status_count,
       COUNT(CASE WHEN sale_status IS NOT NULL THEN 1 END) AS sale_status_count,
       COUNT(CASE WHEN auth_3d_status IS NOT NULL THEN 1 END) AS auth_3d_status_count,
       CASE WHEN COUNT(CASE WHEN auth_status IS NOT NULL THEN 1 END) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4 
WHERE DATE(transaction_date) = '2025-09-05';

-- Conditional copies validation (should be 1,571,569 for auth3d)
SELECT 'Conditional Copies Validation' AS test_name,
       COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) AS auth3d_conditional_copies,
       1571569 AS expected_count,
       CASE WHEN COUNT(CASE WHEN UPPER(transaction_type) = 'AUTH3D' AND is_sale_3d_auth_3d IS NOT NULL THEN 1 END) >= 1571569 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4 
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- 3. INCREMENTAL PROCESSING VALIDATION (Phase 2+)
-- ========================================

-- Checkpoint metadata exists and is recent
SELECT 'Checkpoint Validation' AS test_name,
       table_name,
       checkpoint_time,
       CASE WHEN checkpoint_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM etl_metadata 
WHERE table_name = 'NCP_SILVER_V4';

-- ========================================
-- 4. SCHEMA VALIDATION
-- ========================================

-- Column count validation (should be 173+ columns)
SELECT 'Schema Validation' AS test_name,
       COUNT(*) AS column_count,
       CASE WHEN COUNT(*) >= 173 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_name = 'NCP_SILVER_V4' 
  AND table_schema = 'PUBLIC';

-- ========================================
-- 5. DATA QUALITY VALIDATION
-- ========================================

-- Test client filtering validation
SELECT 'Test Client Filtering' AS test_name,
       COUNT(*) AS test_client_records,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4 
WHERE DATE(transaction_date) = '2025-09-05'
  AND LOWER(TRIM(multi_client_name)) IN (
    'test multi', 
    'davidh test2 multi', 
    'ice demo multi', 
    'monitoring client pod2 multi'
  );

-- Boolean conversion validation (liability_shift should have both TRUE/FALSE)
SELECT 'Boolean Conversion Validation' AS test_name,
       COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) AS true_count,
       COUNT(CASE WHEN liability_shift = FALSE THEN 1 END) AS false_count,
       CASE WHEN COUNT(CASE WHEN liability_shift = TRUE THEN 1 END) > 0 
            AND COUNT(CASE WHEN liability_shift = FALSE THEN 1 END) > 0 
            THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM POC.PUBLIC.NCP_SILVER_V4 
WHERE DATE(transaction_date) = '2025-09-05';

-- ========================================
-- 6. PERFORMANCE VALIDATION
-- ========================================

-- Execution time validation (should complete in reasonable time)
SELECT 'Performance Check' AS test_name,
       'Manual timing required' AS note,
       '< 5 minutes expected' AS target_time;

-- ========================================
-- SUMMARY REPORT
-- ========================================

SELECT 'VALIDATION SUMMARY' AS summary,
       CURRENT_TIMESTAMP() AS test_time,
       'Run all validation queries above and verify all show ✅ PASS' AS instructions;