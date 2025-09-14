-- ==============================================================================
-- TRANSACTION TYPE ANALYSIS
-- Check what transaction types actually exist in the data
-- ==============================================================================

SET TARGET_TABLE = 'POC.PUBLIC.transactions_silver';

-- 1. What transaction types do we actually have?
SELECT 
    'ACTUAL TRANSACTION TYPES' AS check_type,
    transaction_type,
    COUNT(*) AS count,
    COUNT(CASE WHEN transaction_result_id = '1006' THEN 1 END) AS success_count,
    COUNT(CASE WHEN transaction_result_id = '1008' THEN 1 END) AS decline_count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY transaction_type
ORDER BY count DESC;

-- 2. Sample records with their transaction types and result IDs
SELECT 
    'SAMPLE TRANSACTION DATA' AS check_type,
    transaction_type,
    transaction_result_id,
    final_transaction_status,
    authentication_flow,
    COUNT(*) AS count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY transaction_type, transaction_result_id, final_transaction_status, authentication_flow
ORDER BY count DESC
LIMIT 20;
