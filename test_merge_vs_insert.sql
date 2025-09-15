-- ==============================================================================
-- TEST: VERIFY MERGE OPERATIONS (DELETE + INSERT) vs SIMPLE INSERT
-- ==============================================================================

-- STEP 1: Check current state
SELECT 'Current Total' AS test_type, COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2;

-- STEP 2: Check for any overlapping dates that would test MERGE functionality
SELECT 
    DATE(transaction_date) as date,
    COUNT(*) as records,
    MIN(inserted_at) as earliest_inserted,
    MAX(inserted_at) as latest_inserted
FROM POC.PUBLIC.NCP_SILVER_V2
GROUP BY DATE(transaction_date)
ORDER BY date;

-- STEP 3: THE REAL TEST - Re-run Sept 2 data to see if MERGE works
-- If it's true MERGE: count should stay 22,319,066
-- If it's just INSERT: count would increase to ~33M

-- Before re-running Sept 2, let's check if there are any duplicate transaction_main_id + transaction_date
SELECT 'Duplicate Check Before' AS test_type, COUNT(*) AS duplicate_transactions
FROM (
    SELECT 
        transaction_main_id,
        transaction_date,
        COUNT(*) as cnt
    FROM POC.PUBLIC.NCP_SILVER_V2
    GROUP BY transaction_main_id, transaction_date
    HAVING COUNT(*) > 1
) duplicates;

-- STEP 4: Sample a few Sept 2 records to track them
SELECT 'Sample Sept 2 Records' AS test_type, 
       transaction_main_id, 
       transaction_date,
       inserted_at,
       amount_in_usd
FROM POC.PUBLIC.NCP_SILVER_V2
WHERE DATE(transaction_date) = '2025-09-02'
LIMIT 5;