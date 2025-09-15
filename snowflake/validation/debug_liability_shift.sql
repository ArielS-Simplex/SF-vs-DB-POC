-- Debug liability_shift values in bronze data
SELECT 'Liability Shift Values' AS test_type,
       liability_shift AS value,
       LENGTH(liability_shift) AS length,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM POC.PUBLIC.NCP_BRONZE_V2 WHERE DATE(transaction_date) = '2025-09-05'), 2) AS percentage
FROM POC.PUBLIC.NCP_BRONZE_V2 
WHERE DATE(transaction_date) = '2025-09-05'
GROUP BY liability_shift 
ORDER BY count DESC;

-- Check what current ETL produces for liability_shift
SELECT 'Current Silver Liability Shift' AS test_type,
       liability_shift AS value,
       COUNT(*) AS count
FROM POC.PUBLIC.NCP_SILVER_V2
GROUP BY liability_shift
ORDER BY count DESC;
