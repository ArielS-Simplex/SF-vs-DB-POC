-- DATABRICKS: Compare the same specific transaction IDs
WITH deduplicated AS (
    SELECT *
    FROM ncp.silver
    WHERE DATE(transaction_date) = '2025-09-05'
      AND transaction_main_id IS NOT NULL 
      AND transaction_date IS NOT NULL
      AND LOWER(TRIM(multi_client_name)) NOT IN (
        'test multi', 'davidh test2 multi', 'ice demo multi', 'monitoring client pod2 multi'
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_main_id, transaction_date ORDER BY inserted_at DESC) = 1
)
SELECT 
    'DATABRICKS_SPECIFIC_RECORDS' AS source,
    transaction_main_id,
    transaction_date,
    multi_client_name,
    CAST(amount_in_usd AS DECIMAL(18,2)) AS amount_in_usd,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_void, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS is_void,
    CASE 
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('yes', 'true', '1') THEN true
        WHEN LOWER(TRIM(COALESCE(is_3d, ''))) IN ('no', 'false', '0', '') THEN false
        ELSE NULL
    END AS is_3d,
    UPPER(TRIM(REGEXP_REPLACE(COALESCE(final_transaction_status, ''), '[^A-Za-z0-9\\s]', ''))) AS final_transaction_status
FROM deduplicated
WHERE transaction_main_id IN (
    '1120000004642701595',
    '1120000004643561000', 
    '1120000004646187576',
    '1120000004647424581',
    '1120000004650264849'
)
ORDER BY transaction_main_id, transaction_date;
