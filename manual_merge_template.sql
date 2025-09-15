-- ==============================================================================
-- MANUAL MERGE TEMPLATE - TRUE MERGE FOR FAIR DATABRICKS COMPARISON
-- ==============================================================================

MERGE INTO IDENTIFIER($TARGET_TABLE) AS target
USING IDENTIFIER($STAGING_TABLE) AS source
ON target.transaction_main_id = source.transaction_main_id 
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN UPDATE SET
    -- YOU NEED TO COPY ALL 174 COLUMNS FROM THE STAGING TABLE HERE
    -- Example format:
    target.transaction_main_id = source.transaction_main_id,
    target.transaction_date = source.transaction_date,
    target.transaction_id_life_cycle = source.transaction_id_life_cycle,
    target.transaction_date_life_cycle = source.transaction_date_life_cycle,
    target.transaction_type_id = source.transaction_type_id,
    target.transaction_type = source.transaction_type,
    target.transaction_result_id = source.transaction_result_id,
    target.final_transaction_status = source.final_transaction_status,
    target.threed_flow_status = source.threed_flow_status,
    target.challenge_preference = source.challenge_preference,
    -- ... continue for ALL 174 columns ...
    target.inserted_at = source.inserted_at
WHEN NOT MATCHED THEN INSERT (
    -- ALL 174 column names
    transaction_main_id, transaction_date, transaction_id_life_cycle, /* ... ALL 174 ... */
) VALUES (
    -- ALL 174 source columns  
    source.transaction_main_id, source.transaction_date, source.transaction_id_life_cycle, /* ... ALL 174 ... */
);