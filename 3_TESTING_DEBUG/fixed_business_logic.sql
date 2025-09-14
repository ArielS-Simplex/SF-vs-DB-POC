-- ==============================================================================
-- FIXED BUSINESS LOGIC
-- Replace the business logic section in complete_etl_production.sql
-- ==============================================================================

-- This replaces the with_status_flags CTE in your ETL
with_status_flags AS (
    -- Step 6: Add transaction status flags - SIMPLIFIED AND FIXED
    SELECT *,
        threed_flow_status AS "3d_flow_status",
        
        -- SIMPLIFIED STATUS FLAGS: Just check transaction_result_id directly
        CASE WHEN transaction_type = 'initauth3d' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS init_status,
        
        CASE WHEN transaction_type = 'auth3d' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS auth_3d_status,
        
        CASE WHEN transaction_type = 'sale' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS sale_status,
        
        CASE WHEN transaction_type = 'auth' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS auth_status,
        
        CASE WHEN transaction_type = 'settle' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS settle_status,
        
        CASE WHEN transaction_type = 'verify_auth_3d' THEN 
            CASE WHEN transaction_result_id = '1006' THEN true ELSE false END 
            ELSE NULL
        END AS verify_auth_3d_status,
        
        -- SUCCESS METRICS: Simplified conditions
        CASE 
            WHEN threed_flow_status = '3D_Success' THEN true
            WHEN threed_flow_status = '3D_Failure' THEN false
            ELSE NULL
        END AS is_successful_challenge,
        
        CASE 
            WHEN authentication_flow = 'Exemption' THEN true
            ELSE NULL
        END AS is_successful_exemption,
        
        CASE 
            WHEN authentication_flow = 'Frictionless' AND status = '40' THEN true
            WHEN authentication_flow = 'Frictionless' AND status != '40' THEN false
            ELSE NULL
        END AS is_successful_frictionless,
        
        CASE 
            WHEN threed_flow_status = '3D_Success' THEN true
            WHEN authentication_flow = 'Frictionless' AND status = '40' THEN true
            WHEN authentication_flow = 'Exemption' THEN true
            WHEN threed_flow_status = '3D_Failure' THEN false
            WHEN authentication_flow = 'Frictionless' AND status != '40' THEN false
            ELSE NULL
        END AS is_successful_authentication
        
    FROM filtered_data
),

final_data AS (
    -- Step 7: Add derived business logic - SIMPLIFIED
    SELECT *,
        CASE 
            WHEN auth_status = true OR sale_status = true THEN true
            WHEN auth_status = false OR sale_status = false THEN false
            ELSE NULL
        END AS is_approved,
        
        CASE 
            WHEN transaction_type IN ('sale', 'auth') AND transaction_result_id = '1008' THEN true
            WHEN auth_status IS NOT NULL OR sale_status IS NOT NULL THEN false
            ELSE NULL
        END AS is_declined
        
    FROM with_status_flags
)
