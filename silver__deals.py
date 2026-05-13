import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE silver__deals AS
SELECT
    deal_id,
    account_id,
    owner_user_id,
    pipeline_id,
    current_stage_id,

    LOWER(status) AS status,

    created_at,
    CAST(created_at AS DATE) AS created_date,

    closed_at,
    CAST(closed_at AS DATE) AS close_date,    
            
    last_stage_changed_at,
    CAST(last_stage_changed_at AS DATE) AS last_stage_changed_date,
            
    amount,
    UPPER(currency) AS currency
    UPPER(country_code) AS country_code
            
    source_system,
            
    (closed_at IS NOT NULL) AS is_closed,
            
    CASE
        WHEN LOWER(status) IN ('closed_won', 'won') THEN TRUE
        ELSE FALSE
    END AS is_won
            
    CASE
        WHEN closed_at IS NOT NULL THEN DATE_DIFF('day', CAST(created_at AS DATE), CAST(closed_at AS DATE))
        ELSE NULL
    END AS deal_cycle_days,
            
    CASE
        WHEN CAST(created_at AS DATE) > CURRENT_DATE THEN 0
        ELSE DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE)
    END AS deal_age_days
            
FROM bronze__deals;
""")

print("Created silver__deals table successfully.")

con.close()