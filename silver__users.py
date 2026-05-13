import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE silver__users AS
SELECT
    user_id,
    account_id,
            
    -- Core descriptors
    full_name,
    LOWER(SPLIT_PART(email, '@', 2)) AS email_domain,
    last_seen_at,
            
    -- Tenure and recency
    CASE
        WHEN CAST(created_at AS DATE) > CURRENT_DATE THEN 0
        ELSE DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE)
    END AS user_tenure_days,
            
    CASE
        WHEN last_seen_at IS NULL THEN 'never seen'
        WHEN DATE DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 7 THEN '0-7 days'
        WHEN DATE DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 30 THEN '8-30 days'
        WHEN DATE DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 90 THEN '31-90 days'
        ELSE '90+ days'
    END AS recency_bucket,
            
    --Flags
    (user_status = 'active') AS is_active_user,
    is_admin
            
FROM bronze__users;""")

print("silver__users table created successfully.")

con.close()