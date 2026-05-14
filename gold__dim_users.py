import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE gold__dim_users AS
SELECT
    user_id AS UserId,
    account_id AS AccountId,
            
    full_name AS FullName,
    email_domain AS EmailDomain,
    job_role AS JobRole,
    user_status AS UserStatus,
            
    is_active_user AS IsActiveUser,
    is_admin AS IsAdmin,
            
    created_at AS CreatedAt,
    created_date AS CreatedDate,
    last_seen_at AS LastSeenAt,
            
    user_tenure_days AS UserTenureDays,
    days_since_last_seen AS DaysSinceLastSeen,
    recency_bucket AS RecencyBucket
FROM silver__users
ORDER BY UserId;""")

print("gold__dim_users table created successfully.")
con.close()