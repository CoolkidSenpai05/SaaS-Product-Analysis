import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE gold__dim_accounts AS
SELECT
    event_ts_date AS EventDate,
    event_name AS EventName,
    event_category AS EventCategory,
    account_id AS AccountId,
            
    COUNT(*) AS EventCount,
    COUNT(DISTINCT user_id) AS UniqueUsers
FROM silver__product_events
GROUP BY
    event_ts_date,
    event_name,
    event_category,
    account_id
ORDER BY
    EventDate,
    EventName,
    AccountId;""")

print("gold__dim_accounts table created successfully.")
con.close()