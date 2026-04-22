import duckdb

con = duckdb.connect("product_analytics_light.db")

# create the bronze__accounts table
con.execute("""
CREATE TABLE IF NOT EXISTS bronze__accounts AS
SELECT
        account_id,
        account_name,
        country_code,
        city,
        industry,
        employee_band,
        segment,
        created_at,
        trial_start_date,
        trial_end_date,
        account_status,
        acquisition_channel
FROM raw__accounts;
""")

print("Created bronze__accounts table successfully.")

con.close()