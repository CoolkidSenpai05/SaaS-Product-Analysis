import duckdb

con = duckdb.connect("product_analytics_light.db")

# create the bronze__users table
con.execute("""
CREATE TABLE IF NOT EXISTS bronze__users AS
SELECT *
FROM raw__users;
""")

print("Created bronze__users table successfully.")

con.close()