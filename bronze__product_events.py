import duckdb

con = duckdb.connect("product_analytics_light.db")

# create the bronze__product_events table
con.execute("""
CREATE TABLE IF NOT EXISTS bronze__product_events AS
SELECT *
FROM raw__product_events;
""")

print("Created bronze__product_events table successfully.")

con.close()