import duckdb

con = duckdb.connect("product_analytics_light.db")

# create the bronze__deals table
con.execute("""
CREATE TABLE IF NOT EXISTS bronze__deals AS
SELECT *
FROM raw__deals;
""")

print("Created bronze__deals table successfully.")

con.close()