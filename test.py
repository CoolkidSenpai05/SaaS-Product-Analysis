import duckdb

con = duckdb.connect("product_analytics_light.db")
print(con.execute("SHOW TABLES").fetchall())
print(con.execute("SELECT * FROM raw__users LIMIT 5").fetchall())