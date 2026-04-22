import duckdb
import pandas as pd

con = duckdb.connect("product_analytics_light.db")

# path to excel file
excel_file_path = "raw__geography--.xlsx"

# load the excel file into a pandas DataFrame
df = pd.read_excel(excel_file_path, sheet_name=0)

# register the DataFrame as a temporary table in DuckDB
con.register("raw__geography_df", df)

# create the bronze__geography table
con.execute("""
CREATE TABLE IF NOT EXISTS bronze__geography AS
SELECT *
FROM raw__geography_df;
""")

print("Created bronze__geography table successfully.")

con.close()