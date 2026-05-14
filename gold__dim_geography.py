import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE gold__dim_geography AS
SELECT
    country_code AS CountryCode,
    country_name AS CountryName,
    region AS Region,
    market AS Market,
    sales_region AS SalesRegion,
    currency_code AS Currency
FROM silver__geography
ORDER BY CountryCode;""")

print("gold__dim_geography table created successfully.")
con.close()