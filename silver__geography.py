import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TALBE silver__geography AS
WITH base AS (
    SELECT
        UPPER(TRIM(country_code)) AS country_code,
        country_name
        region,
        market,
        currency,
        sales_region
    FROM bronze__geography
),
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY country_code
            ORDER BY country_namne
            ) AS rn
    FROM base
)
SELECT
    country_code,
    country_name,
    region,
            
    --Fill a small known gap from the raw sheet
    CASE
        WHEN country_code = 'UK' AND market IS NULL THEN 'UKI'
        ELSE market
    END AS market,
            
    currency,
    sales_region
FROM deduped
WHERE rn = 1;
""")

print("Created silver__geography table successfully.")

con.close()