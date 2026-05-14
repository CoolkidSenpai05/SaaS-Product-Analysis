import duckdb

con = duckdb.connect("product_analytics_light.db")

con.execute("""
CREATE OR REPLACE TABLE gold__dim_accounts AS
SELECT
    deal_id AS DealId,
    account_id AS AccountId,
    owner_user_id AS OwnerUserId,
            
    pipeline_id AS PipelineId,
    current_stage_id AS CurrentStageId,
            
    status AS Status,
            
    created_at AS CreatedAt,
    created_date AS CreatedDate,
            
    closed_at AS ClosedAt,
    closed_date AS ClosedDate,
            
    last_stage_change_at AS LastStageChangeAt,
    last_stage_change_date AS LastStageChangeDate,
            
    amount AS DealAmount,
    currency AS Currency,
    currency_code AS CurrencyCode,
            
    source_system AS SourceSystem,
            
    is_closed AS IsClosed,
    is_won AS IsWon,
            
    deal_cycle_days AS DealCycleDays,
    deal_age_days AS DealAgeDays
FROM silver__deals
ORDER BY DealId;""")

print("gold__dim_accounts table created successfully.")
con.close()