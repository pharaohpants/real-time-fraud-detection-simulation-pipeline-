-- Silver: Enriched transactions with derived columns
WITH raw_tx AS (
    SELECT * FROM {{ ref('stg_raw_transactions') }}
),
fraud_flags AS (
    SELECT * FROM {{ ref('stg_fraud_flags') }}
)
SELECT
    rt.transaction_id,
    rt.customer_id,
    rt.merchant,
    rt.amount,
    rt.location,
    rt.lat,
    rt.lon,
    rt.card_type,
    rt.timestamp,
    rt.ingested_at,
    EXTRACT(HOUR FROM rt.timestamp) AS hour_of_day,
    EXTRACT(DOW FROM rt.timestamp) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM rt.timestamp) IN (0, 6) THEN true
        ELSE false
    END AS is_weekend,
    CASE
        WHEN rt.amount < 50000 THEN 'micro'
        WHEN rt.amount < 500000 THEN 'small'
        WHEN rt.amount < 5000000 THEN 'medium'
        ELSE 'large'
    END AS amount_bucket,
    CASE
        WHEN ff.transaction_id IS NOT NULL THEN true
        ELSE false
    END AS has_fraud_flag,
    COALESCE(ff.risk_score, 0) AS risk_score,
    COALESCE(ff.decision, 'APPROVE') AS fraud_decision
FROM raw_tx rt
LEFT JOIN fraud_flags ff ON rt.transaction_id = ff.transaction_id
