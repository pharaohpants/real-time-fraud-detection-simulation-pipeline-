-- Gold: Fraud summary aggregated by hour and merchant
WITH fraud_data AS (
    SELECT
        DATE_TRUNC('hour', t.timestamp) AS hour_bucket,
        t.merchant,
        t.transaction_id,
        t.amount,
        COALESCE(f.risk_score, 0) AS risk_score,
        COALESCE(f.decision, 'APPROVE') AS decision
    FROM {{ ref('int_transactions_enriched') }} t
    LEFT JOIN {{ source('fraud_db', 'fraud_flags') }} f 
        ON t.transaction_id = f.transaction_id
)
SELECT
    hour_bucket,
    merchant,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN decision != 'APPROVE' THEN 1 END) AS fraud_count,
    COUNT(CASE WHEN decision = 'BLOCK' THEN 1 END) AS block_count,
    COUNT(CASE WHEN decision = 'REVIEW' THEN 1 END) AS review_count,
    ROUND(
        COUNT(CASE WHEN decision != 'APPROVE' THEN 1 END)::NUMERIC / 
        COUNT(*) * 100, 2
    ) AS fraud_rate_pct,
    ROUND(AVG(risk_score)::NUMERIC, 2) AS avg_risk_score
FROM fraud_data
GROUP BY hour_bucket, merchant
