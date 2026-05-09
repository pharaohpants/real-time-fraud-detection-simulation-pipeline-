-- Gold: Daily volume metrics
WITH daily_data AS (
    SELECT
        DATE(t.timestamp) AS date,
        t.transaction_id,
        t.amount,
        t.customer_id,
        COALESCE(f.decision, 'APPROVE') AS decision
    FROM {{ ref('int_transactions_enriched') }} t
    LEFT JOIN {{ source('fraud_db', 'fraud_flags') }} f 
        ON t.transaction_id = f.transaction_id
)
SELECT
    date,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount,
    ROUND(
        COUNT(CASE WHEN decision != 'APPROVE' THEN 1 END)::NUMERIC / 
        COUNT(*) * 100, 2
    ) AS fraud_rate_pct,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM daily_data
GROUP BY date
ORDER BY date DESC
