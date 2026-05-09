-- Gold: Fraud rule performance metrics
WITH unpivoted AS (
    SELECT transaction_id, risk_score, 'VELOCITY' AS rule_name
    FROM {{ source('fraud_db', 'fraud_flags') }}
    WHERE is_velocity

    UNION ALL

    SELECT transaction_id, risk_score, 'IMPOSSIBLE_TRAVEL' AS rule_name
    FROM {{ source('fraud_db', 'fraud_flags') }}
    WHERE is_impossible_travel

    UNION ALL

    SELECT transaction_id, risk_score, 'AMOUNT_SPIKE' AS rule_name
    FROM {{ source('fraud_db', 'fraud_flags') }}
    WHERE is_amount_spike

    UNION ALL

    SELECT transaction_id, risk_score, 'OFF_HOURS' AS rule_name
    FROM {{ source('fraud_db', 'fraud_flags') }}
    WHERE is_off_hours

    UNION ALL

    SELECT transaction_id, risk_score, 'CARD_TESTING' AS rule_name
    FROM {{ source('fraud_db', 'fraud_flags') }}
    WHERE is_card_testing
),
total_txs AS (
    SELECT COUNT(*) AS total_count FROM {{ ref('int_transactions_enriched') }}
)
SELECT
    rule_name,
    COUNT(*) AS trigger_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT total_count FROM total_txs) * 100, 2) AS trigger_pct,
    ROUND(AVG(risk_score)::NUMERIC, 2) AS avg_risk_score_when_triggered
FROM unpivoted
GROUP BY rule_name
ORDER BY trigger_count DESC
