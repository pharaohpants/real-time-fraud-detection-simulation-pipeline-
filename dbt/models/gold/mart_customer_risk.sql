-- Gold: Customer risk profile
WITH customer_data AS (
    SELECT
        t.customer_id,
        t.transaction_id,
        t.amount,
        COALESCE(f.risk_score, 0) AS risk_score,
        COALESCE(f.decision, 'APPROVE') AS decision,
        COALESCE(f.is_velocity, false) AS is_velocity,
        COALESCE(f.is_impossible_travel, false) AS is_impossible_travel,
        COALESCE(f.is_amount_spike, false) AS is_amount_spike,
        COALESCE(f.is_off_hours, false) AS is_off_hours,
        COALESCE(f.is_card_testing, false) AS is_card_testing
    FROM {{ ref('int_transactions_enriched') }} t
    LEFT JOIN {{ source('fraud_db', 'fraud_flags') }} f 
        ON t.transaction_id = f.transaction_id
),
rule_counts AS (
    SELECT
        customer_id,
        COUNT(CASE WHEN is_velocity THEN 1 END) AS velocity_count,
        COUNT(CASE WHEN is_impossible_travel THEN 1 END) AS impossible_travel_count,
        COUNT(CASE WHEN is_amount_spike THEN 1 END) AS amount_spike_count,
        COUNT(CASE WHEN is_off_hours THEN 1 END) AS off_hours_count,
        COUNT(CASE WHEN is_card_testing THEN 1 END) AS card_testing_count
    FROM customer_data
    GROUP BY customer_id
)
SELECT
    cd.customer_id,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN cd.decision != 'APPROVE' THEN 1 END) AS fraud_transaction_count,
    SUM(CASE WHEN cd.decision = 'BLOCK' THEN cd.amount ELSE 0 END) AS total_amount_blocked,
    MAX(cd.risk_score) AS max_risk_score,
    ROUND(AVG(cd.risk_score)::NUMERIC, 2) AS avg_risk_score,
    CASE
        WHEN rc.velocity_count >= rc.impossible_travel_count AND rc.velocity_count >= rc.amount_spike_count 
             AND rc.velocity_count >= rc.off_hours_count AND rc.velocity_count >= rc.card_testing_count THEN 'VELOCITY'
        WHEN rc.impossible_travel_count >= rc.velocity_count AND rc.impossible_travel_count >= rc.amount_spike_count 
             AND rc.impossible_travel_count >= rc.off_hours_count AND rc.impossible_travel_count >= rc.card_testing_count THEN 'IMPOSSIBLE_TRAVEL'
        WHEN rc.amount_spike_count >= rc.velocity_count AND rc.amount_spike_count >= rc.impossible_travel_count 
             AND rc.amount_spike_count >= rc.off_hours_count AND rc.amount_spike_count >= rc.card_testing_count THEN 'AMOUNT_SPIKE'
        WHEN rc.off_hours_count >= rc.velocity_count AND rc.off_hours_count >= rc.impossible_travel_count 
             AND rc.off_hours_count >= rc.amount_spike_count AND rc.off_hours_count >= rc.card_testing_count THEN 'OFF_HOURS'
        ELSE 'CARD_TESTING'
    END AS most_triggered_rule,
    CASE
        WHEN AVG(cd.risk_score) > 50 THEN 'HIGH'
        WHEN AVG(cd.risk_score) > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_label
FROM customer_data cd
LEFT JOIN rule_counts rc ON cd.customer_id = rc.customer_id
GROUP BY cd.customer_id, rc.velocity_count, rc.impossible_travel_count, rc.amount_spike_count, rc.off_hours_count, rc.card_testing_count
