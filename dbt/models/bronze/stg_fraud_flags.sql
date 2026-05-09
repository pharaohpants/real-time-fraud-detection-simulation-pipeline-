-- Bronze: copy 1:1 dari tabel fraud_flags
SELECT
    id,
    transaction_id,
    customer_id,
    amount,
    risk_score,
    decision,
    reasons,
    is_velocity,
    is_impossible_travel,
    is_amount_spike,
    is_off_hours,
    is_card_testing,
    processed_at
FROM {{ source('fraud_db', 'fraud_flags') }}
