-- Bronze: copy 1:1 dari tabel raw, tidak ada transformasi
SELECT
    transaction_id,
    customer_id,
    merchant,
    amount,
    location,
    lat,
    lon,
    card_type,
    timestamp,
    ingested_at
FROM {{ source('fraud_db', 'raw_transactions') }}
