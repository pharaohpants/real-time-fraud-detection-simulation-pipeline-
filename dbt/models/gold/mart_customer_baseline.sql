{{
  config(
    materialized='table',
    schema='public'
  )
}}

-- Customer baseline: historical average and std dev for Z-score calculation
SELECT
    customer_id,
    ROUND(AVG(amount)::numeric, 2) as avg_amount,
    ROUND(STDDEV_POP(amount)::numeric, 2) as std_amount,
    COUNT(*) as transaction_count,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_raw_transactions') }}
WHERE amount > 0  -- Exclude zero/negative amounts
GROUP BY customer_id
