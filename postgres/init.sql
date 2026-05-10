-- Tabel 1: raw_transactions (sink dari Spark Streaming)
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id   VARCHAR(36) PRIMARY KEY,
    customer_id      VARCHAR(36) NOT NULL,
    name             VARCHAR(100),
    merchant         VARCHAR(100),
    amount           NUMERIC(15, 2) NOT NULL,
    location         VARCHAR(50),
    lat              NUMERIC(9, 6),
    lon              NUMERIC(9, 6),
    card_type        VARCHAR(20),
    timestamp        TIMESTAMP NOT NULL,
    ingested_at      TIMESTAMP DEFAULT NOW()
);

-- Tabel 2: fraud_flags (hasil scoring dari Spark)
CREATE TABLE IF NOT EXISTS fraud_flags (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(36) NOT NULL UNIQUE,
    customer_id      VARCHAR(36) NOT NULL,
    amount           NUMERIC(15, 2),
    risk_score       INTEGER NOT NULL,
    decision         VARCHAR(10) NOT NULL CHECK (decision IN ('APPROVE','REVIEW','BLOCK')),
    reasons          TEXT[],
    is_velocity      BOOLEAN DEFAULT FALSE,
    is_impossible_travel BOOLEAN DEFAULT FALSE,
    is_amount_spike  BOOLEAN DEFAULT FALSE,
    is_off_hours     BOOLEAN DEFAULT FALSE,
    is_card_testing  BOOLEAN DEFAULT FALSE,
    processed_at     TIMESTAMP DEFAULT NOW()
);

-- Index untuk query performa Grafana
CREATE INDEX idx_fraud_flags_processed_at ON fraud_flags(processed_at);
CREATE INDEX idx_fraud_flags_customer_id ON fraud_flags(customer_id);
CREATE INDEX idx_raw_transactions_timestamp ON raw_transactions(timestamp);
-- Tabel untuk tracking Bronze/Silver/Gold layer
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
