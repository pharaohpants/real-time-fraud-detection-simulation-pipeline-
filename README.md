# Real-Time Fraud Detection Pipeline 🚀

## Project Overview

Sistem pipeline real-time untuk deteksi fraud pada transaksi finansial menggunakan teknologi modern data engineering. Pipeline ini menghasilkan transaksi secara sintetis dengan Faker, memproses stream data menggunakan Kafka dan Spark, menyimpan ke PostgreSQL, melakukan transformasi dengan dbt, validasi data dengan Great Expectations, dan visualisasi real-time di Grafana.

**Fitur utama:**
- Simulasi transaksi real-time dengan 20 customer pool tetap
- 5 fraud detection rules dengan composite risk scoring
- Streaming data processing menggunakan Apache Spark
- Data transformation layer: Bronze → Silver → Gold
- Data quality checks otomatis dengan Great Expectations
- Orchestration dengan Apache Airflow
- Real-time monitoring dashboard dengan Grafana

---
## Workflow 

<img width="4611" height="3976" alt="image" src="https://github.com/user-attachments/assets/c4b1ac74-6a7f-4c99-b03f-2956dd60baf9" />


## Tech Stack

| Layer | Technology | Versi |
|---|---|---|
| Simulasi Data | Python 3.11 + Faker | 24.0.0 |
| Message Broker | Apache Kafka | 7.5.0 |
| Stream Processing | Apache Spark | 3.5.0 |
| Database | PostgreSQL | 15 |
| Data Transformation | dbt-core | 1.x |
| Orchestration | Apache Airflow | 2.7.0 |
| Visualization | Grafana | 10.0.0 |
| Containerization | Docker + Docker Compose | Latest |

---

## Project Structure

```
fraud-pipeline/
├── docker-compose.yml          # Orkestrasi semua services
├── .env                        # Environment variables
├── .env.example                # Template environment variables
├── .gitignore                  # Git ignore rules
│
├── simulator/                  # Transaction data generator
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py            # Kafka producer
│
├── spark/                      # Stream processing engine
│   ├── Dockerfile
│   ├── requirements.txt
│   └── fraud_engine.py        # 5 fraud rules + scoring
│
├── dbt/                        # Data transformation
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/            # Raw data copy
│       │   ├── stg_raw_transactions.sql
│       │   ├── stg_fraud_flags.sql
│       │   └── sources.yml
│       ├── silver/            # Data enrichment
│       │   └── int_transactions_enriched.sql
│       └── gold/              # Business metrics (4 marts)
│           ├── mart_fraud_summary.sql
│           ├── mart_customer_risk.sql
│           ├── mart_daily_volume.sql
│           ├── mart_rule_performance.sql
│           └── schema.yml
│
├── great_expectations/        # Data quality framework
│   ├── great_expectations.yml
│   ├── expectations_definitions.py
│   └── validate_data.py
│
├── airflow/                    # Workflow orchestration
│   └── dags/
│       └── fraud_pipeline_dag.py
│
├── grafana/                    # Monitoring dashboard
│   └── dashboards/
│       └── fraud_dashboard.json
│
├── postgres/                   # Database initialization
│   └── init.sql               # Schema + tables
│
└── README.md                   # Dokumentasi ini
```

---

## Quick Start

### 1. Clone dan setup project

```bash
git clone <your-repo-url>
cd fraud-pipeline

# Copy environment template (jika ada)
cp .env.example .env  # atau setup .env sesuai konfigurasi
```

### 2. Start all services

```bash
docker-compose up -d

# Verifikasi semua service running
docker-compose ps
```

### 3. Monitor logs

```bash
# Monitor simulator (transaksi masuk)
docker-compose logs -f simulator

# Monitor Spark (fraud detection)
docker-compose logs -f spark

# Monitor PostgreSQL (data masuk)
docker-compose logs -f postgres
```

### 4. Akses interfaces

| Service | URL | Credentials |
|---|---|---|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| Airflow UI | http://localhost:8080 | airflow / airflow |
| PostgreSQL | localhost:5432 | fraud_user / fraud_pass |

---

## Fraud Detection Rules

Pipeline menggunakan 5 fraud rules dengan composite scoring (0-200 total score):

### 1. **Velocity Check** ⚡
- **Score:** 40 points
- **Rule:** > 5 transaksi dari customer yang sama dalam 1 menit
- **Use Case:** Deteksi suspicious account access atau card cloning

### 2. **Impossible Travel** 🌍
- **Score:** 50 points (tertinggi)
- **Rule:** Transaksi dari lokasi berbeda dengan jarak > 900 km/jam
- **Use Case:** Deteksi penggunaan stolen card dari lokasi tidak mungkin

### 3. **Amount Spike** 💰
- **Score:** 30 points
- **Rule:** Transaksi > Rp 10M dari customer dengan historical average rendah
- **Use Case:** Deteksi transaksi abnormal atau compromised account

### 4. **Off-Hours Transaction** 🌙
- **Score:** 20 points
- **Rule:** Transaksi antara jam 01:00 - 04:00
- **Use Case:** Transaksi di waktu jarang normal

### 5. **Card Testing** 🧪
- **Score:** 35 points
- **Rule:** 3+ micro-transactions (< Rp 10k) diikuti transaksi besar
- **Use Case:** Deteksi fraudster testing stolen card

### Decision Logic

```
Risk Score >= 70  → BLOCK (reject transaksi)
Risk Score >= 40  → REVIEW (manual review required)
Risk Score < 40   → APPROVE (proses normal)
```

---

## Data Pipeline Layers

### Bronze Layer (Raw Copy)
- Copy 1:1 dari `raw_transactions` dan `fraud_flags`
- Materialized sebagai VIEW (bukan storage)
- Purpose: Staging area untuk audit trail

### Silver Layer (Enrichment)
- Join raw transactions dengan fraud flags
- Tambah derived columns:
  - `hour_of_day`, `day_of_week`, `is_weekend`
  - `amount_bucket` (micro/small/medium/large)
  - `has_fraud_flag`, `risk_score`, `fraud_decision`
- Materialized sebagai TABLE

### Gold Layer (Business Metrics) - 4 Marts

**1. mart_fraud_summary**
- Aggregasi: Per jam + merchant
- Metrics: fraud_count, block_count, fraud_rate_pct, avg_risk_score

**2. mart_customer_risk**
- Per customer profile
- Metrics: total_txs, fraud_txs, risk_label (HIGH/MEDIUM/LOW)

**3. mart_daily_volume**
- Daily metrics
- Metrics: total_txs, total_amount, fraud_rate_pct, unique_customers

**4. mart_rule_performance**
- Fraud rule effectiveness
- Metrics: trigger_count, trigger_pct, avg_risk_score_when_triggered

---

## Data Quality Check

### `raw_transactions.warning` (Updated)

```python
# Not null validation
✓ transaction_id NOT NULL
✓ customer_id NOT NULL
✓ amount NOT NULL
✓ timestamp NOT NULL

# Range validation
✓ amount BETWEEN 0 dan 100M
✓ lat BETWEEN -11 dan 6 (Indonesia bounds)
✓ lon BETWEEN 95 dan 141 (Indonesia bounds)

# Categorical validation
✓ card_type IN ['VISA', 'MASTERCARD', 'GPN', 'JCB']

# Uniqueness
✓ transaction_id UNIQUE
```

### `fraud_flags.warning` (Updated)

```python
# Not null validation
✓ transaction_id NOT NULL
✓ risk_score NOT NULL

# Range validation
✓ risk_score BETWEEN 0 dan 200

# Categorical validation
✓ decision IN ['APPROVE', 'REVIEW', 'BLOCK']
```
## Data Quality Checks

Script `great_expectations/data_quality_checks.py` mendukung layer-based validation:

### Raw Layer Validation
Mengecek `raw_transactions` dan `fraud_flags` tables:

```bash
python data_quality_checks.py --layer raw
```

Checks:
- **raw_transactions**: NOT NULL, amount range (0-100M), valid card types, uniqueness
- **fraud_flags**: NOT NULL, risk_score range (0-200), valid decision values

### Gold Layer Validation
Mengecek gold layer marts (mart_fraud_summary, mart_customer_risk, mart_daily_volume, mart_rule_performance):

```bash
python data_quality_checks.py --layer gold
```

Checks:
- **mart_fraud_summary**: Row count, fraud_count NOT NULL, fraud_rate_pct valid (0-100%)
- **mart_customer_risk**: Row count, total_txs > 0, risk_label valid (HIGH/MEDIUM/LOW)
- **mart_daily_volume**: Row count, total_txs NOT NULL
- **mart_rule_performance**: Row count, trigger_count NOT NULL

### Run All Layers
```bash
python data_quality_checks.py  # Default: both layers
python data_quality_checks.py --layer both
```

---

## Airflow DAG

DAG runs **@daily** dengan 8 sequential tasks:

<img width="1898" height="605" alt="image" src="https://github.com/user-attachments/assets/8a681403-31b1-4f53-91b4-6216e0414b35" />

```
validate_raw_data (Great Expectations)
    ↓
run_dbt_bronze (materialisasi Bronze layer)
    ↓
run_dbt_silver (materialisasi Silver layer)
    ↓
run_dbt_gold (materialisasi 4 Marts)
    ↓
run_dbt_tests (dbt generic tests)
    ↓
validate_gold_data (Great Expectations pada marts)
    ↓
notify_success (log success message)
```

**Cara trigger manual:**
1. Buka http://localhost:8080
2. Login: airflow / airflow
3. Cari DAG: `fraud_pipeline_daily`
4. Klik "Trigger DAG" button

---

## Grafana Dashboard

Real-time monitoring dengan 5 panels (auto-refresh setiap 5 detik):
<img width="1919" height="671" alt="image" src="https://github.com/user-attachments/assets/6ee6b6e1-a39f-4c36-99e4-aa9ffd80d380" />


### Panel 1: Fraud Rate Per Minute (Time Series)
- Metrik: fraud_rate_pct dari fraud_flags per menit
- Time range: Last 1 hour
- Type: Time series line chart

### Panel 2: Top 10 Risky Merchants (Bar Gauge)
- Top 10 merchant dengan fraud_count tertinggi
- Time range: Last 24 hours
- Color threshold: Green (< 5), Yellow (5-10), Red (> 10)

### Panel 3: Decision Breakdown (Pie Chart)
- Breakdown: APPROVE / REVIEW / BLOCK count
- Time range: Last 24 hours

### Panel 4: Today's Transactions (Stat Card)
- Total transaksi hari ini
- Single number

### Panel 5: Today's Fraud Rate % (Stat Card)
- Fraud rate percentage hari ini
- Color threshold: Green (< 5%), Yellow (5-10%), Red (> 10%)

**Akses:** http://localhost:3000 (admin / admin)

---

## Common Operations

### Monitoring Data Flow

```bash
# Check simulator sending data
docker-compose logs simulator | grep SENT

# Check Spark processing
## Airflow DAG (Updated)
docker-compose logs spark | grep "Inserted"

# Verify data in PostgreSQL
docker exec fraud-pipeline-postgres-1 psql -U fraud_user -d fraud_db -c \
  "SELECT COUNT(*) FROM raw_transactions; SELECT COUNT(*) FROM fraud_flags;"
```

### Running dbt Manually

```bash
# Build all models
docker exec fraud-pipeline-spark-1 bash -c "cd /dbt && dbt run"

# Test all models
docker exec fraud-pipeline-spark-1 bash -c "cd /dbt && dbt test"

# Generate docs
docker exec fraud-pipeline-spark-1 bash -c "cd /dbt && dbt docs generate"
```

### Database Queries

```bash
# Connect ke PostgreSQL
docker exec -it fraud-pipeline-postgres-1 psql -U fraud_user -d fraud_db

# Query raw data
SELECT * FROM raw_transactions LIMIT 10;

# Query fraud flags
SELECT * FROM fraud_flags LIMIT 10;

# Query gold models
SELECT * FROM gold.mart_fraud_summary LIMIT 10;
SELECT * FROM gold.mart_customer_risk LIMIT 10;
```

### Data Quality Checks

```bash
# Run validation for raw layer (raw_transactions & fraud_flags)
docker exec fraud-pipeline-airflow-scheduler-1 bash -c \
    "cd /opt/airflow/great_expectations && python data_quality_checks.py --layer raw"

# Run validation for gold layer (all marts)
docker exec fraud-pipeline-airflow-scheduler-1 bash -c \
    "cd /opt/airflow/great_expectations && python data_quality_checks.py --layer gold"

# Run validation for both layers
docker exec fraud-pipeline-airflow-scheduler-1 bash -c \
    "cd /opt/airflow/great_expectations && python data_quality_checks.py"
```

### Scaling Considerations

- **Simulator:** Ubah `SIMULATOR_INTERVAL` di .env untuk adjust frequency
- **Spark parallelism:** Ubah `SPARK_MASTER=local[*]` di .env
- **Kafka partitions:** Default 1, increase untuk throughput lebih tinggi
- **PostgreSQL connections:** Adjust di docker-compose.yml

---

## Troubleshooting

| Issue | Solution |
|---|---|
| Services tidak start | Check port availability (`netstat -an`) |
| Simulator tidak terhubung Kafka | Verify `KAFKA_BOOTSTRAP_SERVERS` di .env |
| Spark job error | Check Spark logs: `docker-compose logs spark` |
| dbt run gagal | Verify PostgreSQL credentials + connectivity |
| Grafana dashboard kosong | Verify PostgreSQL datasource di Grafana UI |
| Airflow DAG tidak trigger | Check Airflow scheduler running |

---

## Performance Metrics

Typical performance dengan simulator 1 transaksi/detik:

| Metric | Value |
|---|---|
| Transactions/hour | ~3,600 |
| Fraud flags/hour | ~1,800 (50% flagged) |
| Spark latency | < 5 seconds |
| PostgreSQL insert throughput | 500-1,000 rows/sec |
| Grafana refresh rate | 5 seconds |
| DAG daily runtime | ~5-10 minutes |

---


