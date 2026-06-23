import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, from_json, schema_of_json, current_timestamp,
    when, sum as spark_sum, count, avg, stddev,
    window, row_number, radians, sin, cos, asin, sqrt,
    hour, dayofweek, date_format, array, concat_ws, unix_timestamp,
    lag, max as spark_max
)
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql.functions import lit

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_TRANSACTIONS', 'transactions')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'fraud_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'fraud_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'fraud_pass')

# Load fraud rules config
RULES_CONFIG_PATH = os.getenv('RULES_CONFIG_PATH', '/app/config/fraud_rules.yaml')


def load_rules_config():
    """Load fraud rules configuration from YAML file."""
    try:
        with open(RULES_CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded fraud rules config from {RULES_CONFIG_PATH}")
            return config
    except FileNotFoundError:
        logger.warning(f"Config file not found at {RULES_CONFIG_PATH}, using defaults")
        return {
            'rules': {
                'velocity': {'enabled': True, 'score': 40, 'max_transactions': 5, 'window_seconds': 60},
                'impossible_travel': {'enabled': True, 'score': 50, 'max_speed_kmh': 900},
                'amount_spike': {'enabled': True, 'score': 30, 'z_score_threshold': 2.5, 'fallback_amount': 10000000},
                'off_hours': {'enabled': True, 'score': 20, 'start_hour': 1, 'end_hour': 5},
                'card_testing': {'enabled': True, 'score': 35, 'min_micro_count': 3, 'micro_amount': 10000, 'large_amount': 1000000, 'window_seconds': 600},
            },
            'decision': {'block_threshold': 70, 'review_threshold': 40}
        }


RULES_CONFIG = load_rules_config()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FraudDetectionEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session initialized")


def get_postgres_connection():
    """Create PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=int(POSTGRES_PORT),
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def load_customer_baseline():
    """Load customer baseline (avg_amount, std_amount) from PostgreSQL."""
    try:
        logger.info("Loading customer_baseline from PostgreSQL...")
        baseline_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", "public.mart_customer_baseline") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        logger.info("Customer baseline loaded successfully")
        return baseline_df
    except Exception as e:
        logger.warning(f"Could not load customer_baseline (first run?): {e}")
        return None


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate haversine distance between two points (in kilometers).
    """
    R = 6371  # Earth radius in km
    
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * asin(sqrt(a))
    
    return R * c


def write_to_postgres(batch_df, batch_id, table_name):
    """Write batch data to PostgreSQL."""
    try:
        # Convert to Pandas for insertion (avoid triggering count() action)
        pandas_df = batch_df.toPandas()
        
        if pandas_df.empty:
            logger.info(f"Batch {batch_id} is empty for {table_name}")
            return
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        if table_name == 'raw_transactions':
            columns = [
                'transaction_id', 'customer_id', 'name', 'merchant',
                'amount', 'location', 'lat', 'lon', 'card_type', 'timestamp'
            ]
            values = [
                (row['transaction_id'], row['customer_id'], row['name'], row['merchant'],
                 float(row['amount']), row['location'], float(row['lat']), float(row['lon']),
                 row['card_type'], row['timestamp'])
                for _, row in pandas_df.iterrows()
            ]
            
            insert_query = f"""
                INSERT INTO raw_transactions ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (transaction_id) DO NOTHING
            """
            
        elif table_name == 'fraud_flags':
            columns = [
                'transaction_id', 'customer_id', 'amount', 'risk_score',
                'decision', 'is_velocity', 'is_impossible_travel',
                'is_amount_spike', 'is_off_hours', 'is_card_testing', 'reasons'
            ]
            values = [
                (row['transaction_id'], row['customer_id'], float(row['amount']),
                 int(row['risk_score']), row['decision'], row['is_velocity'],
                 row['is_impossible_travel'], row['is_amount_spike'],
                 row['is_off_hours'], row['is_card_testing'],
                 [
                    reason for reason, is_active in [
                        ('VELOCITY_EXCEEDED', row['is_velocity']),
                        ('IMPOSSIBLE_TRAVEL', row['is_impossible_travel']),
                        ('AMOUNT_SPIKE', row['is_amount_spike']),
                        ('OFF_HOURS', row['is_off_hours']),
                        ('CARD_TESTING', row['is_card_testing'])
                    ] if is_active
                 ])
                for _, row in pandas_df.iterrows()
            ]
            
            insert_query = f"""
                INSERT INTO fraud_flags ({', '.join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
            """
        
        if values:
            execute_values(cursor, insert_query, values)
            conn.commit()
            logger.info(f"Inserted {len(values)} rows into {table_name} (batch {batch_id})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}")


def score_transactions(batch_df, customer_baseline_df=None):
    """Apply fraud rules to a micro-batch using batch DataFrame operations.
    
    Args:
        batch_df: Transaction batch
        customer_baseline_df: Customer baseline for Z-score calculation (optional)
    """
    # Load rule thresholds from config
    rules = RULES_CONFIG.get('rules', {})
    decision_cfg = RULES_CONFIG.get('decision', {})

    velocity_cfg = rules.get('velocity', {})
    travel_cfg = rules.get('impossible_travel', {})
    spike_cfg = rules.get('amount_spike', {})
    hours_cfg = rules.get('off_hours', {})
    card_cfg = rules.get('card_testing', {})

    batch_df = batch_df.withColumn(
        "timestamp", col("timestamp").cast("timestamp")
    ).withColumn(
        "event_time_ms",
        (unix_timestamp(col("timestamp")) * 1000).cast("long")
    )

    # Join with customer baseline if available for Z-score calculation
    if customer_baseline_df is not None:
        batch_df = batch_df.join(
            customer_baseline_df,
            on="customer_id",
            how="left"
        )
    else:
        batch_df = batch_df.withColumn("avg_amount", lit(None).cast("double")) \
                      .withColumn("std_amount", lit(None).cast("double"))

    velocity_window_ms = velocity_cfg.get('window_seconds', 60) * 1000
    velocity_window = Window \
        .partitionBy("customer_id") \
        .orderBy(col("event_time_ms")) \
        .rangeBetween(-velocity_window_ms, 0)

    sequence_window = Window \
        .partitionBy("customer_id") \
        .orderBy(col("event_time_ms"))

    card_window_ms = card_cfg.get('window_seconds', 600) * 1000
    micro_window = Window \
        .partitionBy("customer_id") \
        .orderBy(col("event_time_ms")) \
        .rangeBetween(-card_window_ms, 0)

    # Rule 1: Velocity Check
    max_txns = velocity_cfg.get('max_transactions', 5)
    scored_df = batch_df.withColumn(
        "velocity_count",
        count("*").over(velocity_window)
    ).withColumn(
        "is_velocity",
        when(col("velocity_count") > max_txns, True).otherwise(False)
    )

    # Rule 2: Impossible Travel
    max_speed = travel_cfg.get('max_speed_kmh', 900)
    scored_df = scored_df.withColumn(
        "prev_lat",
        lag("lat").over(sequence_window)
    ).withColumn(
        "prev_lon",
        lag("lon").over(sequence_window)
    ).withColumn(
        "prev_event_time_ms",
        lag("event_time_ms").over(sequence_window)
    ).withColumn(
        "time_diff_hours",
        (col("event_time_ms") - col("prev_event_time_ms")) / 3600000.0
    ).withColumn(
        "distance_km",
        when(
            col("prev_lat").isNull() | col("prev_lon").isNull() | col("prev_event_time_ms").isNull(),
            0.0
        ).otherwise(haversine_distance(col("prev_lat"), col("prev_lon"), col("lat"), col("lon")))
    ).withColumn(
        "speed_kmh",
        when(col("time_diff_hours") > 0, col("distance_km") / col("time_diff_hours")).otherwise(0.0)
    ).withColumn(
        "is_impossible_travel",
        when(col("speed_kmh") > max_speed, True).otherwise(False)
    )

    # Rule 3: Amount Spike (Z-score based with fallback)
    z_threshold = spike_cfg.get('z_score_threshold', 2.5)
    fallback_amount = spike_cfg.get('fallback_amount', 10_000_000)
    scored_df = scored_df.withColumn(
        "z_score_amount",
        when(
            (col("std_amount").isNotNull()) & (col("std_amount") > 0),
            (col("amount") - col("avg_amount")) / col("std_amount")
        ).otherwise(None)
    ).withColumn(
        "is_amount_spike",
        when(
            col("z_score_amount").isNotNull() & (col("z_score_amount") > z_threshold),
            True
        ).when(
            col("z_score_amount").isNull() & (col("amount") > fallback_amount),
            True
        ).otherwise(False)
    )

    # Rule 4: Off-Hours
    start_hour = hours_cfg.get('start_hour', 1)
    end_hour = hours_cfg.get('end_hour', 5)
    scored_df = scored_df.withColumn(
        "is_off_hours",
        when((hour(col("timestamp")) >= start_hour) & (hour(col("timestamp")) < end_hour), True).otherwise(False)
    )

    # Rule 5: Card Testing
    micro_amount = card_cfg.get('micro_amount', 10_000)
    large_amount = card_cfg.get('large_amount', 1_000_000)
    min_micro = card_cfg.get('min_micro_count', 3)
    scored_df = scored_df.withColumn(
        "micro_count",
        count(when(col("amount") < micro_amount, 1)).over(micro_window)
    ).withColumn(
        "max_amount_10min",
        spark_max(col("amount")).over(micro_window)
    ).withColumn(
        "is_card_testing",
        when((col("micro_count") >= min_micro) & (col("max_amount_10min") > large_amount), True).otherwise(False)
    )

    # Composite scoring
    velocity_score = velocity_cfg.get('score', 40)
    travel_score = travel_cfg.get('score', 50)
    spike_score = spike_cfg.get('score', 30)
    hours_score = hours_cfg.get('score', 20)
    card_score = card_cfg.get('score', 35)

    risk_score = (
        when(col("is_velocity"), velocity_score).otherwise(0) +
        when(col("is_impossible_travel"), travel_score).otherwise(0) +
        when(col("is_amount_spike"), spike_score).otherwise(0) +
        when(col("is_off_hours"), hours_score).otherwise(0) +
        when(col("is_card_testing"), card_score).otherwise(0)
    )

    block_threshold = decision_cfg.get('block_threshold', 70)
    review_threshold = decision_cfg.get('review_threshold', 40)

    decision = (
        when(risk_score >= block_threshold, "BLOCK")
        .when(risk_score >= review_threshold, "REVIEW")
        .otherwise("APPROVE")
    )

    return scored_df.withColumn("risk_score", risk_score).withColumn("decision", decision)


def main():
    """Main Spark Streaming job."""
    
    try:
        # Step 0: Load customer baseline for Z-score calculation
        logger.info("Initializing customer baseline...")
        customer_baseline_df = load_customer_baseline()
        if customer_baseline_df is not None:
            customer_baseline_df.cache()
            logger.info("Customer baseline cached for micro-batch joins")
        
        # Step A: Read from Kafka
        logger.info(f"Reading from Kafka topic: {KAFKA_TOPIC}")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Step B: Parse JSON schema
        sample_json = """{
            "transaction_id": "123",
            "customer_id": "CUST_001",
            "name": "John Doe",
            "merchant": "Indomaret",
            "amount": 100000.0,
            "location": "Jakarta",
            "lat": -6.2088,
            "lon": 106.8456,
            "card_type": "VISA",
            "timestamp": "2024-01-01T00:00:00"
        }"""
        
        schema = schema_of_json(sample_json)
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        parsed_df = parsed_df.withColumn("timestamp", 
            col("timestamp").cast("timestamp"))
        
        # Step C: Apply fraud scoring inside foreachBatch so the micro-batch can use batch-only window logic.
        def process_batch(batch_df, batch_id):
            fraud_scored_df = score_transactions(batch_df, customer_baseline_df).select(
                col("transaction_id"),
                col("customer_id"),
                col("name"),
                col("merchant"),
                col("amount"),
                col("location"),
                col("lat"),
                col("lon"),
                col("card_type"),
                col("timestamp"),
                col("risk_score"),
                col("decision"),
                col("is_velocity"),
                col("is_impossible_travel"),
                col("is_amount_spike"),
                col("is_off_hours"),
                col("is_card_testing")
            )

            raw_batch_df = fraud_scored_df.select(
                col("transaction_id"),
                col("customer_id"),
                col("name"),
                col("merchant"),
                col("amount"),
                col("location"),
                col("lat"),
                col("lon"),
                col("card_type"),
                col("timestamp")
            )
            write_to_postgres(raw_batch_df, batch_id, "raw_transactions")

            flagged_df = fraud_scored_df.filter(col("risk_score") >= 40).select(
                col("transaction_id"),
                col("customer_id"),
                col("amount"),
                col("risk_score"),
                col("decision"),
                col("is_velocity"),
                col("is_impossible_travel"),
                col("is_amount_spike"),
                col("is_off_hours"),
                col("is_card_testing")
            )
            write_to_postgres(flagged_df, batch_id, "fraud_flags")

        # Step D: Sink to PostgreSQL
        logger.info("Setting up streaming sink...")

        query_raw = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", "/opt/spark/work-dir/checkpoint_raw") \
            .start()
        
        logger.info("Streaming queries started. Waiting for queries to terminate...")
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == '__main__':
    logger.info("Starting Fraud Detection Engine...")
    main()
