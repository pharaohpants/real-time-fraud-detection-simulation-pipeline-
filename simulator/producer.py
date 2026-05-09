import os
import json
import uuid
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_TRANSACTIONS', 'transactions')
SIMULATOR_INTERVAL = float(os.getenv('SIMULATOR_INTERVAL', 1.0))

# Initialize Faker with Indonesian locale
fake = Faker('id_ID')

# Constant pools for realistic data
MERCHANTS = [
    'Indomaret', 'Alfamart', 'Tokopedia', 'Shopee', 'GoPay',
    'OVO', 'Dana', 'BCA Mobile', 'Traveloka', 'Grab',
    'GoFood', 'Lazada', 'Blibli', 'BPJS', 'PLN'
]

CITIES = {
    'Surabaya': {'lat': -7.2575, 'lon': 112.7521},
    'Jakarta': {'lat': -6.2088, 'lon': 106.8456},
    'Bandung': {'lat': -6.9175, 'lon': 107.6191},
    'Medan': {'lat': 3.5952, 'lon': 98.6722},
    'Semarang': {'lat': -6.9667, 'lon': 110.4167},
    'Makassar': {'lat': -5.1477, 'lon': 119.4327},
    'Yogyakarta': {'lat': -7.7956, 'lon': 110.3695},
    'Denpasar': {'lat': -8.6705, 'lon': 115.2126},
    'Balikpapan': {'lat': -1.2329, 'lon': 116.8355},
    'Malang': {'lat': -7.9827, 'lon': 112.6345}
}

CARD_TYPES = ['VISA', 'MASTERCARD', 'GPN', 'JCB']

# Pool of 20 customer IDs for pattern generation
CUSTOMER_POOL = [f'CUST_{i:04d}' for i in range(1, 21)]


def generate_transaction():
    """Generate a single transaction record."""
    try:
        customer_id = random.choice(CUSTOMER_POOL)
        city = random.choice(list(CITIES.keys()))
        coords = CITIES[city]
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'customer_id': customer_id,
            'name': fake.name(),
            'merchant': random.choice(MERCHANTS),
            'amount': round(random.uniform(10_000, 50_000_000), 2),
            'location': city,
            'lat': coords['lat'],
            'lon': coords['lon'],
            'card_type': random.choice(CARD_TYPES),
            'timestamp': datetime.now().isoformat()
        }
        return transaction
    except Exception as e:
        logger.error(f"Error generating transaction: {e}")
        return None


def main():
    """Main producer loop."""
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            request_timeout_ms=10000
        )
        logger.info(f"Successfully connected to Kafka. Topic: {KAFKA_TOPIC}")
        
        transaction_count = 0
        while True:
            try:
                tx = generate_transaction()
                if tx:
                    producer.send(KAFKA_TOPIC, value=tx)
                    transaction_count += 1
                    logger.info(
                        f"[SENT] {tx['transaction_id'][:8]}... | "
                        f"{tx['customer_id']} | "
                        f"Rp {tx['amount']:,.0f} | "
                        f"{tx['merchant']}"
                    )
                
                time.sleep(SIMULATOR_INTERVAL)
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(5)  # Wait before retry
                
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()


if __name__ == '__main__':
    logger.info("Starting Transaction Simulator...")
    main()
