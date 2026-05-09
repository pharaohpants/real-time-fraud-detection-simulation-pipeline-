"""
Data quality validation runner for fraud pipeline
"""
import os
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'fraud_db'),
    'user': os.getenv('POSTGRES_USER', 'fraud_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'fraud_pass')
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_raw_transactions():
    """Validate raw_transactions table"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        logger.info("Validating raw_transactions table...")
        
        validations = [
            ("transaction_id NOT NULL", 
             "SELECT COUNT(*) FROM raw_transactions WHERE transaction_id IS NULL"),
            ("customer_id NOT NULL", 
             "SELECT COUNT(*) FROM raw_transactions WHERE customer_id IS NULL"),
            ("amount NOT NULL", 
             "SELECT COUNT(*) FROM raw_transactions WHERE amount IS NULL"),
            ("amount BETWEEN 0 AND 100M", 
             "SELECT COUNT(*) FROM raw_transactions WHERE amount < 0 OR amount > 100000000"),
            ("valid card types", 
             "SELECT COUNT(*) FROM raw_transactions WHERE card_type NOT IN ('VISA','MASTERCARD','GPN','JCB')"),
        ]
        
        results = {}
        for check_name, query in validations:
            cursor.execute(query)
            result = cursor.fetchone()
            invalid_count = result['count']
            results[check_name] = invalid_count == 0
            logger.info(f"  {check_name}: {'✓' if results[check_name] else '✗'} ({invalid_count} issues)")
        
        cursor.close()
        conn.close()
        
        return all(results.values())
        
    except Exception as e:
        logger.error(f"Error validating raw_transactions: {e}")
        return False


def validate_fraud_flags():
    """Validate fraud_flags table"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        logger.info("Validating fraud_flags table...")
        
        validations = [
            ("transaction_id NOT NULL", 
             "SELECT COUNT(*) FROM fraud_flags WHERE transaction_id IS NULL"),
            ("risk_score NOT NULL", 
             "SELECT COUNT(*) FROM fraud_flags WHERE risk_score IS NULL"),
            ("risk_score BETWEEN 0 AND 200", 
             "SELECT COUNT(*) FROM fraud_flags WHERE risk_score < 0 OR risk_score > 200"),
            ("valid decision values", 
             "SELECT COUNT(*) FROM fraud_flags WHERE decision NOT IN ('APPROVE','REVIEW','BLOCK')"),
        ]
        
        results = {}
        for check_name, query in validations:
            cursor.execute(query)
            result = cursor.fetchone()
            invalid_count = result['count']
            results[check_name] = invalid_count == 0
            logger.info(f"  {check_name}: {'✓' if results[check_name] else '✗'} ({invalid_count} issues)")
        
        cursor.close()
        conn.close()
        
        return all(results.values())
        
    except Exception as e:
        logger.error(f"Error validating fraud_flags: {e}")
        return False


if __name__ == '__main__':
    logger.info("Starting data quality validation...")
    
    raw_tx_valid = validate_raw_transactions()
    fraud_flags_valid = validate_fraud_flags()
    
    if raw_tx_valid and fraud_flags_valid:
        logger.info("✓ All data quality checks passed!")
    else:
        logger.warning("✗ Some data quality checks failed")
