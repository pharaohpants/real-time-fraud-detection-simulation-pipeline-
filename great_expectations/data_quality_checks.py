"""
Data quality validation runner for fraud pipeline.

Supports layer-based validation:
- raw: Validate raw_transactions and fraud_flags tables
- gold: Validate gold layer marts (mart_fraud_summary, mart_customer_risk, etc.)

Usage:
    python data_quality_checks.py --layer raw
    python data_quality_checks.py --layer gold
    python data_quality_checks.py  # Default: both layers
"""
import os
import sys
import logging
import argparse
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
    """Validate raw_transactions table."""
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
            logger.info(f"  {check_name}: {'OK' if results[check_name] else 'FAIL'} ({invalid_count} issues)")

        cursor.close()
        conn.close()

        return all(results.values())

    except Exception as e:
        logger.error(f"Error validating raw_transactions: {e}")
        return False


def validate_fraud_flags():
    """Validate fraud_flags table."""
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
            logger.info(f"  {check_name}: {'OK' if results[check_name] else 'FAIL'} ({invalid_count} issues)")

        cursor.close()
        conn.close()

        return all(results.values())

    except Exception as e:
        logger.error(f"Error validating fraud_flags: {e}")
        return False




def validate_gold_marts():
    """Validate gold layer marts."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        logger.info("Validating gold layer marts...")
        all_valid = True

        # Validate mart_fraud_summary
        logger.info("  Checking mart_fraud_summary...")
        fraud_summary_checks = [
            ("row count > 0", "SELECT COUNT(*) FROM gold.mart_fraud_summary"),
            ("fraud_count NOT NULL", "SELECT COUNT(*) FROM gold.mart_fraud_summary WHERE fraud_count IS NULL"),
            ("fraud_rate_pct BETWEEN 0-100", "SELECT COUNT(*) FROM gold.mart_fraud_summary WHERE fraud_rate_pct < 0 OR fraud_rate_pct > 100"),
        ]

        for check_name, query in fraud_summary_checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                invalid_count = result['count']
                check_pass = (check_name == "row count > 0" and invalid_count > 0) or (invalid_count == 0)
                all_valid = all_valid and check_pass
                logger.info(f"    {check_name}: {'✓' if check_pass else '✗'}")
            except Exception as e:
                logger.warning(f"    {check_name}: Could not check - {e}")

        # Validate mart_customer_risk
        logger.info("  Checking mart_customer_risk...")
        customer_risk_checks = [
            ("row count > 0", "SELECT COUNT(*) FROM gold.mart_customer_risk"),
            ("total_transactions > 0", "SELECT COUNT(*) FROM gold.mart_customer_risk WHERE total_transactions <= 0"),
            ("risk_label valid", "SELECT COUNT(*) FROM gold.mart_customer_risk WHERE risk_label NOT IN ('HIGH','MEDIUM','LOW')"),
        ]

        for check_name, query in customer_risk_checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                invalid_count = result['count']
                check_pass = (check_name == "row count > 0" and invalid_count > 0) or (invalid_count == 0)
                all_valid = all_valid and check_pass
                logger.info(f"    {check_name}: {'✓' if check_pass else '✗'}")
            except Exception as e:
                logger.warning(f"    {check_name}: Could not check - {e}")

        # Validate mart_daily_volume
        logger.info("  Checking mart_daily_volume...")
        daily_volume_checks = [
            ("row count > 0", "SELECT COUNT(*) FROM gold.mart_daily_volume"),
            ("total_transactions NOT NULL", "SELECT COUNT(*) FROM gold.mart_daily_volume WHERE total_transactions IS NULL"),
        ]

        for check_name, query in daily_volume_checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                invalid_count = result['count']
                check_pass = (check_name == "row count > 0" and invalid_count > 0) or (invalid_count == 0)
                all_valid = all_valid and check_pass
                logger.info(f"    {check_name}: {'✓' if check_pass else '✗'}")
            except Exception as e:
                logger.warning(f"    {check_name}: Could not check - {e}")

        # Validate mart_rule_performance
        logger.info("  Checking mart_rule_performance...")
        rule_perf_checks = [
            ("row count > 0", "SELECT COUNT(*) FROM gold.mart_rule_performance"),
            ("trigger_count NOT NULL", "SELECT COUNT(*) FROM gold.mart_rule_performance WHERE trigger_count IS NULL"),
        ]

        for check_name, query in rule_perf_checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                invalid_count = result['count']
                check_pass = (check_name == "row count > 0" and invalid_count > 0) or (invalid_count == 0)
                all_valid = all_valid and check_pass
                logger.info(f"    {check_name}: {'✓' if check_pass else '✗'}")
            except Exception as e:
                logger.warning(f"    {check_name}: Could not check - {e}")

        cursor.close()
        conn.close()
        return all_valid

    except Exception as e:
        logger.error(f"Error validating gold layer: {e}")
        return False


def validate_layer(layer='both'):
    """Validate specific layer(s).
    
    Args:
        layer: 'raw', 'gold', or 'both' (default)
    """
    logger.info(f"Starting data quality validation for layer: {layer}")

    results = {}

    if layer in ('raw', 'both'):
        results['raw_transactions'] = validate_raw_transactions()
        results['fraud_flags'] = validate_fraud_flags()

    if layer in ('gold', 'both'):
        results['gold_marts'] = validate_gold_marts()

    # Summary
    logger.info("\n" + "="*50)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*50)
    for check_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"  {check_name}: {status}")

    all_passed = all(results.values())
    if all_passed:
        logger.info("All data quality checks passed ✓")
    else:
        logger.warning("Some data quality checks failed ✗")
        sys.exit(1)

    return all_passed


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fraud Pipeline Data Quality Validation')
    parser.add_argument('--layer', choices=['raw', 'gold', 'both'], default='both',
                        help='Which layer to validate (raw, gold, or both)')
    
    args = parser.parse_args()
    
    validate_layer(args.layer)