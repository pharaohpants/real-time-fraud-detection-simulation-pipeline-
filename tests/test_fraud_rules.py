"""
Unit tests for fraud detection rules.

Tests the scoring logic in spark/fraud_engine.py without requiring
Kafka, PostgreSQL, or Spark cluster connectivity.
"""
import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Module under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))
from fraud_engine import score_transactions


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("FraudRulesTest") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def schema():
    """Transaction schema matching Kafka message format."""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("location", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("card_type", StringType(), True),
        StructField("timestamp", TimestampType(), False),
    ])


class TestVelocityRule:
    """Test velocity check: >5 transactions from same customer in 1 minute."""

    def test_normal_velocity_not_flagged(self, spark, schema):
        """3 transactions in 1 minute should NOT trigger velocity."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Alfamart", 200000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(seconds=10)),
            ("tx3", "CUST_001", "John", "Shopee", 150000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(seconds=20)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.select("is_velocity").collect()
        assert all(row["is_velocity"] is False for row in rows)

    def test_high_velocity_flagged(self, spark, schema):
        """7 transactions in 1 minute SHOULD trigger velocity."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            (f"tx{i}", "CUST_001", "John", "Indomaret", 50000.0,
             "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(seconds=i * 5))
            for i in range(7)
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.orderBy("transaction_id").select("is_velocity").collect()
        # At least the last transactions should be flagged (count > 5 in window)
        flagged_count = sum(1 for row in rows if row["is_velocity"] is True)
        assert flagged_count > 0


class TestImpossibleTravel:
    """Test impossible travel: speed > 900 km/h between consecutive transactions."""

    def test_same_location_not_flagged(self, spark, schema):
        """Transactions from same location should NOT trigger impossible travel."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2088, 106.8456, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Alfamart", 200000.0, "Jakarta", -6.2088, 106.8456, "VISA", base_time + timedelta(minutes=5)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.select("is_impossible_travel").collect()
        assert all(row["is_impossible_travel"] is False for row in rows)

    def test_far_locations_short_time_flagged(self, spark, schema):
        """Jakarta to Medan in 5 minutes (1750km) should trigger impossible travel."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2088, 106.8456, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Alfamart", 200000.0, "Medan", 3.5952, 98.6722, "VISA", base_time + timedelta(minutes=5)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.orderBy("transaction_id").select("is_impossible_travel").collect()
        # Second transaction should be flagged
        assert rows[1]["is_impossible_travel"] is True

    def test_far_locations_long_time_not_flagged(self, spark, schema):
        """Jakarta to Medan in 3 hours (normal flight) should NOT trigger."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2088, 106.8456, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Alfamart", 200000.0, "Medan", 3.5952, 98.6722, "VISA", base_time + timedelta(hours=3)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.orderBy("transaction_id").select("is_impossible_travel").collect()
        assert rows[1]["is_impossible_travel"] is False


class TestAmountSpike:
    """Test amount spike detection."""

    def test_small_amount_not_flagged(self, spark, schema):
        """Normal amount (< 10M, no baseline) should NOT trigger."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 500000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.select("is_amount_spike").collect()
        assert rows[0]["is_amount_spike"] is False

    def test_large_amount_flagged_no_baseline(self, spark, schema):
        """Amount > 10M without baseline should trigger (fallback rule)."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Tokopedia", 15000000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.select("is_amount_spike").collect()
        assert rows[0]["is_amount_spike"] is True

    def test_z_score_spike_with_baseline(self, spark, schema):
        """Amount with Z-score > 2.5 from baseline should trigger."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        # Customer avg=100k, std=20k → amount of 200k has Z=(200k-100k)/20k = 5.0 > 2.5
        data = [
            ("tx1", "CUST_001", "John", "Tokopedia", 200000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
        ]
        df = spark.createDataFrame(data, schema)

        baseline_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("avg_amount", DoubleType(), True),
            StructField("std_amount", DoubleType(), True),
        ])
        baseline_data = [("CUST_001", 100000.0, 20000.0)]
        baseline_df = spark.createDataFrame(baseline_data, baseline_schema)

        result = score_transactions(df, baseline_df)
        rows = result.select("is_amount_spike").collect()
        assert rows[0]["is_amount_spike"] is True


class TestOffHours:
    """Test off-hours rule: transactions between 01:00 and 05:00."""

    def test_daytime_not_flagged(self, spark, schema):
        """10:00 AM transaction should NOT trigger off-hours."""
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2, 106.8, "VISA",
             datetime(2024, 6, 15, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        assert result.collect()[0]["is_off_hours"] is False

    def test_early_morning_flagged(self, spark, schema):
        """02:30 AM transaction SHOULD trigger off-hours."""
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2, 106.8, "VISA",
             datetime(2024, 6, 15, 2, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        assert result.collect()[0]["is_off_hours"] is True

    def test_boundary_midnight_not_flagged(self, spark, schema):
        """00:30 (before 01:00) should NOT trigger off-hours."""
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2, 106.8, "VISA",
             datetime(2024, 6, 15, 0, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        assert result.collect()[0]["is_off_hours"] is False


class TestCardTesting:
    """Test card testing: 3+ micro-transactions followed by large amount."""

    def test_no_micro_transactions_not_flagged(self, spark, schema):
        """Normal transactions should NOT trigger card testing."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 500000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Alfamart", 300000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(minutes=2)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.select("is_card_testing").collect()
        assert all(row["is_card_testing"] is False for row in rows)

    def test_micro_then_large_flagged(self, spark, schema):
        """3 micro-transactions (<10k) + 1 large (>1M) in 10min should trigger."""
        base_time = datetime(2024, 6, 15, 10, 0, 0)
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 5000.0, "Jakarta", -6.2, 106.8, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Indomaret", 3000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(minutes=1)),
            ("tx3", "CUST_001", "John", "Indomaret", 7000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(minutes=2)),
            ("tx4", "CUST_001", "John", "Tokopedia", 5000000.0, "Jakarta", -6.2, 106.8, "VISA", base_time + timedelta(minutes=3)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.orderBy("transaction_id").select("is_card_testing").collect()
        # The large transaction (tx4) or at least one should be flagged
        flagged_count = sum(1 for row in rows if row["is_card_testing"] is True)
        assert flagged_count > 0


class TestDecisionLogic:
    """Test composite scoring and decision output."""

    def test_approve_decision_low_score(self, spark, schema):
        """Single normal transaction should get APPROVE with score 0."""
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2, 106.8, "VISA",
             datetime(2024, 6, 15, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        row = result.collect()[0]
        assert row["risk_score"] == 0
        assert row["decision"] == "APPROVE"

    def test_review_decision_medium_score(self, spark, schema):
        """Off-hours (20) + amount_spike (30) = 50 → REVIEW."""
        data = [
            ("tx1", "CUST_001", "John", "Tokopedia", 15000000.0, "Jakarta", -6.2, 106.8, "VISA",
             datetime(2024, 6, 15, 2, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        row = result.collect()[0]
        # off_hours=20 + amount_spike=30 = 50
        assert row["risk_score"] == 50
        assert row["decision"] == "REVIEW"

    def test_block_decision_high_score(self, spark, schema):
        """Multiple rules triggered → score >= 70 → BLOCK."""
        # Trigger: off_hours(20) + amount_spike(30) + impossible_travel(50) = 100
        base_time = datetime(2024, 6, 15, 2, 0, 0)  # off hours
        data = [
            ("tx1", "CUST_001", "John", "Indomaret", 100000.0, "Jakarta", -6.2088, 106.8456, "VISA", base_time),
            ("tx2", "CUST_001", "John", "Tokopedia", 15000000.0, "Medan", 3.5952, 98.6722, "VISA",
             base_time + timedelta(minutes=2)),
        ]
        df = spark.createDataFrame(data, schema)
        result = score_transactions(df)
        rows = result.orderBy("transaction_id").collect()
        # tx2 should be BLOCK (impossible_travel=50 + amount_spike=30 + off_hours=20 = 100)
        assert rows[1]["decision"] == "BLOCK"
        assert rows[1]["risk_score"] >= 70
