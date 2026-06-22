"""
Unit tests for the transaction simulator.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'simulator'))

from producer import generate_transaction, CUSTOMER_POOL, MERCHANTS, CITIES, CARD_TYPES


class TestTransactionGeneration:
    """Test synthetic transaction generation."""

    def test_transaction_has_required_fields(self):
        """Generated transaction must contain all required fields."""
        tx = generate_transaction()
        required_fields = [
            'transaction_id', 'customer_id', 'name', 'merchant',
            'amount', 'location', 'lat', 'lon', 'card_type', 'timestamp'
        ]
        for field in required_fields:
            assert field in tx, f"Missing field: {field}"

    def test_customer_id_from_pool(self):
        """Customer ID must come from the fixed pool."""
        tx = generate_transaction()
        assert tx['customer_id'] in CUSTOMER_POOL

    def test_merchant_from_list(self):
        """Merchant must come from defined list."""
        tx = generate_transaction()
        assert tx['merchant'] in MERCHANTS

    def test_location_from_cities(self):
        """Location must be a valid city."""
        tx = generate_transaction()
        assert tx['location'] in CITIES

    def test_card_type_valid(self):
        """Card type must be one of the accepted types."""
        tx = generate_transaction()
        assert tx['card_type'] in CARD_TYPES

    def test_amount_positive(self):
        """Amount must be positive."""
        tx = generate_transaction()
        assert tx['amount'] > 0

    def test_amount_within_range(self):
        """Amount must be within simulator range (10k - 50M)."""
        for _ in range(100):
            tx = generate_transaction()
            assert 10_000 <= tx['amount'] <= 50_000_000

    def test_coordinates_indonesia_bounds(self):
        """Coordinates must be within Indonesia bounds."""
        tx = generate_transaction()
        assert -11 <= tx['lat'] <= 6
        assert 95 <= tx['lon'] <= 141

    def test_transaction_id_is_uuid(self):
        """Transaction ID must be a valid UUID format."""
        import uuid
        tx = generate_transaction()
        # Should not raise ValueError
        uuid.UUID(tx['transaction_id'])

    def test_timestamp_is_iso_format(self):
        """Timestamp must be valid ISO format."""
        from datetime import datetime
        tx = generate_transaction()
        # Should not raise ValueError
        datetime.fromisoformat(tx['timestamp'])

    def test_unique_transaction_ids(self):
        """Each generated transaction should have a unique ID."""
        ids = [generate_transaction()['transaction_id'] for _ in range(100)]
        assert len(ids) == len(set(ids))
