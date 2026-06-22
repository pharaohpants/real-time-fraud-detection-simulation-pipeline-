"""
Unit tests for data quality validation logic.
Tests the validation functions without requiring a live PostgreSQL connection.
"""
import pytest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'great_expectations'))
from expectations_definitions import (
    get_raw_transactions_expectations,
    get_fraud_flags_expectations
)


class TestExpectationDefinitions:
    """Test that expectation suites are properly defined."""

    def test_raw_transactions_has_not_null_checks(self):
        """Raw transactions expectations must include NOT NULL checks."""
        expectations = get_raw_transactions_expectations()
        not_null_checks = [
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_not_be_null'
        ]
        columns_checked = [e['kwargs']['column'] for e in not_null_checks]
        assert 'transaction_id' in columns_checked
        assert 'customer_id' in columns_checked
        assert 'amount' in columns_checked
        assert 'timestamp' in columns_checked

    def test_raw_transactions_has_range_checks(self):
        """Raw transactions must validate amount and coordinate ranges."""
        expectations = get_raw_transactions_expectations()
        range_checks = [
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_be_between'
        ]
        columns_checked = [e['kwargs']['column'] for e in range_checks]
        assert 'amount' in columns_checked
        assert 'lat' in columns_checked
        assert 'lon' in columns_checked

    def test_raw_transactions_amount_range(self):
        """Amount range should be 0 to 100M."""
        expectations = get_raw_transactions_expectations()
        amount_check = next(
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_be_between'
            and e['kwargs']['column'] == 'amount'
        )
        assert amount_check['kwargs']['min_value'] == 0
        assert amount_check['kwargs']['max_value'] == 100_000_000

    def test_raw_transactions_card_type_set(self):
        """Card type must be limited to valid values."""
        expectations = get_raw_transactions_expectations()
        card_check = next(
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_be_in_set'
            and e['kwargs']['column'] == 'card_type'
        )
        assert set(card_check['kwargs']['value_set']) == {'VISA', 'MASTERCARD', 'GPN', 'JCB'}

    def test_fraud_flags_has_risk_score_range(self):
        """Fraud flags must validate risk_score between 0 and 200."""
        expectations = get_fraud_flags_expectations()
        range_checks = [
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_be_between'
        ]
        risk_check = next(e for e in range_checks if e['kwargs']['column'] == 'risk_score')
        assert risk_check['kwargs']['min_value'] == 0
        assert risk_check['kwargs']['max_value'] == 200

    def test_fraud_flags_decision_values(self):
        """Decision must be one of APPROVE, REVIEW, BLOCK."""
        expectations = get_fraud_flags_expectations()
        decision_check = next(
            e for e in expectations
            if e['expectation_type'] == 'expect_column_values_to_be_in_set'
            and e['kwargs']['column'] == 'decision'
        )
        assert set(decision_check['kwargs']['value_set']) == {'APPROVE', 'REVIEW', 'BLOCK'}
