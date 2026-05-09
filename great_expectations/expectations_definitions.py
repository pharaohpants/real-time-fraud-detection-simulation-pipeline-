"""
Great Expectations Expectation Suite for raw_transactions table
"""
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def get_raw_transactions_expectations():
    """Define expectations for raw_transactions table"""
    
    expectations = [
        # Not null checks
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "transaction_id"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "amount"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "timestamp"}
        },
        
        # Range validations
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "amount",
                "min_value": 0,
                "max_value": 100_000_000
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "lat",
                "min_value": -11,
                "max_value": 6
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "lon",
                "min_value": 95,
                "max_value": 141
            }
        },
        
        # Categorical checks
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "card_type",
                "value_set": ["VISA", "MASTERCARD", "GPN", "JCB"]
            }
        },
        
        # Uniqueness
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "transaction_id"}
        }
    ]
    
    return expectations


def get_fraud_flags_expectations():
    """Define expectations for fraud_flags table"""
    
    expectations = [
        # Not null checks
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "transaction_id"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "risk_score"}
        },
        
        # Range validation for risk score
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "risk_score",
                "min_value": 0,
                "max_value": 200
            }
        },
        
        # Valid decision values
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "decision",
                "value_set": ["APPROVE", "REVIEW", "BLOCK"]
            }
        }
    ]
    
    return expectations
