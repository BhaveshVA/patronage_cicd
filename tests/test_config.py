"""
Test configuration management utilities.
"""
import pytest
from src.config import validate_config

def test_validate_config_valid():
    valid_config = {
        'source_format': 'delta',
        'write_mode': 'overwrite',
        'spark_configs': {
            'spark.sql.sources.partitionOverwriteMode': 'dynamic'
        }
    }
    # Should not raise any exception
    validate_config(valid_config)

def test_validate_config_missing_key():
    invalid_config = {
        'source_format': 'delta',
        'spark_configs': {}
    }
    with pytest.raises(ValueError, match="Missing required configuration key: write_mode"):
        validate_config(invalid_config)

def test_validate_config_invalid_source_format():
    invalid_config = {
        'source_format': 'invalid',
        'write_mode': 'overwrite',
        'spark_configs': {}
    }
    with pytest.raises(ValueError, match="Unsupported source format: invalid"):
        validate_config(invalid_config)

def test_validate_config_invalid_write_mode():
    invalid_config = {
        'source_format': 'delta',
        'write_mode': 'invalid',
        'spark_configs': {}
    }
    with pytest.raises(ValueError, match="Unsupported write mode: invalid"):
        validate_config(invalid_config)
