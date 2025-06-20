"""
Configuration management utilities.
"""
import yaml
from typing import Dict, Any

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration parameters.
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        ValueError: If configuration is invalid
    """
    required_keys = ['source_format', 'write_mode', 'spark_configs']
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")

    if config['source_format'] not in ['delta', 'parquet', 'csv']:
        raise ValueError(f"Unsupported source format: {config['source_format']}")

    if config['write_mode'] not in ['overwrite', 'append', 'merge']:
        raise ValueError(f"Unsupported write mode: {config['write_mode']}")
