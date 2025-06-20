"""
Main entry point for the ETL pipeline.
"""
from pyspark.sql import SparkSession
from src.config import load_config, validate_config
from src.transformer import ETLTransformer
import argparse
import logging

def create_spark_session(app_name: str, configs: dict) -> SparkSession:
    """
    Create a SparkSession with the given configurations.
    
    Args:
        app_name: Name of the Spark application
        configs: Spark configuration dictionary
        
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run ETL pipeline')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--source', required=True, help='Path to source data')
    parser.add_argument('--target', required=True, help='Path to target location')
    args = parser.parse_args()

    try:
        # Load and validate configuration
        config = load_config(args.config)
        validate_config(config)

        # Create Spark session
        spark = create_spark_session("Databricks ETL", config["spark_configs"])
        logger.info("Created Spark session")

        # Initialize and run transformer
        transformer = ETLTransformer(spark, config)
        transformer.run_pipeline(args.source, args.target)
        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
