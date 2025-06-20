"""
Core ETL transformation functions.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import yaml
from delta.tables import DeltaTable

class ETLTransformer:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the ETL transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config

    def extract(self, source_path: str) -> DataFrame:
        """
        Extract data from source.
        
        Args:
            source_path: Path to source data
            
        Returns:
            DataFrame containing source data
        """
        return self.spark.read.format(self.config["source_format"]).load(source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Enhanced transformation: deduplicate, SCD2 logic, change tracking, and audit columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        from pyspark.sql.functions import col, lit, current_timestamp, concat_ws, when, sha2
        from pyspark.sql.types import StringType, BooleanType, DateType
        import logging
        logger = logging.getLogger(__name__)
        try:
            # Deduplicate by key columns
            key_cols = ["ICN", "Veteran_ICN", "Applicant_Type"]
            if all(k in df.columns for k in key_cols):
                df = df.dropDuplicates(key_cols)
            # SCD2: Add hash for change tracking
            change_cols = [c for c in ["Status_Begin_Date", "Status_Termination_Date", "Caregiver_Status"] if c in df.columns]
            if change_cols:
                df = df.withColumn(
                    "change_hash",
                    sha2(concat_ws("|", *[col(c).cast(StringType()) for c in change_cols]), 256)
                )
            # Add audit columns
            if "RecordStatus" not in df.columns:
                df = df.withColumn("RecordStatus", lit(True).cast(BooleanType()))
            if "RecordLastUpdated" not in df.columns:
                df = df.withColumn("RecordLastUpdated", current_timestamp().cast(DateType()))
            if "Batch_CD" not in df.columns:
                df = df.withColumn("Batch_CD", lit("CG").cast(StringType()))
            # Change log: Track what changed (example for Status_Begin_Date)
            if "Status_Begin_Date" in df.columns and "Status_Last_Update" in df.columns:
                df = df.withColumn(
                    "change_log",
                    when(col("Status_Begin_Date") != col("Status_Last_Update"),
                         concat_ws(" ", lit("Status_Begin_Date changed from"), col("Status_Last_Update"), lit("to"), col("Status_Begin_Date")))
                    .otherwise(lit(""))
                )
            return df
        except Exception as e:
            logger.error(f"Error in enhanced transform: {e}")
            raise

    def load(self, df: DataFrame, target_path: str) -> None:
        """
        Load data to target location.
        
        Args:
            df: DataFrame to load
            target_path: Path to target location
        """
        df.write \
          .format("delta") \
          .mode(self.config["write_mode"]) \
          .save(target_path)

    def run_pipeline(self, source_path: str, target_path: str) -> None:
        """
        Run the complete ETL pipeline.
        
        Args:
            source_path: Path to source data
            target_path: Path to target location
        """
        df = self.extract(source_path)
        transformed_df = self.transform(df)
        self.load(transformed_df, target_path)
