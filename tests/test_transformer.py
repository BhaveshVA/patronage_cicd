"""
Test ETL transformation functions.
"""
import pytest
from pyspark.sql import SparkSession
from src.transformer import ETLTransformer

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test-etl") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture
def config():
    return {
        'source_format': 'delta',
        'write_mode': 'overwrite'
    }

@pytest.fixture
def transformer(spark, config):
    return ETLTransformer(spark, config)

def test_extract(spark, transformer, tmp_path):
    # Create test data
    test_data = [("John", 30), ("Jane", 25)]
    test_df = spark.createDataFrame(test_data, ["name", "age"])
    test_path = str(tmp_path / "test_data")
    test_df.write.format("delta").save(test_path)
    
    # Test extraction
    result_df = transformer.extract(test_path)
    assert result_df.count() == 2
    assert result_df.columns == ["name", "age"]

def test_transform(transformer):
    # Create test data
    test_data = [("John", 30), ("Jane", 25)]
    test_df = transformer.spark.createDataFrame(test_data, ["name", "age"])
    
    # Test transformation
    result_df = transformer.transform(test_df)
    assert result_df.count() == 2  # Basic test, update based on your transformation logic

def test_load(transformer, tmp_path):
    # Create test data
    test_data = [("John", 30), ("Jane", 25)]
    test_df = transformer.spark.createDataFrame(test_data, ["name", "age"])
    
    # Test loading
    test_path = str(tmp_path / "test_output")
    transformer.load(test_df, test_path)
    
    # Verify the data was written
    loaded_df = transformer.spark.read.format("delta").load(test_path)
    assert loaded_df.count() == 2
    assert loaded_df.columns == ["name", "age"]
