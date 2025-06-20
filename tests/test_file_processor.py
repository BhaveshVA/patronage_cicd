import pytest
from pyspark.sql import SparkSession
from src.file_processor import FileProcessor
from src.schemas import new_cg_schema, scd_schema, scd_schema1
import logging

def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_initialize_caregivers(monkeypatch):
    spark = spark_session()
    config = {
        "initial_cg_file": "dummy.csv",
        "cg_source": "dummy",
        "scd_source": "dummy",
        "pt_old_source": "dummy",
        "pt_new_source": "dummy",
        "patronage_tablename": "dummy",
        "patronage_table_location": "dummy",
        "cg_start_datetime": "2024-12-18 23:59:59",
        "others_start_datetime": "2025-04-01 00:00:00",
        "identity_correlations_path": "dummy",
        "new_cg_schema": new_cg_schema,
        "scd_schema": scd_schema,
        "scd_schema1": scd_schema1
    }
    # Patch spark.read.csv to return a test DataFrame
    monkeypatch.setattr(spark.read, "csv", lambda *a, **k: spark.createDataFrame([], new_cg_schema))
    monkeypatch.setattr(spark.read, "format", lambda *a, **k: spark.read)
    monkeypatch.setattr(spark.read, "load", lambda *a, **k: spark.createDataFrame([], new_cg_schema))
    processor = FileProcessor(spark, config)
    result = processor.initialize_caregivers()
    assert result is not None
    assert hasattr(result, "columns")

def test_prepare_caregivers_data(monkeypatch):
    spark = spark_session()
    config = {
        "initial_cg_file": "dummy.csv",
        "cg_source": "dummy",
        "scd_source": "dummy",
        "pt_old_source": "dummy",
        "pt_new_source": "dummy",
        "patronage_tablename": "dummy",
        "patronage_table_location": "dummy",
        "cg_start_datetime": "2024-12-18 23:59:59",
        "others_start_datetime": "2025-04-01 00:00:00",
        "identity_correlations_path": "dummy",
        "new_cg_schema": new_cg_schema,
        "scd_schema": scd_schema,
        "scd_schema1": scd_schema1
    }
    monkeypatch.setattr(spark.read, "csv", lambda *a, **k: spark.createDataFrame([], new_cg_schema))
    monkeypatch.setattr(spark.read, "format", lambda *a, **k: spark.read)
    monkeypatch.setattr(spark.read, "load", lambda *a, **k: spark.createDataFrame([], new_cg_schema))
    processor = FileProcessor(spark, config)
    empty_df = spark.createDataFrame([], new_cg_schema)
    result = processor.prepare_caregivers_data(empty_df)
    assert result is not None
    assert hasattr(result, "columns")

def test_prepare_scd_data(monkeypatch):
    spark = spark_session()
    config = {
        "initial_cg_file": "dummy.csv",
        "cg_source": "dummy",
        "scd_source": "dummy",
        "pt_old_source": "dummy",
        "pt_new_source": "dummy",
        "patronage_tablename": "dummy",
        "patronage_table_location": "dummy",
        "cg_start_datetime": "2024-12-18 23:59:59",
        "others_start_datetime": "2025-04-01 00:00:00",
        "identity_correlations_path": "dummy",
        "new_cg_schema": new_cg_schema,
        "scd_schema": scd_schema,
        "scd_schema1": scd_schema1
    }
    monkeypatch.setattr(spark.read, "csv", lambda *a, **k: spark.createDataFrame([], scd_schema))
    monkeypatch.setattr(spark.read, "format", lambda *a, **k: spark.read)
    monkeypatch.setattr(spark.read, "load", lambda *a, **k: spark.createDataFrame([], scd_schema))
    processor = FileProcessor(spark, config)
    class DummyRow:
        path = "dummy.csv"
    dummy_row = DummyRow()
    result = processor.prepare_scd_data(dummy_row)
    assert result is not None
    assert hasattr(result, "columns")

def test_update_pai_data(monkeypatch):
    spark = spark_session()
    config = {
        "initial_cg_file": "dummy.csv",
        "cg_source": "dummy",
        "scd_source": "dummy",
        "pt_old_source": "dummy",
        "pt_new_source": "dummy",
        "patronage_tablename": "dummy",
        "patronage_table_location": "dummy",
        "cg_start_datetime": "2024-12-18 23:59:59",
        "others_start_datetime": "2025-04-01 00:00:00",
        "identity_correlations_path": "dummy",
        "new_cg_schema": new_cg_schema,
        "scd_schema": scd_schema,
        "scd_schema1": scd_schema1
    }
    monkeypatch.setattr(spark.read, "csv", lambda *a, **k: spark.createDataFrame([], scd_schema))
    monkeypatch.setattr(spark.read, "format", lambda *a, **k: spark.read)
    monkeypatch.setattr(spark.read, "load", lambda *a, **k: spark.createDataFrame([], scd_schema))
    processor = FileProcessor(spark, config)
    class DummyRow:
        path = "dummy.csv"
        dateTime = "2025-06-20T00:00:00"
    dummy_row = DummyRow()
    result = processor.update_pai_data(dummy_row, "text")
    assert result is not None
    assert hasattr(result, "columns")
