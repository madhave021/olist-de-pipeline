"""
pytest configuration for the Silver pipeline unit tests.

Adds the silver package directory to sys.path so modules can be imported
without installation.  Creates a session-scoped SparkSession with Delta
support for use in tests that need a real Spark runtime.
"""
import os
import sys

import pytest

# Make the silver package importable from tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "silver"))


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession with Delta Lake enabled.
    Skips automatically when pyspark or delta-spark are not installed.
    """
    pytest.importorskip("pyspark", reason="pyspark not installed")
    pytest.importorskip("delta", reason="delta-spark not installed")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("olist-silver-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
