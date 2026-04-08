"""Shared utilities for the Silver pipeline."""
import logging
from datetime import datetime

from pyspark.sql import SparkSession


def get_spark(app_name: str = "olist-silver") -> SparkSession:
    """
    Get or create a SparkSession configured for Delta Lake + S3.

    On Databricks the active session is returned immediately.
    For local testing a minimal local session is created.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Allow new columns from source to be merged into existing Delta tables
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Bin-pack small files during writes
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO,
    )
    return logging.getLogger(name)


def pipeline_run_timestamp() -> datetime:
    """Return current UTC timestamp truncated to seconds (cleaner Delta storage)."""
    now = datetime.utcnow()
    return now.replace(microsecond=0)
