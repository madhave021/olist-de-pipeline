"""
Unit tests for schema_manager.py.

Tests cover align_source_to_target (type casting + NULL backfill) and
evolve_delta_schema (ALTER TABLE ADD COLUMNS).
"""
import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from schema_manager import align_source_to_target, evolve_delta_schema  # noqa: E402
from config import SCD2_CURRENT_COL, SCD2_END_COL, SCD2_HASH_COL, SCD2_START_COL  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# align_source_to_target
# ─────────────────────────────────────────────────────────────────────────────

class TestAlignSourceToTarget:
    def test_identical_schemas_pass_through(self, spark):
        schema = StructType([
            StructField("id", StringType()),
            StructField("val", IntegerType()),
        ])
        df = spark.createDataFrame([("a", 1)], schema=schema)
        result = align_source_to_target(df, schema)
        assert set(result.columns) == {"id", "val"}

    def test_missing_target_col_backfilled_as_null(self, spark):
        source_schema = StructType([StructField("id", StringType())])
        target_schema = StructType([
            StructField("id", StringType()),
            StructField("new_col", IntegerType()),
        ])
        df = spark.createDataFrame([("a",)], schema=source_schema)
        result = align_source_to_target(df, target_schema)
        assert "new_col" in result.columns
        row = result.collect()[0]
        assert row["new_col"] is None

    def test_widening_int_to_long_casts_correctly(self, spark):
        source_schema = StructType([StructField("amount", IntegerType())])
        target_schema = StructType([StructField("amount", LongType())])
        df = spark.createDataFrame([(42,)], schema=source_schema)
        result = align_source_to_target(df, target_schema)
        assert result.schema["amount"].dataType == LongType()

    def test_unsafe_type_change_casts_to_string(self, spark):
        source_schema = StructType([StructField("val", TimestampType())])
        target_schema = StructType([StructField("val", IntegerType())])
        from datetime import datetime
        df = spark.createDataFrame([(datetime(2024, 1, 1),)], schema=source_schema)
        result = align_source_to_target(df, target_schema)
        # Unsafe cast → STRING
        assert result.schema["val"].dataType == StringType()

    def test_scd_internal_cols_excluded_from_output(self, spark):
        """SCD metadata cols in target must be excluded from the aligned source."""
        source_schema = StructType([StructField("id", StringType())])
        target_schema = StructType([
            StructField("id", StringType()),
            StructField(SCD2_CURRENT_COL, StringType()),
            StructField(SCD2_HASH_COL, StringType()),
            StructField(SCD2_START_COL, TimestampType()),
            StructField(SCD2_END_COL, TimestampType()),
        ])
        df = spark.createDataFrame([("a",)], schema=source_schema)
        result = align_source_to_target(df, target_schema)
        for scd_col in [SCD2_CURRENT_COL, SCD2_HASH_COL, SCD2_START_COL, SCD2_END_COL]:
            assert scd_col not in result.columns

    def test_extra_source_cols_appended(self, spark):
        """Source columns not yet in target should be passed through (schema evolution)."""
        source_schema = StructType([
            StructField("id", StringType()),
            StructField("brand_new_col", DoubleType()),
        ])
        target_schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([("a", 3.14)], schema=source_schema)
        result = align_source_to_target(df, target_schema)
        assert "brand_new_col" in result.columns


# ─────────────────────────────────────────────────────────────────────────────
# evolve_delta_schema
# ─────────────────────────────────────────────────────────────────────────────

class TestEvolveDeltaSchema:
    def test_noop_when_table_does_not_exist(self, spark, tmp_path):
        """evolve_delta_schema must not raise when the target table is absent."""
        df = spark.createDataFrame([("a",)], schema="id STRING")
        # Should complete silently without error
        evolve_delta_schema(spark, df, str(tmp_path / "nonexistent"))

    def test_adds_new_column_to_existing_table(self, spark, tmp_path):
        pytest.importorskip("delta", reason="delta-spark not installed")
        from delta.tables import DeltaTable

        path = str(tmp_path / "evolve_test")
        schema_v1 = StructType([StructField("id", StringType())])
        spark.createDataFrame([("a",)], schema=schema_v1).write.format("delta").save(path)

        schema_v2 = StructType([
            StructField("id", StringType()),
            StructField("extra_col", IntegerType()),
        ])
        df_v2 = spark.createDataFrame([("b", 1)], schema=schema_v2)
        evolve_delta_schema(spark, df_v2, path)

        updated_cols = {f.name for f in DeltaTable.forPath(spark, path).toDF().schema.fields}
        assert "extra_col" in updated_cols

    def test_no_duplicate_columns_added(self, spark, tmp_path):
        """Calling evolve twice with same schema must not duplicate columns."""
        pytest.importorskip("delta", reason="delta-spark not installed")
        from delta.tables import DeltaTable

        path = str(tmp_path / "evolve_idem")
        schema = StructType([
            StructField("id", StringType()),
            StructField("val", IntegerType()),
        ])
        spark.createDataFrame([("a", 1)], schema=schema).write.format("delta").save(path)

        df_same = spark.createDataFrame([("b", 2)], schema=schema)
        evolve_delta_schema(spark, df_same, path)
        evolve_delta_schema(spark, df_same, path)  # second call — must be idempotent

        cols = [f.name for f in DeltaTable.forPath(spark, path).toDF().schema.fields]
        assert len(cols) == len(set(cols))  # no duplicates
