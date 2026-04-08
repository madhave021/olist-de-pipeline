"""
Unit tests for scd_processor.py.

Tests use a local SparkSession with Delta Lake and a tmp_path Delta table.
Each test gets its own isolated Delta table path.
"""
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")
pytest.importorskip("delta", reason="delta-spark not installed")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import IntegerType, StringType, StructField, StructType  # noqa: E402

from config import SCD2_CURRENT_COL, SCD2_END_COL, SCD2_HASH_COL, SCD2_START_COL, TableConfig  # noqa: E402
from scd_processor import apply_scd_type1, apply_scd_type2  # noqa: E402

TS1 = datetime(2024, 1, 1, 0, 0, 0)
TS2 = datetime(2024, 1, 2, 0, 0, 0)

SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])

SIMPLE_CONFIG = TableConfig(
    name="customers",
    primary_keys=["customer_id"],
    scd_type=2,
    tracked_columns=["name", "city"],
    not_null_cols=["customer_id"],
)


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


# ─────────────────────────────────────────────────────────────────────────────
# SCD Type 2
# ─────────────────────────────────────────────────────────────────────────────

class TestScdType2:
    def test_initial_load_creates_table(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        df = _df(spark, [("c1", "Alice", "NY"), ("c2", "Bob", "LA")])
        apply_scd_type2(spark, df, path, SIMPLE_CONFIG, TS1)

        from delta.tables import DeltaTable
        assert DeltaTable.isDeltaTable(spark, path)

        result = spark.read.format("delta").load(path)
        assert result.count() == 2
        # All records should be current after initial load
        assert result.filter(F.col(SCD2_CURRENT_COL) == True).count() == 2  # noqa: E712

    def test_initial_load_sets_scd_metadata(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        df = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df, path, SIMPLE_CONFIG, TS1)

        result = spark.read.format("delta").load(path).collect()
        assert len(result) == 1
        row = result[0]
        assert row[SCD2_CURRENT_COL] is True
        assert row[SCD2_START_COL] == TS1
        assert row[SCD2_END_COL] is None
        assert row[SCD2_HASH_COL] is not None

    def test_unchanged_record_stays_current(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        df1 = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df1, path, SIMPLE_CONFIG, TS1)

        df2 = _df(spark, [("c1", "Alice", "NY")])  # same data
        apply_scd_type2(spark, df2, path, SIMPLE_CONFIG, TS2)

        result = spark.read.format("delta").load(path)
        # Only one record: no new version created
        assert result.count() == 1
        assert result.filter(F.col(SCD2_CURRENT_COL) == True).count() == 1  # noqa: E712

    def test_changed_record_expires_old_inserts_new(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        df1 = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df1, path, SIMPLE_CONFIG, TS1)

        df2 = _df(spark, [("c1", "Alice", "CA")])  # city changed
        apply_scd_type2(spark, df2, path, SIMPLE_CONFIG, TS2)

        result = spark.read.format("delta").load(path)
        # Two versions: old (expired) + new (current)
        assert result.count() == 2

        expired = result.filter(F.col(SCD2_CURRENT_COL) == False).collect()  # noqa: E712
        assert len(expired) == 1
        assert expired[0]["city"] == "NY"
        assert expired[0][SCD2_END_COL] == TS2

        current = result.filter(F.col(SCD2_CURRENT_COL) == True).collect()  # noqa: E712
        assert len(current) == 1
        assert current[0]["city"] == "CA"
        assert current[0][SCD2_END_COL] is None

    def test_new_pk_inserted_without_expiring_others(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        df1 = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df1, path, SIMPLE_CONFIG, TS1)

        df2 = _df(spark, [("c1", "Alice", "NY"), ("c2", "Bob", "LA")])
        apply_scd_type2(spark, df2, path, SIMPLE_CONFIG, TS2)

        result = spark.read.format("delta").load(path)
        assert result.count() == 2
        assert result.filter(F.col(SCD2_CURRENT_COL) == True).count() == 2  # noqa: E712

    def test_multiple_changes_accumulate_history(self, spark, tmp_path):
        path = str(tmp_path / "customers")
        ts3 = datetime(2024, 1, 3, 0, 0, 0)

        df1 = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df1, path, SIMPLE_CONFIG, TS1)

        df2 = _df(spark, [("c1", "Alice", "CA")])
        apply_scd_type2(spark, df2, path, SIMPLE_CONFIG, TS2)

        df3 = _df(spark, [("c1", "Alice", "TX")])
        apply_scd_type2(spark, df3, path, SIMPLE_CONFIG, ts3)

        result = spark.read.format("delta").load(path)
        assert result.count() == 3
        assert result.filter(F.col(SCD2_CURRENT_COL) == True).count() == 1  # noqa: E712
        assert result.filter(F.col(SCD2_CURRENT_COL) == True).collect()[0]["city"] == "TX"  # noqa: E712

    def test_null_to_value_change_detected(self, spark, tmp_path):
        """NULL → value in a tracked column must be detected as a change."""
        path = str(tmp_path / "customers")
        df1 = _df(spark, [("c1", "Alice", None)])
        apply_scd_type2(spark, df1, path, SIMPLE_CONFIG, TS1)

        df2 = _df(spark, [("c1", "Alice", "NY")])
        apply_scd_type2(spark, df2, path, SIMPLE_CONFIG, TS2)

        result = spark.read.format("delta").load(path)
        assert result.count() == 2


# ─────────────────────────────────────────────────────────────────────────────
# SCD Type 1
# ─────────────────────────────────────────────────────────────────────────────

class TestScdType1:
    _CONFIG = TableConfig(
        name="payments",
        primary_keys=["order_id", "seq"],
        scd_type=1,
        tracked_columns=["amount"],
        not_null_cols=["order_id"],
    )
    _SCHEMA = StructType([
        StructField("order_id", StringType()),
        StructField("seq", IntegerType()),
        StructField("amount", IntegerType()),
    ])

    def _df(self, spark, rows):
        return spark.createDataFrame(rows, schema=self._SCHEMA)

    def test_initial_load(self, spark, tmp_path):
        path = str(tmp_path / "payments")
        df = self._df(spark, [("o1", 1, 100)])
        apply_scd_type1(spark, df, path, self._CONFIG)
        result = spark.read.format("delta").load(path)
        assert result.count() == 1

    def test_upsert_overwrites_existing(self, spark, tmp_path):
        path = str(tmp_path / "payments")
        df1 = self._df(spark, [("o1", 1, 100)])
        apply_scd_type1(spark, df1, path, self._CONFIG)

        df2 = self._df(spark, [("o1", 1, 200)])  # amount updated
        apply_scd_type1(spark, df2, path, self._CONFIG)

        result = spark.read.format("delta").load(path).collect()
        assert len(result) == 1
        assert result[0]["amount"] == 200

    def test_new_record_inserted(self, spark, tmp_path):
        path = str(tmp_path / "payments")
        df1 = self._df(spark, [("o1", 1, 100)])
        apply_scd_type1(spark, df1, path, self._CONFIG)

        df2 = self._df(spark, [("o1", 1, 100), ("o2", 1, 50)])
        apply_scd_type1(spark, df2, path, self._CONFIG)

        result = spark.read.format("delta").load(path)
        assert result.count() == 2

    def test_no_history_created(self, spark, tmp_path):
        """SCD1 must not create extra historical rows on change."""
        path = str(tmp_path / "payments")
        df1 = self._df(spark, [("o1", 1, 100)])
        apply_scd_type1(spark, df1, path, self._CONFIG)

        df2 = self._df(spark, [("o1", 1, 999)])
        apply_scd_type1(spark, df2, path, self._CONFIG)

        result = spark.read.format("delta").load(path)
        # Must still be exactly 1 row — no SCD-2 history
        assert result.count() == 1
