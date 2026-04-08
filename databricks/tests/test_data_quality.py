"""
Unit tests for data_quality.py.

These tests use a real SparkSession (Delta not required) and cover every check
function plus the orchestrating run_dq_checks() and assert_no_critical_failures().
"""
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import IntegerType, StringType, StructField, StructType  # noqa: E402

from data_quality import (  # noqa: E402
    assert_no_critical_failures,
    check_duplicates,
    check_empty_table,
    check_nulls,
    check_range,
    check_valid_values,
    run_dq_checks,
)
from config import TableConfig  # noqa: E402

RUN_TS = datetime(2024, 1, 1, 12, 0, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _df(spark: SparkSession, rows, schema=None):
    if schema:
        return spark.createDataFrame(rows, schema=schema)
    return spark.createDataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# check_empty_table
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckEmptyTable:
    def test_pass_when_rows_present(self, spark):
        rows = check_empty_table(None, "t", RUN_TS, total=10)
        assert rows[0]["status"] == "PASS"

    def test_fail_when_empty(self, spark):
        rows = check_empty_table(None, "t", RUN_TS, total=0)
        assert rows[0]["status"] == "FAIL"
        assert rows[0]["failed_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# check_nulls
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckNulls:
    def test_pass_when_no_nulls(self, spark):
        df = _df(spark, [("a",), ("b",)], schema="id STRING")
        rows = check_nulls(df, "t", ["id"], RUN_TS, total=2)
        assert all(r["status"] == "PASS" for r in rows)

    def test_fail_when_null_present(self, spark):
        df = _df(spark, [("a",), (None,)], schema="id STRING")
        rows = check_nulls(df, "t", ["id"], RUN_TS, total=2)
        assert rows[0]["status"] == "FAIL"
        assert rows[0]["failed_count"] == 1

    def test_skip_column_not_in_df(self, spark):
        df = _df(spark, [("a",)], schema="id STRING")
        rows = check_nulls(df, "t", ["missing_col"], RUN_TS, total=1)
        assert rows == []

    def test_warn_threshold_produces_warning(self, spark):
        # 5 nulls out of 1 000 = 0.5 % (above warn=0.1%, below fail=1%)
        data = [(None,)] * 5 + [("x",)] * 995
        df = _df(spark, data, schema="id STRING")
        rows = check_nulls(df, "t", ["id"], RUN_TS, total=1000)
        assert rows[0]["status"] == "WARNING"


# ─────────────────────────────────────────────────────────────────────────────
# check_duplicates
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckDuplicates:
    def test_pass_when_unique(self, spark):
        df = _df(spark, [("a",), ("b",), ("c",)], schema="id STRING")
        rows = check_duplicates(df, "t", ["id"], RUN_TS, total=3)
        assert rows[0]["status"] == "PASS"
        assert rows[0]["failed_count"] == 0

    def test_fail_when_duplicates(self, spark):
        df = _df(spark, [("a",), ("a",), ("b",)], schema="id STRING")
        rows = check_duplicates(df, "t", ["id"], RUN_TS, total=3)
        assert rows[0]["status"] == "FAIL"
        assert rows[0]["failed_count"] == 1

    def test_composite_key(self, spark):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("item_id", IntegerType()),
        ])
        df = _df(spark, [("o1", 1), ("o1", 2), ("o1", 1)], schema=schema)
        rows = check_duplicates(df, "t", ["order_id", "item_id"], RUN_TS, total=3)
        assert rows[0]["failed_count"] == 1

    def test_empty_pk_list_returns_no_results(self, spark):
        df = _df(spark, [("a",)], schema="id STRING")
        rows = check_duplicates(df, "t", ["missing"], RUN_TS, total=1)
        assert rows == []


# ─────────────────────────────────────────────────────────────────────────────
# check_valid_values
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckValidValues:
    _ALLOWED = ["shipped", "delivered", "canceled"]

    def test_pass_when_all_valid(self, spark):
        df = _df(spark, [("shipped",), ("delivered",)], schema="status STRING")
        rows = check_valid_values(df, "t", {"status": self._ALLOWED}, RUN_TS, total=2)
        assert rows[0]["status"] == "PASS"

    def test_fail_when_invalid_present(self, spark):
        df = _df(spark, [("shipped",), ("unknown",)], schema="status STRING")
        rows = check_valid_values(df, "t", {"status": self._ALLOWED}, RUN_TS, total=2)
        assert rows[0]["status"] == "FAIL"
        assert rows[0]["failed_count"] == 1

    def test_null_is_tolerated(self, spark):
        df = _df(spark, [("shipped",), (None,)], schema="status STRING")
        rows = check_valid_values(df, "t", {"status": self._ALLOWED}, RUN_TS, total=2)
        assert rows[0]["status"] == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# check_range
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckRange:
    def test_pass_within_range(self, spark):
        df = _df(spark, [(3,), (5,)], schema="score INT")
        rows = check_range(df, "t", {"score": {"min": 1, "max": 5}}, RUN_TS, total=2)
        assert rows[0]["status"] == "PASS"

    def test_fail_below_min(self, spark):
        df = _df(spark, [(0,), (3,)], schema="score INT")
        rows = check_range(df, "t", {"score": {"min": 1, "max": 5}}, RUN_TS, total=2)
        assert rows[0]["status"] == "FAIL"
        assert rows[0]["failed_count"] == 1

    def test_fail_above_max(self, spark):
        df = _df(spark, [(3,), (6,)], schema="score INT")
        rows = check_range(df, "t", {"score": {"min": 1, "max": 5}}, RUN_TS, total=2)
        assert rows[0]["status"] == "FAIL"

    def test_null_values_tolerated(self, spark):
        df = _df(spark, [(3,), (None,)], schema="score INT")
        rows = check_range(df, "t", {"score": {"min": 1, "max": 5}}, RUN_TS, total=2)
        assert rows[0]["status"] == "PASS"

    def test_min_only_rule(self, spark):
        df = _df(spark, [(-1,), (10,)], schema="price INT")
        rows = check_range(df, "t", {"price": {"min": 0}}, RUN_TS, total=2)
        assert rows[0]["failed_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# run_dq_checks (orchestrator)
# ─────────────────────────────────────────────────────────────────────────────

class TestRunDqChecks:
    def _make_config(self) -> TableConfig:
        return TableConfig(
            name="test_orders",
            primary_keys=["order_id"],
            scd_type=2,
            tracked_columns=["status"],
            not_null_cols=["order_id"],
            dq_rules={"amount": {"min": 0}},
            valid_values={"status": ["open", "closed"]},
        )

    def test_returns_dataframe_with_correct_schema(self, spark):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("status", StringType()),
            StructField("amount", IntegerType()),
        ])
        df = spark.createDataFrame([("o1", "open", 10), ("o2", "closed", 20)], schema=schema)
        config = self._make_config()
        report = run_dq_checks(spark, df, config, RUN_TS)
        assert "table_name" in report.columns
        assert "status" in report.columns
        assert report.count() > 0

    def test_reports_fail_for_null_primary_key(self, spark):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("status", StringType()),
            StructField("amount", IntegerType()),
        ])
        df = spark.createDataFrame([(None, "open", 10), ("o2", "closed", 20)], schema=schema)
        config = self._make_config()
        report = run_dq_checks(spark, df, config, RUN_TS)
        from pyspark.sql import functions as F
        fail_rows = report.filter(
            (F.col("check_name") == "null_check__order_id") & (F.col("status") == "FAIL")
        ).count()
        assert fail_rows == 1

    def test_all_pass_for_clean_data(self, spark):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("status", StringType()),
            StructField("amount", IntegerType()),
        ])
        df = spark.createDataFrame([("o1", "open", 10), ("o2", "closed", 20)], schema=schema)
        config = self._make_config()
        report = run_dq_checks(spark, df, config, RUN_TS)
        from pyspark.sql import functions as F
        fail_count = report.filter(F.col("status") == "FAIL").count()
        assert fail_count == 0


# ─────────────────────────────────────────────────────────────────────────────
# assert_no_critical_failures
# ─────────────────────────────────────────────────────────────────────────────

class TestAssertNoCriticalFailures:
    def test_raises_on_fail(self, spark):
        from data_quality import DQ_REPORT_SCHEMA

        rows = [{
            "run_timestamp": RUN_TS,
            "table_name": "t",
            "check_name": "null_check__id",
            "check_type": "nullness",
            "column_name": "id",
            "failed_count": 5,
            "total_count": 10,
            "pass_rate": 0.5,
            "status": "FAIL",
            "details": "5 NULLs",
        }]
        report = spark.createDataFrame(rows, schema=DQ_REPORT_SCHEMA)
        with pytest.raises(ValueError, match="null_check__id"):
            assert_no_critical_failures(report, "t")

    def test_no_raise_when_pass(self, spark):
        from data_quality import DQ_REPORT_SCHEMA

        rows = [{
            "run_timestamp": RUN_TS,
            "table_name": "t",
            "check_name": "null_check__id",
            "check_type": "nullness",
            "column_name": "id",
            "failed_count": 0,
            "total_count": 10,
            "pass_rate": 1.0,
            "status": "PASS",
            "details": "0 NULLs",
        }]
        report = spark.createDataFrame(rows, schema=DQ_REPORT_SCHEMA)
        assert_no_critical_failures(report, "t")  # should not raise
