"""
Data quality checks for the Silver layer.

Each check function returns a list of result dicts that are assembled into a
DQ report DataFrame.  Downstream code calls assert_no_critical_failures() to
halt the pipeline when FAIL-level issues are found.
"""
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from config import DQ_FAIL_THRESHOLD, DQ_WARN_THRESHOLD, TableConfig
except ImportError:
    from .config import DQ_FAIL_THRESHOLD, DQ_WARN_THRESHOLD, TableConfig

# ─────────────────────────────────────────────────────────────────────────────
# DQ report schema
# ─────────────────────────────────────────────────────────────────────────────
DQ_REPORT_SCHEMA = StructType([
    StructField("run_timestamp", TimestampType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_type", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("failed_count", LongType(), False),
    StructField("total_count", LongType(), False),
    StructField("pass_rate", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("details", StringType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _status(failed: int, total: int) -> str:
    if total == 0 or failed == 0:
        return "PASS"
    rate = failed / total
    if rate > DQ_FAIL_THRESHOLD:
        return "FAIL"
    if rate > DQ_WARN_THRESHOLD:
        return "WARNING"
    return "PASS"


def _pass_rate(failed: int, total: int) -> float:
    if total == 0:
        return 1.0
    return round(1.0 - failed / total, 6)


def _row(
    run_ts: datetime,
    table_name: str,
    check_name: str,
    check_type: str,
    column_name: str,
    failed: int,
    total: int,
    details: str,
) -> dict:
    return {
        "run_timestamp": run_ts,
        "table_name": table_name,
        "check_name": check_name,
        "check_type": check_type,
        "column_name": column_name,
        "failed_count": failed,
        "total_count": total,
        "pass_rate": _pass_rate(failed, total),
        "status": _status(failed, total),
        "details": details,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def check_empty_table(df: DataFrame, table_name: str, run_ts: datetime, total: int) -> List[dict]:
    """Ensure table has at least one row."""
    failed = 0 if total > 0 else 1
    safe_total = max(total, 1)
    return [_row(
        run_ts, table_name,
        check_name="non_empty_table",
        check_type="completeness",
        column_name=None,
        failed=failed,
        total=safe_total,
        details=f"Table has {total} row(s)",
    )]


def check_nulls(
    df: DataFrame,
    table_name: str,
    not_null_cols: List[str],
    run_ts: datetime,
    total: int,
) -> List[dict]:
    """Check that required columns contain no NULLs."""
    results = []
    for col_name in not_null_cols:
        if col_name not in df.columns:
            continue
        failed = df.filter(F.col(col_name).isNull()).count()
        results.append(_row(
            run_ts, table_name,
            check_name=f"null_check__{col_name}",
            check_type="nullness",
            column_name=col_name,
            failed=failed,
            total=total,
            details=f"{failed} NULL value(s) found in '{col_name}'",
        ))
    return results


def check_duplicates(
    df: DataFrame,
    table_name: str,
    primary_keys: List[str],
    run_ts: datetime,
    total: int,
) -> List[dict]:
    """Check for duplicate primary key combinations."""
    existing_pks = [pk for pk in primary_keys if pk in df.columns]
    if not existing_pks:
        return []
    distinct = df.select(existing_pks).distinct().count()
    failed = total - distinct
    return [_row(
        run_ts, table_name,
        check_name="duplicate_primary_key",
        check_type="uniqueness",
        column_name=",".join(existing_pks),
        failed=failed,
        total=total,
        details=f"{failed} duplicate primary key combination(s)",
    )]


def check_valid_values(
    df: DataFrame,
    table_name: str,
    valid_values: dict,
    run_ts: datetime,
    total: int,
) -> List[dict]:
    """Check categorical columns contain only allowed values (NULLs are tolerated)."""
    results = []
    for col_name, allowed in valid_values.items():
        if col_name not in df.columns:
            continue
        failed = df.filter(
            F.col(col_name).isNotNull() & ~F.col(col_name).isin(allowed)
        ).count()
        results.append(_row(
            run_ts, table_name,
            check_name=f"valid_values__{col_name}",
            check_type="validity",
            column_name=col_name,
            failed=failed,
            total=total,
            details=f"{failed} invalid value(s) in '{col_name}'; allowed: {allowed}",
        ))
    return results


def check_range(
    df: DataFrame,
    table_name: str,
    dq_rules: dict,
    run_ts: datetime,
    total: int,
) -> List[dict]:
    """Check numeric columns fall within [min, max] bounds (NULLs are tolerated)."""
    results = []
    for col_name, rules in dq_rules.items():
        if col_name not in df.columns:
            continue
        min_val = rules.get("min")
        max_val = rules.get("max")

        condition = F.lit(False)
        desc_parts = []
        if min_val is not None:
            condition = condition | (F.col(col_name) < min_val)
            desc_parts.append(f">= {min_val}")
        if max_val is not None:
            condition = condition | (F.col(col_name) > max_val)
            desc_parts.append(f"<= {max_val}")

        failed = df.filter(F.col(col_name).isNotNull() & condition).count()
        results.append(_row(
            run_ts, table_name,
            check_name=f"range_check__{col_name}",
            check_type="range",
            column_name=col_name,
            failed=failed,
            total=total,
            details=f"{failed} out-of-range value(s) in '{col_name}' (expected {', '.join(desc_parts)})",
        ))
    return results


def check_referential_integrity(
    df: DataFrame,
    table_name: str,
    fk_col: str,
    ref_df: DataFrame,
    ref_col: str,
    run_ts: datetime,
    total: int,
) -> List[dict]:
    """Check that FK values in df exist in the reference DataFrame."""
    if fk_col not in df.columns:
        return []
    orphans = (
        df.select(F.col(fk_col))
        .filter(F.col(fk_col).isNotNull())
        .join(ref_df.select(F.col(ref_col).alias(fk_col)), fk_col, "left_anti")
        .count()
    )
    return [_row(
        run_ts, table_name,
        check_name=f"referential_integrity__{fk_col}",
        check_type="referential_integrity",
        column_name=fk_col,
        failed=orphans,
        total=total,
        details=f"{orphans} value(s) in '{fk_col}' not found in reference",
    )]


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    config: TableConfig,
    run_ts: datetime,
) -> DataFrame:
    """
    Execute all DQ checks for a single table and return a DQ report DataFrame.
    The DataFrame is cached before checks to avoid recomputation.
    """
    df.cache()
    total = df.count()

    rows: List[dict] = []
    rows += check_empty_table(df, config.name, run_ts, total)
    rows += check_nulls(df, config.name, config.not_null_cols, run_ts, total)
    rows += check_duplicates(df, config.name, config.primary_keys, run_ts, total)
    rows += check_valid_values(df, config.name, config.valid_values, run_ts, total)
    rows += check_range(df, config.name, config.dq_rules, run_ts, total)

    df.unpersist()
    return spark.createDataFrame(rows, schema=DQ_REPORT_SCHEMA)


def assert_no_critical_failures(report_df: DataFrame, table_name: str) -> None:
    """Raise ValueError if any FAIL-status checks exist for the given table."""
    failures = (
        report_df
        .filter((F.col("table_name") == table_name) & (F.col("status") == "FAIL"))
        .select("check_name")
        .collect()
    )
    if failures:
        names = [r["check_name"] for r in failures]
        raise ValueError(
            f"[DQ FAIL] Table '{table_name}' failed critical checks: {names}. "
            "Fix source data and re-run."
        )
