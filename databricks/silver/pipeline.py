"""
Silver layer pipeline for the Olist dataset.

Entry point for a Databricks Job or local execution.

Flow per table
--------------
1. Read CSV from S3 Bronze layer.
2. Drop corrupt records (PERMISSIVE mode).
3. Cache and run all data quality checks → accumulate DQ report.
4. Halt (or warn) on FAIL-level DQ findings.
5. Evolve target Delta schema if new source columns are detected.
6. Align source DataFrame types/columns to the current target schema.
7. Apply SCD Type 2 or Type 1 merge into Silver Delta table.
8. OPTIMIZE + Z-ORDER the updated table for query performance.

Usage
-----
python pipeline.py                          # process all tables
python pipeline.py olist_orders_dataset     # process specific tables
"""
import sys
from datetime import datetime
from functools import reduce
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from config import BRONZE_BASE_PATH, SILVER_BASE_PATH, DQ_REPORTS_PATH, TABLE_CONFIGS, TableConfig
    from data_quality import assert_no_critical_failures, run_dq_checks
    from schema_manager import align_source_to_target, evolve_delta_schema
    from scd_processor import apply_scd_type1, apply_scd_type2
    from utils import get_logger, get_spark, pipeline_run_timestamp
except ImportError:
    from .config import BRONZE_BASE_PATH, SILVER_BASE_PATH, DQ_REPORTS_PATH, TABLE_CONFIGS, TableConfig
    from .data_quality import assert_no_critical_failures, run_dq_checks
    from .schema_manager import align_source_to_target, evolve_delta_schema
    from .scd_processor import apply_scd_type1, apply_scd_type2
    from .utils import get_logger, get_spark, pipeline_run_timestamp

logger = get_logger("olist.silver.pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# Bronze reader
# ─────────────────────────────────────────────────────────────────────────────

def read_bronze_csv(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """
    Read a CSV from the S3 Bronze layer.

    Uses PERMISSIVE mode so corrupt rows are captured in _corrupt_record
    rather than aborting the whole load.  Returns None if the file is missing.
    """
    path = f"{BRONZE_BASE_PATH}/{table_name}.csv"
    logger.info(f"Reading bronze CSV: {path}")
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(path)
        )
        logger.info(f"  → {df.count():,} rows read from {table_name}")
        return df
    except Exception as exc:
        logger.warning(f"  → Could not read {path}: {exc}")
        return None


def _drop_corrupt_records(df: DataFrame, table_name: str) -> DataFrame:
    """Remove rows flagged by PERMISSIVE mode and drop the sentinel column."""
    if "_corrupt_record" not in df.columns:
        return df
    n_corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
    if n_corrupt:
        logger.warning(f"  → Dropping {n_corrupt} corrupt record(s) from {table_name}")
    return df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")


# ─────────────────────────────────────────────────────────────────────────────
# Per-table processing
# ─────────────────────────────────────────────────────────────────────────────

def process_table(
    spark: SparkSession,
    table_name: str,
    run_ts: datetime,
    dq_accumulator: List[DataFrame],
    fail_on_dq_error: bool = True,
) -> None:
    config: TableConfig = TABLE_CONFIGS[table_name]
    silver_path = f"{SILVER_BASE_PATH}/{table_name}"

    logger.info(f"━━━ [{table_name}] ━━━")

    # 1. Read bronze
    source_df = read_bronze_csv(spark, table_name)
    if source_df is None:
        logger.warning(f"  → Skipping {table_name}: file not found in bronze layer.")
        return

    # 2. Drop corrupt rows
    source_df = _drop_corrupt_records(source_df, table_name)

    # 3. Data quality checks
    dq_report = run_dq_checks(spark, source_df, config, run_ts)
    dq_accumulator.append(dq_report)

    if fail_on_dq_error:
        assert_no_critical_failures(dq_report, table_name)
    else:
        # Log warnings without stopping the pipeline
        fails = dq_report.filter(F.col("status") == "FAIL").collect()
        if fails:
            logger.warning(f"  → DQ issues (non-fatal): {[r['check_name'] for r in fails]}")

    # 4. Schema evolution — add new source columns to existing Delta table
    evolve_delta_schema(spark, source_df, silver_path)

    # 5. Align source to (possibly updated) target schema
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, silver_path):
            target_schema = DeltaTable.forPath(spark, silver_path).toDF().schema
            source_df = align_source_to_target(source_df, target_schema)
    except ImportError:
        pass  # Delta not available locally

    # 6. SCD merge
    if config.scd_type == 2:
        apply_scd_type2(spark, source_df, silver_path, config, run_ts)
    else:
        apply_scd_type1(spark, source_df, silver_path, config)

    logger.info(f"  → Written to {silver_path}")

    # 7. Optimize
    _optimize_table(spark, silver_path, config)


def _optimize_table(spark: SparkSession, path: str, config: TableConfig) -> None:
    """Run OPTIMIZE (+ Z-ORDER) on the Silver Delta table for query performance."""
    try:
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark, path):
            return
    except ImportError:
        return

    try:
        ref = f"delta.`{path}`"
        if config.zorder_cols:
            cols = ", ".join(config.zorder_cols)
            spark.sql(f"OPTIMIZE {ref} ZORDER BY ({cols})")
            logger.info(f"  → OPTIMIZE ZORDER BY ({cols})")
        else:
            spark.sql(f"OPTIMIZE {ref}")
            logger.info("  → OPTIMIZE complete")
    except Exception as exc:
        logger.warning(f"  → OPTIMIZE skipped (non-critical): {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# DQ report writer
# ─────────────────────────────────────────────────────────────────────────────

def write_dq_report(
    spark: SparkSession,
    dq_reports: List[DataFrame],
    run_ts: datetime,
) -> None:
    """Union all per-table DQ reports and write to S3 as a Delta table."""
    if not dq_reports:
        return

    combined = reduce(DataFrame.unionByName, dq_reports)
    ts_str = run_ts.strftime("%Y%m%d_%H%M%S")
    report_path = f"{DQ_REPORTS_PATH}/run_ts={ts_str}"

    (
        combined.coalesce(1)
        .write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .save(report_path)
    )
    logger.info(f"DQ report → {report_path}")

    logger.info("━━━ DQ Summary ━━━")
    combined.groupBy("table_name", "status").count().orderBy("table_name", "status").show(
        100, truncate=False
    )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(
    tables: Optional[List[str]] = None,
    fail_on_dq_error: bool = True,
) -> None:
    """
    Execute the Silver pipeline for the specified tables (all by default).

    Parameters
    ----------
    tables : list of str, optional
        Table names from TABLE_CONFIGS to process.  Defaults to all.
    fail_on_dq_error : bool
        When True, a FAIL-level DQ finding halts the pipeline for that table.
    """
    spark = get_spark()
    run_ts = pipeline_run_timestamp()
    tables = tables or list(TABLE_CONFIGS.keys())
    dq_reports: List[DataFrame] = []

    logger.info(f"Silver pipeline started — run_ts={run_ts}, tables={tables}")

    for table_name in tables:
        if table_name not in TABLE_CONFIGS:
            logger.warning(f"Unknown table '{table_name}' — skipping.")
            continue
        try:
            process_table(spark, table_name, run_ts, dq_reports, fail_on_dq_error)
        except ValueError as exc:
            logger.error(str(exc))
            if fail_on_dq_error:
                raise

    write_dq_report(spark, dq_reports, run_ts)
    logger.info("Silver pipeline complete.")


if __name__ == "__main__":
    selected = sys.argv[1:] or None
    run(tables=selected)
