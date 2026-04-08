"""
SCD Type 2 (full history) and SCD Type 1 (upsert) processor using Delta Lake MERGE.

SCD Type 2 strategy — two-pass merge:
  Pass 1  Expire changed records
          Condition: same PK + is_current=true + row_hash changed
          Action:    set is_current=false, eff_end_dt=run_ts

  Pass 2  Insert new / updated records
          Source:  incoming rows whose PK has no current counterpart in target
                   (brand-new PKs  +  PKs just expired in pass 1)
          Action:  whenNotMatchedInsertAll

This two-pass approach is standard for Delta Lake SCD-2 because a single MERGE
cannot both close an old version and open a new one for the same business key.

SCD Type 1 strategy — standard upsert:
  Single MERGE: whenMatchedUpdateAll + whenNotMatchedInsertAll
"""
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

try:
    from config import (
        SCD2_CURRENT_COL,
        SCD2_END_COL,
        SCD2_HASH_COL,
        SCD2_START_COL,
        TableConfig,
    )
except ImportError:
    from .config import (
        SCD2_CURRENT_COL,
        SCD2_END_COL,
        SCD2_HASH_COL,
        SCD2_START_COL,
        TableConfig,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _add_scd2_metadata(
    df: DataFrame,
    tracked_columns: List[str],
    run_ts: datetime,
) -> DataFrame:
    """
    Attach SCD Type 2 metadata columns to a source DataFrame.

    row_hash is computed over tracked_columns only (using COALESCE to handle
    NULLs deterministically so a NULL → value change is detected).
    """
    existing_tracked = [c for c in tracked_columns if c in df.columns]
    if not existing_tracked:
        # No tracked columns present — hash on all non-system columns
        existing_tracked = df.columns

    hash_input = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in existing_tracked],
    )
    return (
        df
        .withColumn(SCD2_HASH_COL, F.sha2(hash_input, 256))
        .withColumn(SCD2_START_COL, F.lit(run_ts).cast(TimestampType()))
        .withColumn(SCD2_END_COL, F.lit(None).cast(TimestampType()))
        .withColumn(SCD2_CURRENT_COL, F.lit(True))
    )


def _pk_condition(primary_keys: List[str], left: str = "tgt", right: str = "src") -> str:
    """Build SQL join condition string for primary key equality."""
    return " AND ".join(f"{left}.{pk} = {right}.{pk}" for pk in primary_keys)


def _initial_write(df: DataFrame, path: str, config: TableConfig) -> None:
    """Write initial Delta table, enabling auto-optimize properties."""
    writer = (
        df.write
        .format("delta")
        .option("mergeSchema", "true")
    )
    if config.partition_cols:
        writer = writer.partitionBy(*config.partition_cols)
    writer.save(path)
    _set_table_properties(df.sparkSession, path)


def _set_table_properties(spark: SparkSession, path: str) -> None:
    """Enable Delta auto-compaction, optimized writes, and CDF."""
    ref = f"delta.`{path}`"
    for key, val in {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
    }.items():
        try:
            spark.sql(f"ALTER TABLE {ref} SET TBLPROPERTIES ('{key}' = '{val}')")
        except Exception:
            pass  # Non-critical; may not be supported in all Delta versions


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
    config: TableConfig,
    run_ts: datetime,
) -> None:
    """
    Merge source_df into a Delta table using SCD Type 2 (full history tracking).

    After this call:
      - Unchanged records remain as-is (is_current=true).
      - Changed records have their old version closed (is_current=false, eff_end_dt set)
        and a new current version inserted.
      - New PKs are inserted as current records.
      - Deleted PKs (not in source) retain their last state as is_current=true
        (no hard-delete; treat as late-arriving data on the next run if needed).
    """
    from delta.tables import DeltaTable

    source = _add_scd2_metadata(source_df, config.tracked_columns, run_ts)
    pk_cond = _pk_condition(config.primary_keys)

    if not DeltaTable.isDeltaTable(spark, target_table_path):
        _initial_write(source, target_table_path, config)
        return

    delta_tbl = DeltaTable.forPath(spark, target_table_path)

    # ── Pass 1: expire changed records ────────────────────────────────────────
    (
        delta_tbl.alias("tgt")
        .merge(
            source.alias("src"),
            f"{pk_cond} AND tgt.{SCD2_CURRENT_COL} = true "
            f"AND tgt.{SCD2_HASH_COL} != src.{SCD2_HASH_COL}",
        )
        .whenMatchedUpdate(set={
            SCD2_CURRENT_COL: F.lit(False),
            SCD2_END_COL: F.col(f"src.{SCD2_START_COL}"),
        })
        .execute()
    )

    # ── Pass 2: insert new and changed records ────────────────────────────────
    # After pass 1, changed records are expired → left_anti against current
    # records yields both brand-new PKs and just-expired PKs.
    current_pks = (
        delta_tbl.toDF()
        .filter(F.col(SCD2_CURRENT_COL) == True)  # noqa: E712
        .select(config.primary_keys)
        .cache()
    )

    records_to_insert = source.join(current_pks, config.primary_keys, "left_anti")

    # Only execute if there is something to insert (avoids a no-op MERGE)
    if records_to_insert.count() > 0:
        (
            delta_tbl.alias("tgt")
            .merge(
                records_to_insert.alias("src"),
                f"{pk_cond} AND tgt.{SCD2_CURRENT_COL} = true",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    current_pks.unpersist()


def apply_scd_type1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
    config: TableConfig,
) -> None:
    """
    Merge source_df into a Delta table using SCD Type 1 (no history tracking).
    Existing records are overwritten; new records are inserted.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target_table_path):
        _initial_write(source_df, target_table_path, config)
        return

    delta_tbl = DeltaTable.forPath(spark, target_table_path)
    pk_cond = _pk_condition(config.primary_keys)

    (
        delta_tbl.alias("tgt")
        .merge(source_df.alias("src"), pk_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
