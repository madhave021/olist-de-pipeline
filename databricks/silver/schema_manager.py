"""
Schema evolution management for Silver Delta tables.

Responsibilities:
  - Detect new columns in source that are absent from the target Delta table
    and add them via ALTER TABLE (explicit, auditable, avoids silent mergeSchema).
  - Align source DataFrame to the current target schema before MERGE:
      * Cast source columns to target type when a safe widening promotion exists.
      * Supply NULL for target columns absent in source (schema drift / removal).
      * Pass through extra source columns not yet in target (newly added columns).
  - Skip SCD internal columns (eff_start_dt, eff_end_dt, is_current, row_hash) —
    they are managed exclusively by scd_processor.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StringType, StructType

try:
    from config import SCD_INTERNAL_COLS
except ImportError:
    from .config import SCD_INTERNAL_COLS

# Allowed widening type promotions: (source_type_name, target_type_name) → SQL cast
_SAFE_PROMOTIONS: Dict[tuple, str] = {
    ("ShortType", "IntegerType"): "INT",
    ("ShortType", "LongType"): "BIGINT",
    ("IntegerType", "LongType"): "BIGINT",
    ("FloatType", "DoubleType"): "DOUBLE",
    ("IntegerType", "DoubleType"): "DOUBLE",
    ("LongType", "DoubleType"): "DOUBLE",
    ("ByteType", "IntegerType"): "INT",
    ("ByteType", "LongType"): "BIGINT",
    ("ByteType", "ShortType"): "SMALLINT",
}

# Spark DataType class name → SQL DDL keyword
_TYPE_TO_SQL: Dict[str, str] = {
    "StringType": "STRING",
    "IntegerType": "INT",
    "LongType": "BIGINT",
    "ShortType": "SMALLINT",
    "ByteType": "TINYINT",
    "DoubleType": "DOUBLE",
    "FloatType": "FLOAT",
    "BooleanType": "BOOLEAN",
    "TimestampType": "TIMESTAMP",
    "DateType": "DATE",
    "BinaryType": "BINARY",
    "DecimalType": "DECIMAL(38,18)",
}


def _schema_dict(schema: StructType) -> Dict[str, DataType]:
    return {f.name: f.dataType for f in schema.fields}


def _sql_type(dtype: DataType) -> str:
    return _TYPE_TO_SQL.get(type(dtype).__name__, "STRING")


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def evolve_delta_schema(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
) -> None:
    """
    Add new columns from source_df to an existing Delta table.

    Uses explicit ALTER TABLE ADD COLUMNS so that schema changes are visible
    in the Delta transaction log with a clear audit trail.  Falls back to
    mergeSchema=true on writes as a safety net for any remaining gaps.

    No-ops if the table does not yet exist (initial load handles schema creation).
    """
    try:
        from delta.tables import DeltaTable
    except ImportError:
        return  # Delta not available (e.g., pure unit-test environment)

    if not DeltaTable.isDeltaTable(spark, target_table_path):
        return

    existing_cols = {
        f.name
        for f in DeltaTable.forPath(spark, target_table_path).toDF().schema.fields
    }
    source_fields = _schema_dict(source_df.schema)

    new_cols = {
        name: dtype
        for name, dtype in source_fields.items()
        if name not in existing_cols and name not in SCD_INTERNAL_COLS
    }
    if not new_cols:
        return

    col_defs = ", ".join(
        f"`{name}` {_sql_type(dtype)}" for name, dtype in new_cols.items()
    )
    spark.sql(f"ALTER TABLE delta.`{target_table_path}` ADD COLUMNS ({col_defs})")
    print(
        f"[schema_manager] Added {len(new_cols)} column(s) to {target_table_path}: "
        f"{list(new_cols)}"
    )


def align_source_to_target(
    source_df: DataFrame,
    target_schema: StructType,
) -> DataFrame:
    """
    Align source_df to match target_schema before a Delta MERGE:

    For each column in target_schema:
      - Skip SCD internal columns (managed by scd_processor, not from source).
      - If column exists in source with the same type → keep as-is.
      - If column exists in source with a safely promotable type → cast.
      - If column exists in source with an incompatible type → cast to STRING.
      - If column is absent from source → supply NULL cast to target type.

    Additionally, any source columns not present in target_schema are appended
    so that they are written to Delta via mergeSchema=true (new columns that
    evolve_delta_schema may not have caught yet, or on the very first evolve).
    """
    target_fields = _schema_dict(target_schema)
    source_fields = _schema_dict(source_df.schema)

    select_exprs = []

    for col_name, tgt_type in target_fields.items():
        if col_name in SCD_INTERNAL_COLS:
            continue  # SCD metadata is injected by scd_processor

        if col_name in source_fields:
            src_type_name = type(source_fields[col_name]).__name__
            tgt_type_name = type(tgt_type).__name__

            if src_type_name == tgt_type_name:
                select_exprs.append(F.col(col_name))
            elif (src_type_name, tgt_type_name) in _SAFE_PROMOTIONS:
                cast_kw = _SAFE_PROMOTIONS[(src_type_name, tgt_type_name)]
                select_exprs.append(F.col(col_name).cast(cast_kw).alias(col_name))
            else:
                # Unsafe / unknown promotion → stringify to preserve data
                select_exprs.append(F.col(col_name).cast(StringType()).alias(col_name))
        else:
            # Column removed from source or never existed → keep as NULL
            select_exprs.append(F.lit(None).cast(tgt_type).alias(col_name))

    # Pass through genuinely new source columns (not yet in target)
    for col_name in source_df.columns:
        if col_name not in target_fields and col_name not in SCD_INTERNAL_COLS:
            select_exprs.append(F.col(col_name))

    return source_df.select(select_exprs)
