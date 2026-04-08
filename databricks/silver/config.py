"""
Configuration for the Silver layer pipeline.
Contains S3 paths, per-table metadata, DQ thresholds, and SCD column names.
"""
from dataclasses import dataclass, field
from typing import Dict, List

# ─────────────────────────────────────────────────────────────────────────────
# S3 Paths
# ─────────────────────────────────────────────────────────────────────────────
RAW_BUCKET = "olist-raw-105906274703"
PROCESSED_BUCKET = "olist-processed-105906274703"
BRONZE_PREFIX = "bronze/olist"
SILVER_PREFIX = "silver"
DQ_REPORTS_PREFIX = "dq_reports"

BRONZE_BASE_PATH = f"s3://{RAW_BUCKET}/{BRONZE_PREFIX}"
SILVER_BASE_PATH = f"s3://{PROCESSED_BUCKET}/{SILVER_PREFIX}"
DQ_REPORTS_PATH = f"s3://{PROCESSED_BUCKET}/{DQ_REPORTS_PREFIX}"

# ─────────────────────────────────────────────────────────────────────────────
# SCD Type 2 internal column names
# ─────────────────────────────────────────────────────────────────────────────
SCD2_START_COL = "eff_start_dt"
SCD2_END_COL = "eff_end_dt"
SCD2_CURRENT_COL = "is_current"
SCD2_HASH_COL = "row_hash"

SCD_INTERNAL_COLS = frozenset({SCD2_START_COL, SCD2_END_COL, SCD2_CURRENT_COL, SCD2_HASH_COL})

# ─────────────────────────────────────────────────────────────────────────────
# DQ thresholds
# ─────────────────────────────────────────────────────────────────────────────
DQ_FAIL_THRESHOLD = 0.01    # > 1 % failures  → FAIL
DQ_WARN_THRESHOLD = 0.001   # > 0.1 % failures → WARNING


# ─────────────────────────────────────────────────────────────────────────────
# Per-table configuration
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class TableConfig:
    name: str                               # Matches CSV stem (without .csv)
    primary_keys: List[str]                 # Business PK columns
    scd_type: int                           # 1 = upsert, 2 = full history
    tracked_columns: List[str]              # Columns that trigger SCD-2 history
    not_null_cols: List[str]                # Must-not-be-null columns
    partition_cols: List[str] = field(default_factory=list)         # Delta partition cols
    zorder_cols: List[str] = field(default_factory=list)            # Z-order cols for OPTIMIZE
    dq_rules: Dict[str, Dict] = field(default_factory=dict)        # Numeric range rules
    valid_values: Dict[str, List] = field(default_factory=dict)    # Allowed categorical values


TABLE_CONFIGS: Dict[str, TableConfig] = {
    "olist_customers_dataset": TableConfig(
        name="olist_customers_dataset",
        primary_keys=["customer_id"],
        scd_type=2,
        tracked_columns=[
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        not_null_cols=["customer_id", "customer_unique_id"],
        partition_cols=["customer_state"],
        zorder_cols=["customer_id", "customer_unique_id"],
    ),
    "olist_sellers_dataset": TableConfig(
        name="olist_sellers_dataset",
        primary_keys=["seller_id"],
        scd_type=2,
        tracked_columns=["seller_zip_code_prefix", "seller_city", "seller_state"],
        not_null_cols=["seller_id"],
        partition_cols=["seller_state"],
        zorder_cols=["seller_id"],
    ),
    "olist_products_dataset": TableConfig(
        name="olist_products_dataset",
        primary_keys=["product_id"],
        scd_type=2,
        tracked_columns=[
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_photos_qty",
        ],
        not_null_cols=["product_id"],
        zorder_cols=["product_id", "product_category_name"],
        dq_rules={
            "product_weight_g": {"min": 0},
            "product_length_cm": {"min": 0},
            "product_height_cm": {"min": 0},
            "product_width_cm": {"min": 0},
        },
    ),
    "olist_orders_dataset": TableConfig(
        name="olist_orders_dataset",
        primary_keys=["order_id"],
        scd_type=2,
        tracked_columns=[
            "order_status",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
        ],
        not_null_cols=["order_id", "customer_id", "order_status"],
        zorder_cols=["order_id", "customer_id"],
        valid_values={
            "order_status": [
                "created",
                "approved",
                "invoiced",
                "processing",
                "shipped",
                "delivered",
                "unavailable",
                "canceled",
            ]
        },
    ),
    "olist_order_items_dataset": TableConfig(
        name="olist_order_items_dataset",
        primary_keys=["order_id", "order_item_id"],
        scd_type=1,
        tracked_columns=["price", "freight_value"],
        not_null_cols=["order_id", "order_item_id", "product_id", "seller_id"],
        zorder_cols=["order_id"],
        dq_rules={
            "price": {"min": 0},
            "freight_value": {"min": 0},
        },
    ),
    "olist_order_payments_dataset": TableConfig(
        name="olist_order_payments_dataset",
        primary_keys=["order_id", "payment_sequential"],
        scd_type=1,
        tracked_columns=["payment_value"],
        not_null_cols=["order_id", "payment_sequential", "payment_type"],
        zorder_cols=["order_id"],
        dq_rules={
            "payment_value": {"min": 0},
            "payment_installments": {"min": 1},
        },
        valid_values={
            "payment_type": [
                "credit_card",
                "boleto",
                "voucher",
                "debit_card",
                "not_defined",
            ]
        },
    ),
    "olist_order_reviews_dataset": TableConfig(
        name="olist_order_reviews_dataset",
        primary_keys=["review_id"],
        scd_type=2,
        tracked_columns=["review_score", "review_comment_message"],
        not_null_cols=["review_id", "order_id", "review_score"],
        zorder_cols=["order_id"],
        dq_rules={
            "review_score": {"min": 1, "max": 5},
        },
    ),
    "olist_geolocation_dataset": TableConfig(
        name="olist_geolocation_dataset",
        primary_keys=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
        scd_type=1,
        tracked_columns=["geolocation_city", "geolocation_state"],
        not_null_cols=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
        partition_cols=["geolocation_state"],
        zorder_cols=["geolocation_zip_code_prefix"],
        dq_rules={
            "geolocation_lat": {"min": -90, "max": 90},
            "geolocation_lng": {"min": -180, "max": 180},
        },
    ),
    "product_category_name_translation": TableConfig(
        name="product_category_name_translation",
        primary_keys=["product_category_name"],
        scd_type=2,
        tracked_columns=["product_category_name_english"],
        not_null_cols=["product_category_name"],
        zorder_cols=["product_category_name"],
    ),
}
