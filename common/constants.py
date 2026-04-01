import os

# AWS
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET", "olist-raw")
S3_PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET", "olist-processed")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "olist-events")

# S3 prefixes
RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"

# Olist dataset tables
OLIST_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "product_category_name_translation",
    "geolocation",
]

# Date formats
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
