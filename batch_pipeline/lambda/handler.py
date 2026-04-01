import os
import json
import boto3
from datetime import datetime
from common.logger import get_logger

logger = get_logger(__name__)

RAW_BUCKET = os.environ.get("RAW_BUCKET", "olist-raw-dev")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "olist-processed-dev")

s3 = boto3.client("s3")


def lambda_handler(event, context):
    """
    Entry point for the olist-ingestor Lambda.
    Triggered by EventBridge on weekdays at 18:00 UTC.
    """
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"Starting ingestion run for date={run_date}")

    try:
        result = ingest(run_date)
        logger.info(f"Ingestion complete: {result}")
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def ingest(run_date: str) -> dict:
    """Copy raw Olist CSVs from the raw bucket prefix into a dated partition."""
    prefix = f"raw/{run_date}/"
    response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="raw/")
    objects = response.get("Contents", [])

    if not objects:
        logger.warning(f"No objects found in s3://{RAW_BUCKET}/raw/")
        return {"processed": 0, "run_date": run_date}

    processed = 0
    for obj in objects:
        source_key = obj["Key"]
        filename = source_key.split("/")[-1]
        dest_key = f"{prefix}{filename}"

        s3.copy_object(
            Bucket=PROCESSED_BUCKET,
            CopySource={"Bucket": RAW_BUCKET, "Key": source_key},
            Key=dest_key,
        )
        logger.info(f"Copied s3://{RAW_BUCKET}/{source_key} → s3://{PROCESSED_BUCKET}/{dest_key}")
        processed += 1

    return {"processed": processed, "run_date": run_date}
