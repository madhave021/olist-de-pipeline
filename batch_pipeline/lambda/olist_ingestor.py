import os
import json
import logging
import glob
import boto3
from botocore.exceptions import ClientError

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
REGION = "ap-south-1"
SECRET_NAME = "olist/kaggle-credentials"
RAW_BUCKET = os.environ.get("RAW_BUCKET", "olist-raw-105906274703")
BRONZE_PREFIX = "bronze/olist"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_DIR = "/tmp/olist"


# ── Secrets ───────────────────────────────────────────────────────────────────
def get_kaggle_credentials() -> dict:
    """Fetch KAGGLE_USERNAME and KAGGLE_KEY from AWS Secrets Manager."""
    logger.info(f"Fetching Kaggle credentials from Secrets Manager: {SECRET_NAME}")
    client = boto3.client("secretsmanager", region_name=REGION)
    try:
        response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response["SecretString"])
        logger.info("Kaggle credentials fetched successfully.")
        return secret
    except ClientError as e:
        logger.error(f"Failed to fetch secret '{SECRET_NAME}': {e}")
        raise


# ── Kaggle download ───────────────────────────────────────────────────────────
def download_dataset(kaggle_username: str, kaggle_key: str) -> str:
    """Download the Olist dataset to /tmp using the Kaggle API."""
    os.environ["KAGGLE_USERNAME"] = kaggle_username
    os.environ["KAGGLE_KEY"] = kaggle_key

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    logger.info(f"Downloading dataset '{KAGGLE_DATASET}' to {DOWNLOAD_DIR}")
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=DOWNLOAD_DIR, unzip=True)
    logger.info("Dataset downloaded and unzipped successfully.")
    return DOWNLOAD_DIR


# ── S3 upload ─────────────────────────────────────────────────────────────────
def upload_to_bronze(download_dir: str) -> list[str]:
    """Upload each CSV from download_dir to s3://<RAW_BUCKET>/bronze/olist/<filename>."""
    s3 = boto3.client("s3", region_name=REGION)
    csv_files = glob.glob(os.path.join(download_dir, "*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {download_dir}")
        return []

    uploaded = []
    for local_path in csv_files:
        filename = os.path.basename(local_path)
        s3_key = f"{BRONZE_PREFIX}/{filename}"
        try:
            s3.upload_file(local_path, RAW_BUCKET, s3_key)
            logger.info(f"Uploaded {filename} → s3://{RAW_BUCKET}/{s3_key}")
            uploaded.append(s3_key)
        except ClientError as e:
            logger.error(f"Failed to upload {filename}: {e}")
            raise

    return uploaded


# ── Handler ───────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Bronze ingestion Lambda.
    Fetches Olist dataset from Kaggle and uploads raw CSVs to S3 bronze layer.
    Triggered by EventBridge on weekdays at 18:00 UTC.
    """
    logger.info("Starting Bronze ingestion.")
    try:
        credentials = get_kaggle_credentials()
        download_dir = download_dataset(
            kaggle_username=credentials["KAGGLE_USERNAME"],
            kaggle_key=credentials["KAGGLE_KEY"],
        )
        uploaded_files = upload_to_bronze(download_dir)

        result = {
            "status": "success",
            "bucket": RAW_BUCKET,
            "files_uploaded": uploaded_files,
            "count": len(uploaded_files),
        }
        logger.info(f"Bronze ingestion complete. {result['count']} file(s) uploaded.")
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}
