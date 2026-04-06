import os

# Must be set before ANY kaggle import
os.environ["KAGGLE_CONFIG_DIR"] = "/tmp"
os.environ["KAGGLE_USERNAME"] = "placeholder"
os.environ["KAGGLE_KEY"] = "placeholder"

import json
import logging
import glob
import stat
import boto3
from botocore.exceptions import ClientError
from kaggle.api.kaggle_api_extended import KaggleApi

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

REGION = "ap-south-1"
SECRET_NAME = "olist/kaggle-credentials"
RAW_BUCKET = os.environ.get("RAW_BUCKET", "olist-raw-105906274703")
BRONZE_PREFIX = "bronze/olist"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_DIR = "/tmp/olist"


def get_kaggle_credentials() -> dict:
    logger.info(f"Fetching credentials from Secrets Manager: {SECRET_NAME}")
    client = boto3.client("secretsmanager", region_name=REGION)
    try:
        response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response["SecretString"])
        logger.info("Credentials fetched successfully.")
        return secret
    except ClientError as e:
        logger.error(f"Failed to fetch secret: {e}")
        raise


def setup_kaggle_auth(username: str, key: str):
    logger.info("Writing kaggle.json to /tmp")
    kaggle_json_path = "/tmp/kaggle.json"
    with open(kaggle_json_path, "w") as f:
        json.dump({"username": username, "key": key}, f)
    os.chmod(kaggle_json_path, stat.S_IRUSR | stat.S_IWUSR)
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key
    logger.info("kaggle.json written with chmod 600")


def download_dataset() -> str:
    logger.info(f"Downloading dataset '{KAGGLE_DATASET}' to {DOWNLOAD_DIR}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=DOWNLOAD_DIR, unzip=True)
    logger.info("Dataset downloaded and unzipped successfully.")
    return DOWNLOAD_DIR


def upload_to_bronze(download_dir: str) -> list:
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


def lambda_handler(event, context):
    logger.info("Lambda started — Bronze ingestion")
    try:
        credentials = get_kaggle_credentials()
        logger.info(f"Username: {credentials['KAGGLE_USERNAME'][:3]}***")
        setup_kaggle_auth(credentials["KAGGLE_USERNAME"], credentials["KAGGLE_KEY"])
        download_dir = download_dataset()
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
