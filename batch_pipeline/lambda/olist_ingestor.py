import os
import json
import logging
import glob
import zipfile
import boto3
import requests
from botocore.exceptions import ClientError

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
KAGGLE_DOWNLOAD_URL = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"
DOWNLOAD_ZIP = "/tmp/olist.zip"
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


def download_dataset(username: str, key: str) -> str:
    logger.info(f"Downloading dataset from Kaggle API: {KAGGLE_DOWNLOAD_URL}")
    response = requests.get(KAGGLE_DOWNLOAD_URL, auth=(username, key), stream=True)
    response.raise_for_status()
    with open(DOWNLOAD_ZIP, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Download complete, saved to {DOWNLOAD_ZIP}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    with zipfile.ZipFile(DOWNLOAD_ZIP, "r") as z:
        z.extractall(DOWNLOAD_DIR)
    logger.info(f"Extracted to {DOWNLOAD_DIR}")
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
            logger.info(f"Uploaded {filename} -> s3://{RAW_BUCKET}/{s3_key}")
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
        download_dir = download_dataset(credentials["KAGGLE_USERNAME"], credentials["KAGGLE_KEY"])
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
