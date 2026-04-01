import boto3
from botocore.exceptions import ClientError
from common.logger import get_logger
from common.constants import AWS_REGION

logger = get_logger(__name__)


def get_s3_client(region: str = AWS_REGION):
    return boto3.client("s3", region_name=region)


def upload_file(local_path: str, bucket: str, s3_key: str, region: str = AWS_REGION) -> bool:
    """Upload a local file to S3. Returns True on success."""
    client = get_s3_client(region)
    try:
        client.upload_file(local_path, bucket, s3_key)
        logger.info(f"Uploaded {local_path} → s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Upload failed: {e}")
        return False


def download_file(bucket: str, s3_key: str, local_path: str, region: str = AWS_REGION) -> bool:
    """Download a file from S3 to a local path. Returns True on success."""
    client = get_s3_client(region)
    try:
        client.download_file(bucket, s3_key, local_path)
        logger.info(f"Downloaded s3://{bucket}/{s3_key} → {local_path}")
        return True
    except ClientError as e:
        logger.error(f"Download failed: {e}")
        return False


def list_objects(bucket: str, prefix: str = "", region: str = AWS_REGION) -> list[str]:
    """Return a list of S3 keys under the given prefix."""
    client = get_s3_client(region)
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def key_exists(bucket: str, s3_key: str, region: str = AWS_REGION) -> bool:
    """Check whether an S3 object exists."""
    client = get_s3_client(region)
    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError:
        return False
