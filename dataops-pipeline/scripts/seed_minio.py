"""Seed MinIO with Olist CSV files from the local ``data/`` directory.

Uploads all ``*.csv`` files found in ``./data/`` to the ``raw`` bucket
in MinIO. Expects MinIO to be running and buckets to be already created
(see ``minio-init`` service in docker-compose).
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import structlog

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def get_s3_client() -> boto3.client:
    """Create a boto3 S3 client configured for MinIO.

    Returns:
        Configured boto3 S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def upload_csv_files(
    data_dir: Path | None = None,
    bucket: str | None = None,
) -> int:
    """Upload all CSV files in ``data_dir`` to the given S3/MinIO bucket.

    Args:
        data_dir: Local directory containing CSV files. Defaults to ``./data/``.
        bucket: Target bucket name. Defaults to ``RAW_BUCKET`` env var.

    Returns:
        Number of files uploaded.
    """
    data_path = data_dir or DATA_DIR
    target_bucket = bucket or os.environ.get("RAW_BUCKET", "raw")

    if not data_path.exists():
        logger.warning(
            "data_directory_not_found",
            path=str(data_path),
            hint="Download the Olist dataset from Kaggle and place CSVs in ./data/",
        )
        return 0

    csv_files = list(data_path.glob("*.csv"))
    if not csv_files:
        logger.warning("no_csv_files_found", path=str(data_path))
        return 0

    s3 = get_s3_client()
    uploaded = 0

    for csv_file in csv_files:
        key = csv_file.name
        logger.info("uploading_file", file=key, bucket=target_bucket)
        s3.upload_file(str(csv_file), target_bucket, key)
        uploaded += 1

    logger.info("seed_complete", files_uploaded=uploaded, bucket=target_bucket)
    return uploaded


if __name__ == "__main__":
    count = upload_csv_files()
    if count == 0:
        logger.warning(
            "no_files_uploaded",
            action="Download the Olist dataset: kaggle datasets download -d olistbr/brazilian-ecommerce",
        )
