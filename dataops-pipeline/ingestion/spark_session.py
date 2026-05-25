"""Factory for creating configured SparkSession instances with Delta Lake support."""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession
import structlog

logger = structlog.get_logger(__name__)


def get_spark_session(
    app_name: str = "dataops-pipeline",
    master: Optional[str] = None,
) -> SparkSession:
    """Create or retrieve a SparkSession configured for Delta Lake and MinIO (S3a).

    Args:
        app_name: Spark application name shown in the UI.
        master: Spark master URL. Falls back to SPARK_MASTER_URL env var,
                then ``local[*]``.

    Returns:
        A fully configured SparkSession instance.
    """
    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    logger.info(
        "creating_spark_session",
        app_name=app_name,
        master=master,
        minio_endpoint=minio_endpoint,
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Delta Lake
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3a / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Delta defaults
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config(
            "spark.databricks.delta.properties.defaults.logRetentionDuration",
            "interval 30 days",
        )
        # Driver memory
        .config(
            "spark.driver.memory",
            os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("spark_session_ready", spark_version=spark.version)
    return spark
