"""Silver layer transformation for Olist orders.

Reads from the Bronze Delta table, applies type casting, deduplication,
null handling, and filtering, then writes the cleaned result as a Silver
Delta table.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = structlog.get_logger(__name__)

# Make sibling package importable via spark-submit.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from ingestion.spark_session import get_spark_session  # noqa: E402


def cast_timestamps(df: DataFrame) -> DataFrame:
    """Cast string timestamp columns to proper TimestampType.

    Args:
        df: Bronze DataFrame with raw string columns.

    Returns:
        DataFrame with timestamp columns properly typed.
    """
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col_name in timestamp_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(T.TimestampType()))
    return df


def calculate_delivery_days(df: DataFrame) -> DataFrame:
    """Compute ``delivery_days`` as the difference between delivery and purchase.

    Missing delivery dates result in ``-1`` as a sentinel value.

    Args:
        df: DataFrame with cast timestamp columns.

    Returns:
        DataFrame with ``delivery_days`` column added.
    """
    return df.withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        ).otherwise(F.lit(-1)),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep only the latest ingestion of each ``order_id``.

    Uses ``ROW_NUMBER()`` window function partitioned by ``order_id`` and
    ordered by ``_ingested_at DESC``.

    Args:
        df: DataFrame that may contain duplicate order_ids across batches.

    Returns:
        Deduplicated DataFrame.
    """
    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def filter_invalid_status(df: DataFrame) -> DataFrame:
    """Remove orders with ``order_status == 'unavailable'``.

    Args:
        df: Input DataFrame.

    Returns:
        Filtered DataFrame.
    """
    return df.filter(F.col("order_status") != "unavailable")


def transform_silver(
    spark: SparkSession,
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None,
) -> int:
    """Run the full Bronze-to-Silver transformation pipeline.

    Args:
        spark: Active SparkSession.
        bronze_path: S3a path of the Bronze Delta table.
        silver_path: S3a path for the Silver Delta table output.

    Returns:
        Number of rows written.
    """
    bronze = bronze_path or f"s3a://{os.environ.get('BRONZE_BUCKET', 'bronze')}/orders"
    silver = silver_path or f"s3a://{os.environ.get('SILVER_BUCKET', 'silver')}/orders"

    logger.info("reading_bronze", path=bronze)
    df = spark.read.format("delta").load(bronze)

    df = cast_timestamps(df)
    df = calculate_delivery_days(df)
    df = deduplicate(df)
    df = filter_invalid_status(df)

    row_count: int = df.count()
    logger.info("writing_silver", path=silver, rows=row_count)

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver)

    logger.info("silver_complete", rows=row_count)
    return row_count


if __name__ == "__main__":
    _spark = get_spark_session(app_name="silver-orders")
    transform_silver(_spark)
