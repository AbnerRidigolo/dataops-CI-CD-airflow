"""Ingest Olist orders CSV from MinIO (raw) into Delta Lake (bronze).

Reads the raw CSV, adds audit columns, and performs an idempotent upsert
(MERGE) by ``order_id`` into the Bronze Delta table.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = structlog.get_logger(__name__)

# Ensure sibling package is importable when run via spark-submit.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_session import get_spark_session  # noqa: E402


def read_raw_orders(spark: SparkSession, source_path: str) -> DataFrame:
    """Read the raw Olist orders CSV from the given S3a path.

    Args:
        spark: Active SparkSession.
        source_path: Full s3a:// path to the CSV file.

    Returns:
        Raw DataFrame with inferred schema.
    """
    logger.info("reading_raw_csv", source_path=source_path)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )


def add_audit_columns(
    df: DataFrame,
    source: str,
    batch_date: str,
) -> DataFrame:
    """Append audit metadata columns to the DataFrame.

    Args:
        df: Input DataFrame.
        source: Identifier for the data source (e.g. ``"olist_kaggle"``).
        batch_date: ISO date string for this ingestion batch.

    Returns:
        DataFrame with ``_ingested_at``, ``_source``, and ``_batch_date`` added.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source))
        .withColumn("_batch_date", F.lit(batch_date))
    )


def upsert_to_delta(
    spark: SparkSession,
    incoming: DataFrame,
    dest_path: str,
) -> None:
    """Merge incoming data into the Bronze Delta table by ``order_id``.

    On the first run the table is created. Subsequent runs perform an
    upsert: matching rows are updated, new rows are inserted.

    Args:
        spark: Active SparkSession.
        incoming: DataFrame with audit columns added.
        dest_path: Full s3a:// path for the Delta table.
    """
    from delta.tables import DeltaTable

    logger.info("upserting_to_delta", dest_path=dest_path, row_count=incoming.count())

    if DeltaTable.isDeltaTable(spark, dest_path):
        delta_table = DeltaTable.forPath(spark, dest_path)
        (
            delta_table.alias("target")
            .merge(incoming.alias("source"), "target.order_id = source.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("upsert_complete", mode="merge")
    else:
        incoming.write.format("delta").mode("overwrite").save(dest_path)
        logger.info("upsert_complete", mode="initial_write")


def ingest_orders(
    date_str: Optional[str] = None,
    source_path: Optional[str] = None,
    dest_path: Optional[str] = None,
) -> int:
    """Run the full ingestion flow and return the row count written.

    Args:
        date_str: Batch date (ISO format). Defaults to today.
        source_path: S3a path to the raw CSV.
        dest_path: S3a path for the Bronze Delta table.

    Returns:
        Number of rows ingested.
    """
    batch_date = date_str or date.today().isoformat()
    source = source_path or f"s3a://{os.environ.get('RAW_BUCKET', 'raw')}/olist_orders_dataset.csv"
    dest = dest_path or f"s3a://{os.environ.get('BRONZE_BUCKET', 'bronze')}/orders"

    spark = get_spark_session(app_name="ingest-orders")

    raw_df = read_raw_orders(spark, source)
    enriched_df = add_audit_columns(raw_df, source="olist_kaggle", batch_date=batch_date)
    upsert_to_delta(spark, enriched_df, dest)

    row_count: int = enriched_df.count()
    logger.info("ingestion_finished", rows=row_count, batch_date=batch_date)
    return row_count


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for spark-submit usage.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description="Ingest Olist orders into Bronze Delta")
    parser.add_argument("--date", default=None, help="Batch date (YYYY-MM-DD)")
    parser.add_argument("--source-path", default=None, help="S3a path to raw CSV")
    parser.add_argument("--dest-path", default=None, help="S3a path for Bronze Delta table")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest_orders(
        date_str=args.date,
        source_path=args.source_path,
        dest_path=args.dest_path,
    )
