"""Gold layer transformation: fact table ``fct_orders``.

Joins Silver orders with customers and payments, computes derived business
metrics, and writes a fully-materialized Gold Delta table partitioned by
``customer_state``.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = structlog.get_logger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from ingestion.spark_session import get_spark_session  # noqa: E402


def classify_delivery_tier(df: DataFrame) -> DataFrame:
    """Add ``delivery_tier`` based on ``delivery_days``.

    - fast: <= 7 days
    - normal: 8-14 days
    - slow: > 14 days

    Args:
        df: DataFrame with ``delivery_days`` column.

    Returns:
        DataFrame with ``delivery_tier`` added.
    """
    return df.withColumn(
        "delivery_tier",
        F.when(F.col("delivery_days") <= 7, F.lit("fast"))
        .when(F.col("delivery_days") <= 14, F.lit("normal"))
        .otherwise(F.lit("slow")),
    )


def classify_revenue_bucket(df: DataFrame) -> DataFrame:
    """Add ``revenue_bucket`` based on ``payment_value``.

    - low: < 50
    - mid: 50-200
    - high: > 200

    Args:
        df: DataFrame with ``payment_value`` column.

    Returns:
        DataFrame with ``revenue_bucket`` added.
    """
    return df.withColumn(
        "revenue_bucket",
        F.when(F.col("payment_value") < 50, F.lit("low"))
        .when(F.col("payment_value") <= 200, F.lit("mid"))
        .otherwise(F.lit("high")),
    )


def flag_repeat_customer(df: DataFrame) -> DataFrame:
    """Add ``is_repeat_customer`` flag using a window function.

    A customer is a repeat customer if they have more than one order.

    Args:
        df: DataFrame with ``customer_id`` column.

    Returns:
        DataFrame with ``is_repeat_customer`` boolean column.
    """
    window = Window.partitionBy("customer_id")
    return df.withColumn(
        "is_repeat_customer",
        F.when(F.count("order_id").over(window) > 1, F.lit(True)).otherwise(
            F.lit(False)
        ),
    )


def calculate_days_to_first_review(df: DataFrame) -> DataFrame:
    """Add ``days_to_first_review`` as the gap between delivery and review.

    Uses ``order_delivered_customer_date`` and ``review_creation_date``.
    Returns ``None`` when either date is missing.

    Args:
        df: DataFrame with delivery and review date columns.

    Returns:
        DataFrame with ``days_to_first_review`` added.
    """
    if "review_creation_date" in df.columns:
        return df.withColumn(
            "days_to_first_review",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("review_creation_date").isNotNull(),
                F.datediff(
                    F.col("review_creation_date"),
                    F.col("order_delivered_customer_date"),
                ),
            ),
        )
    return df.withColumn("days_to_first_review", F.lit(None).cast("int"))


def build_gold_fct_orders(
    spark: SparkSession,
    silver_orders_path: Optional[str] = None,
    silver_customers_path: Optional[str] = None,
    silver_payments_path: Optional[str] = None,
    gold_path: Optional[str] = None,
) -> int:
    """Build the Gold fact table with all derived metrics.

    Performs left joins from Silver orders to customers and payments,
    applies business logic transformations, and writes the output
    partitioned by ``customer_state``.

    Args:
        spark: Active SparkSession.
        silver_orders_path: S3a path to Silver orders Delta table.
        silver_customers_path: S3a path to Silver customers Delta table.
        silver_payments_path: S3a path to Silver payments Delta table.
        gold_path: S3a path for the Gold Delta table output.

    Returns:
        Number of rows written.
    """
    silver_bucket = os.environ.get("SILVER_BUCKET", "silver")
    gold_bucket = os.environ.get("GOLD_BUCKET", "gold")

    orders_path = silver_orders_path or f"s3a://{silver_bucket}/orders"
    customers_path = silver_customers_path or f"s3a://{silver_bucket}/customers"
    payments_path = silver_payments_path or f"s3a://{silver_bucket}/payments"
    out_path = gold_path or f"s3a://{gold_bucket}/fct_orders"

    logger.info(
        "reading_silver_tables",
        orders=orders_path,
        customers=customers_path,
        payments=payments_path,
    )

    orders_df = spark.read.format("delta").load(orders_path)

    # Customers — may not exist yet; use empty DF as fallback.
    try:
        customers_df = spark.read.format("delta").load(customers_path)
    except Exception:
        logger.warning("customers_table_not_found", path=customers_path)
        customers_df = spark.createDataFrame(
            [], "customer_id STRING, customer_state STRING"
        )

    # Payments — may not exist yet; use empty DF as fallback.
    try:
        payments_df = spark.read.format("delta").load(payments_path)
    except Exception:
        logger.warning("payments_table_not_found", path=payments_path)
        payments_df = spark.createDataFrame(
            [], "order_id STRING, payment_value DOUBLE"
        )

    # Aggregate payments per order (there can be multiple payment rows).
    payments_agg = payments_df.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value")
    )

    # Left joins
    df = orders_df.join(customers_df, on="customer_id", how="left").join(
        payments_agg, on="order_id", how="left"
    )

    # Fill null payment_value with 0
    df = df.fillna({"payment_value": 0.0})

    # Apply business transformations
    df = classify_delivery_tier(df)
    df = classify_revenue_bucket(df)
    df = flag_repeat_customer(df)
    df = calculate_days_to_first_review(df)

    # Add refresh timestamp
    df = df.withColumn("_gold_updated_at", F.current_timestamp())

    row_count: int = df.count()
    logger.info("writing_gold", path=out_path, rows=row_count)

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).partitionBy("customer_state").save(out_path)

    logger.info("gold_complete", rows=row_count)
    return row_count


if __name__ == "__main__":
    _spark = get_spark_session(app_name="gold-fct-orders")
    build_gold_fct_orders(_spark)
