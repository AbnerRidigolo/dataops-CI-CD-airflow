"""Unit tests for Gold layer transformations."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@pytest.fixture()
def silver_orders_df(spark: SparkSession):
    """Create a synthetic Silver-like orders DataFrame.

    Returns:
        Spark DataFrame mimicking the Silver layer output.
    """
    data = [
        ("ord_001", "cust_001", "delivered", 5, "2023-01-15 14:00:00", "2023-01-18 10:00:00"),
        ("ord_002", "cust_001", "delivered", 12, "2023-02-10 09:00:00", "2023-02-15 08:00:00"),
        ("ord_003", "cust_002", "delivered", 20, "2023-03-01 16:00:00", None),
        ("ord_004", "cust_003", "shipped", -1, None, None),
        ("ord_005", "cust_004", "delivered", 3, "2023-04-05 12:00:00", "2023-04-06 10:00:00"),
    ]

    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("delivery_days", T.IntegerType()),
        T.StructField("order_delivered_customer_date", T.StringType()),
        T.StructField("review_creation_date", T.StringType()),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture()
def payments_df(spark: SparkSession):
    """Create a synthetic payments DataFrame.

    Returns:
        Spark DataFrame with payment data per order.
    """
    data = [
        ("ord_001", 45.0),   # low
        ("ord_002", 150.0),  # mid
        ("ord_003", 350.0),  # high
        # ord_004 has no payment (test left join)
        ("ord_005", 80.0),   # mid
    ]
    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("payment_value", T.DoubleType()),
    ])
    return spark.createDataFrame(data, schema)


class TestDeliveryTier:
    """Tests for the ``classify_delivery_tier`` transformation."""

    def test_fast_tier(self, spark: SparkSession, silver_orders_df) -> None:
        """Orders with delivery_days <= 7 should be 'fast'."""
        from transformations.gold_fct_orders import classify_delivery_tier

        result = classify_delivery_tier(silver_orders_df)
        row = result.filter(F.col("order_id") == "ord_001").first()
        assert row["delivery_tier"] == "fast"

    def test_normal_tier(self, spark: SparkSession, silver_orders_df) -> None:
        """Orders with delivery_days 8-14 should be 'normal'."""
        from transformations.gold_fct_orders import classify_delivery_tier

        result = classify_delivery_tier(silver_orders_df)
        row = result.filter(F.col("order_id") == "ord_002").first()
        assert row["delivery_tier"] == "normal"

    def test_slow_tier(self, spark: SparkSession, silver_orders_df) -> None:
        """Orders with delivery_days > 14 should be 'slow'."""
        from transformations.gold_fct_orders import classify_delivery_tier

        result = classify_delivery_tier(silver_orders_df)
        row = result.filter(F.col("order_id") == "ord_003").first()
        assert row["delivery_tier"] == "slow"


class TestRevenueBucket:
    """Tests for the ``classify_revenue_bucket`` transformation."""

    def test_low_bucket(self, spark: SparkSession, payments_df) -> None:
        """payment_value < 50 should be 'low'."""
        from transformations.gold_fct_orders import classify_revenue_bucket

        result = classify_revenue_bucket(payments_df)
        row = result.filter(F.col("order_id") == "ord_001").first()
        assert row["revenue_bucket"] == "low"

    def test_mid_bucket(self, spark: SparkSession, payments_df) -> None:
        """payment_value 50-200 should be 'mid'."""
        from transformations.gold_fct_orders import classify_revenue_bucket

        result = classify_revenue_bucket(payments_df)
        row = result.filter(F.col("order_id") == "ord_002").first()
        assert row["revenue_bucket"] == "mid"

    def test_high_bucket(self, spark: SparkSession, payments_df) -> None:
        """payment_value > 200 should be 'high'."""
        from transformations.gold_fct_orders import classify_revenue_bucket

        result = classify_revenue_bucket(payments_df)
        row = result.filter(F.col("order_id") == "ord_003").first()
        assert row["revenue_bucket"] == "high"


class TestRepeatCustomer:
    """Tests for the ``flag_repeat_customer`` transformation."""

    def test_repeat_customer_flagged(
        self, spark: SparkSession, silver_orders_df
    ) -> None:
        """Customers with more than one order should be flagged as repeat."""
        from transformations.gold_fct_orders import flag_repeat_customer

        result = flag_repeat_customer(silver_orders_df)
        # cust_001 has 2 orders
        rows = result.filter(F.col("customer_id") == "cust_001").collect()
        for row in rows:
            assert row["is_repeat_customer"] is True

    def test_single_order_not_repeat(
        self, spark: SparkSession, silver_orders_df
    ) -> None:
        """Customers with exactly one order should not be repeat."""
        from transformations.gold_fct_orders import flag_repeat_customer

        result = flag_repeat_customer(silver_orders_df)
        row = result.filter(F.col("customer_id") == "cust_002").first()
        assert row["is_repeat_customer"] is False


class TestDaysToFirstReview:
    """Tests for the ``calculate_days_to_first_review`` transformation."""

    def test_review_days_calculated(
        self, spark: SparkSession, silver_orders_df
    ) -> None:
        """days_to_first_review should be diff between review and delivery."""
        from transformations.gold_fct_orders import calculate_days_to_first_review

        df = silver_orders_df.withColumn(
            "order_delivered_customer_date",
            F.col("order_delivered_customer_date").cast(T.TimestampType()),
        ).withColumn(
            "review_creation_date",
            F.col("review_creation_date").cast(T.TimestampType()),
        )
        result = calculate_days_to_first_review(df)
        row = result.filter(F.col("order_id") == "ord_001").first()
        # Jan 18 - Jan 15 = 3 days
        assert row["days_to_first_review"] == 3

    def test_null_review_returns_null(
        self, spark: SparkSession, silver_orders_df
    ) -> None:
        """Missing review date should result in null days_to_first_review."""
        from transformations.gold_fct_orders import calculate_days_to_first_review

        df = silver_orders_df.withColumn(
            "order_delivered_customer_date",
            F.col("order_delivered_customer_date").cast(T.TimestampType()),
        ).withColumn(
            "review_creation_date",
            F.col("review_creation_date").cast(T.TimestampType()),
        )
        result = calculate_days_to_first_review(df)
        row = result.filter(F.col("order_id") == "ord_003").first()
        assert row["days_to_first_review"] is None


class TestLeftJoinBehavior:
    """Tests for join behavior when data is missing."""

    def test_order_without_payment_gets_null(
        self, spark: SparkSession, silver_orders_df, payments_df
    ) -> None:
        """Orders with no matching payment should have null payment_value."""
        joined = silver_orders_df.join(payments_df, on="order_id", how="left")
        row = joined.filter(F.col("order_id") == "ord_004").first()
        assert row["payment_value"] is None

    def test_all_orders_preserved_after_join(
        self, spark: SparkSession, silver_orders_df, payments_df
    ) -> None:
        """All orders should be present after left join with payments."""
        joined = silver_orders_df.join(payments_df, on="order_id", how="left")
        assert joined.count() == silver_orders_df.count()
