"""Unit tests for Silver layer transformations."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@pytest.fixture()
def raw_orders_df(spark: SparkSession):
    """Create a synthetic Bronze-like orders DataFrame with realistic Olist data.

    Returns:
        Spark DataFrame mimicking the Bronze layer output.
    """
    now = datetime.now()
    data = [
        # Normal delivered order
        (
            "ord_001", "cust_001", "delivered",
            "2023-01-10 10:00:00", "2023-01-10 11:00:00",
            "2023-01-12 08:00:00", "2023-01-15 14:00:00",
            "2023-01-20 00:00:00",
            now, "olist_kaggle", "2023-01-15",
        ),
        # Duplicate of ord_001 with older ingestion (should be removed)
        (
            "ord_001", "cust_001", "delivered",
            "2023-01-10 10:00:00", "2023-01-10 11:00:00",
            "2023-01-12 08:00:00", "2023-01-15 14:00:00",
            "2023-01-20 00:00:00",
            now - timedelta(days=1), "olist_kaggle", "2023-01-14",
        ),
        # Order with unavailable status (should be filtered out)
        (
            "ord_002", "cust_002", "unavailable",
            "2023-02-01 09:00:00", None, None, None,
            "2023-02-10 00:00:00",
            now, "olist_kaggle", "2023-02-01",
        ),
        # Shipped order (no delivery date -> delivery_days = -1)
        (
            "ord_003", "cust_003", "shipped",
            "2023-03-01 08:00:00", "2023-03-01 09:00:00",
            "2023-03-02 10:00:00", None,
            "2023-03-10 00:00:00",
            now, "olist_kaggle", "2023-03-01",
        ),
        # Normal delivered order
        (
            "ord_004", "cust_004", "delivered",
            "2023-04-01 12:00:00", "2023-04-01 13:00:00",
            "2023-04-03 08:00:00", "2023-04-05 16:00:00",
            "2023-04-10 00:00:00",
            now, "olist_kaggle", "2023-04-01",
        ),
    ]

    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("order_purchase_timestamp", T.StringType()),
        T.StructField("order_approved_at", T.StringType()),
        T.StructField("order_delivered_carrier_date", T.StringType()),
        T.StructField("order_delivered_customer_date", T.StringType()),
        T.StructField("order_estimated_delivery_date", T.StringType()),
        T.StructField("_ingested_at", T.TimestampType()),
        T.StructField("_source", T.StringType()),
        T.StructField("_batch_date", T.StringType()),
    ])

    return spark.createDataFrame(data, schema)


class TestCastTimestamps:
    """Tests for the ``cast_timestamps`` transformation."""

    def test_timestamps_are_cast(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Timestamp columns should be TimestampType after casting."""
        from transformations.silver_orders import cast_timestamps

        result = cast_timestamps(raw_orders_df)
        ts_field = next(
            f for f in result.schema.fields
            if f.name == "order_purchase_timestamp"
        )
        assert isinstance(ts_field.dataType, T.TimestampType)

    def test_null_timestamps_preserved(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Null values in timestamp columns should remain null."""
        from transformations.silver_orders import cast_timestamps

        result = cast_timestamps(raw_orders_df)
        null_count = result.filter(
            F.col("order_approved_at").isNull()
        ).count()
        # ord_002 has null approved_at
        assert null_count >= 1


class TestCalculateDeliveryDays:
    """Tests for the ``calculate_delivery_days`` transformation."""

    def test_delivery_days_calculated(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """delivery_days should be the diff between delivery and purchase."""
        from transformations.silver_orders import (
            calculate_delivery_days,
            cast_timestamps,
        )

        df = cast_timestamps(raw_orders_df)
        result = calculate_delivery_days(df)

        # ord_001: Jan 15 - Jan 10 = 5 days
        row = result.filter(F.col("order_id") == "ord_001").first()
        assert row is not None
        assert row["delivery_days"] == 5

    def test_missing_delivery_gets_sentinel(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Orders without delivery date should get delivery_days = -1."""
        from transformations.silver_orders import (
            calculate_delivery_days,
            cast_timestamps,
        )

        df = cast_timestamps(raw_orders_df)
        result = calculate_delivery_days(df)

        row = result.filter(F.col("order_id") == "ord_003").first()
        assert row is not None
        assert row["delivery_days"] == -1


class TestDeduplicate:
    """Tests for the ``deduplicate`` transformation."""

    def test_keeps_latest_ingestion(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Only the most recently ingested row per order_id should remain."""
        from transformations.silver_orders import deduplicate

        result = deduplicate(raw_orders_df)
        ord_001_rows = result.filter(F.col("order_id") == "ord_001").count()
        assert ord_001_rows == 1

    def test_total_row_count_after_dedup(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Total rows should decrease after removing duplicates."""
        from transformations.silver_orders import deduplicate

        result = deduplicate(raw_orders_df)
        # 5 input rows, 1 duplicate -> 4 unique
        assert result.count() == 4


class TestFilterInvalidStatus:
    """Tests for the ``filter_invalid_status`` transformation."""

    def test_unavailable_removed(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Orders with status 'unavailable' should be removed."""
        from transformations.silver_orders import filter_invalid_status

        result = filter_invalid_status(raw_orders_df)
        unavailable_count = result.filter(
            F.col("order_status") == "unavailable"
        ).count()
        assert unavailable_count == 0

    def test_valid_statuses_preserved(
        self, spark: SparkSession, raw_orders_df
    ) -> None:
        """Orders with valid statuses should be kept."""
        from transformations.silver_orders import filter_invalid_status

        result = filter_invalid_status(raw_orders_df)
        # 5 rows, 1 unavailable -> 4 remaining
        assert result.count() == 4
