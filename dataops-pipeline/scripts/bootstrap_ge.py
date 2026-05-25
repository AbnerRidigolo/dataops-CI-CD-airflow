"""Bootstrap Great Expectations: create data context, suites, and checkpoints.

Run this once after ``docker compose up`` to initialize the GE project
structure. Optionally pass ``--run-checkpoints`` to execute all checkpoints.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

import structlog

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTATIONS_DIR = PROJECT_ROOT / "expectations"
CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"


def _build_bronze_suite() -> Dict[str, Any]:
    """Return the bronze_orders_suite expectation suite as a dict.

    Returns:
        Complete expectation suite dictionary.
    """
    return {
        "expectation_suite_name": "bronze_orders_suite",
        "ge_cloud_id": None,
        "data_asset_type": None,
        "meta": {
            "great_expectations_version": "0.18.8",
            "citations": [
                {
                    "citation_date": "2024-01-01",
                    "comment": "Bronze layer validations for raw Olist orders",
                }
            ],
        },
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "order_id",
                        "customer_id",
                        "order_status",
                        "order_purchase_timestamp",
                        "order_approved_at",
                        "order_delivered_carrier_date",
                        "order_delivered_customer_date",
                        "order_estimated_delivery_date",
                        "_ingested_at",
                        "_source",
                        "_batch_date",
                    ]
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_purchase_timestamp"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "order_id"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "order_status",
                    "value_set": [
                        "delivered",
                        "shipped",
                        "canceled",
                        "unavailable",
                        "invoiced",
                        "processing",
                        "created",
                        "approved",
                    ],
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 100},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                "kwargs": {
                    "column_A": "order_delivered_customer_date",
                    "column_B": "order_purchase_timestamp",
                    "or_equal": True,
                    "ignore_row_if": "either_value_is_missing",
                },
                "meta": {},
            },
        ],
    }


def _build_silver_suite() -> Dict[str, Any]:
    """Return the silver_orders_suite expectation suite as a dict.

    Returns:
        Complete expectation suite dictionary.
    """
    bronze = _build_bronze_suite()
    silver_expectations: List[Dict[str, Any]] = list(bronze["expectations"])

    # Update column list to include delivery_days
    silver_expectations[0] = {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {
            "column_list": [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
                "_ingested_at",
                "_source",
                "_batch_date",
                "delivery_days",
            ]
        },
        "meta": {},
    }

    # Remove 'unavailable' from valid statuses (silver filters it out)
    silver_expectations[5] = {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "order_status",
            "value_set": [
                "delivered",
                "shipped",
                "canceled",
                "invoiced",
                "processing",
                "created",
                "approved",
            ],
        },
        "meta": {},
    }

    # Additional silver-specific expectations
    silver_expectations.extend(
        [
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {
                    "column": "order_purchase_timestamp",
                    "type_": "TimestampType",
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "delivery_days", "type_": "IntegerType"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "delivery_days",
                    "min_value": -1,
                    "max_value": 365,
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "delivery_days"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
                "kwargs": {
                    "column": "customer_id",
                    "min_value": 0.5,
                    "max_value": 1.0,
                },
                "meta": {},
            },
        ]
    )

    return {
        "expectation_suite_name": "silver_orders_suite",
        "ge_cloud_id": None,
        "data_asset_type": None,
        "meta": {
            "great_expectations_version": "0.18.8",
            "citations": [
                {
                    "citation_date": "2024-01-01",
                    "comment": "Silver layer validations for cleaned Olist orders",
                }
            ],
        },
        "expectations": silver_expectations,
    }


def _build_gold_suite() -> Dict[str, Any]:
    """Return the gold_kpis_suite expectation suite as a dict.

    Returns:
        Complete expectation suite dictionary.
    """
    return {
        "expectation_suite_name": "gold_kpis_suite",
        "ge_cloud_id": None,
        "data_asset_type": None,
        "meta": {
            "great_expectations_version": "0.18.8",
            "citations": [
                {
                    "citation_date": "2024-01-01",
                    "comment": "Gold layer validations for fct_orders KPIs",
                }
            ],
        },
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "payment_value",
                    "min_value": 0,
                    "max_value": 100000,
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "delivery_days",
                    "min_value": 0,
                    "max_value": 60,
                    "mostly": 0.99,
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "delivery_tier",
                    "value_set": ["fast", "normal", "slow"],
                },
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "delivery_tier"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "revenue_bucket"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "is_repeat_customer"},
                "meta": {},
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1000},
                "meta": {},
            },
        ],
    }


def write_suite(suite: Dict[str, Any]) -> None:
    """Write an expectation suite to the expectations directory.

    Args:
        suite: Full expectation suite dictionary.
    """
    name = suite["expectation_suite_name"]
    path = EXPECTATIONS_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(suite, indent=2) + "\n", encoding="utf-8")
    logger.info("suite_written", name=name, path=str(path))


def bootstrap() -> None:
    """Create all expectation suites and log the result."""
    logger.info("bootstrapping_great_expectations")

    suites = [
        _build_bronze_suite(),
        _build_silver_suite(),
        _build_gold_suite(),
    ]

    for suite in suites:
        write_suite(suite)

    logger.info(
        "bootstrap_complete",
        suites_created=[s["expectation_suite_name"] for s in suites],
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description="Bootstrap Great Expectations")
    parser.add_argument(
        "--run-checkpoints",
        action="store_true",
        help="Run all checkpoints after bootstrapping",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    bootstrap()

    if args.run_checkpoints:
        logger.info("checkpoint_execution_skipped", reason="requires running Spark context")
