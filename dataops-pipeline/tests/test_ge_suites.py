"""Validate that all Great Expectations suite JSON files are well-formed."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest

EXPECTATIONS_DIR = Path(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
) / "expectations"


def _load_suite(filename: str) -> Dict[str, Any]:
    """Load and parse an expectation suite JSON file.

    Args:
        filename: Name of the JSON file in the expectations directory.

    Returns:
        Parsed suite dictionary.
    """
    path = EXPECTATIONS_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_suite_files() -> List[str]:
    """Return all JSON files in the expectations directory.

    Returns:
        List of filenames.
    """
    return [f.name for f in EXPECTATIONS_DIR.glob("*.json")]


class TestSuiteStructure:
    """Validate the structure of each expectation suite."""

    @pytest.mark.parametrize("suite_file", _get_suite_files())
    def test_suite_is_valid_json(self, suite_file: str) -> None:
        """Suite files must be valid JSON."""
        suite = _load_suite(suite_file)
        assert isinstance(suite, dict)

    @pytest.mark.parametrize("suite_file", _get_suite_files())
    def test_suite_has_name(self, suite_file: str) -> None:
        """Each suite must have an expectation_suite_name."""
        suite = _load_suite(suite_file)
        assert "expectation_suite_name" in suite
        assert isinstance(suite["expectation_suite_name"], str)
        assert len(suite["expectation_suite_name"]) > 0

    @pytest.mark.parametrize("suite_file", _get_suite_files())
    def test_suite_has_expectations(self, suite_file: str) -> None:
        """Each suite must have a non-empty expectations list."""
        suite = _load_suite(suite_file)
        assert "expectations" in suite
        assert isinstance(suite["expectations"], list)
        assert len(suite["expectations"]) > 0

    @pytest.mark.parametrize("suite_file", _get_suite_files())
    def test_each_expectation_has_type(self, suite_file: str) -> None:
        """Every expectation must have an expectation_type field."""
        suite = _load_suite(suite_file)
        for i, exp in enumerate(suite["expectations"]):
            assert "expectation_type" in exp, (
                f"Expectation #{i} in {suite_file} missing expectation_type"
            )
            assert exp["expectation_type"].startswith("expect_"), (
                f"Expectation #{i} type '{exp['expectation_type']}' "
                "doesn't start with 'expect_'"
            )

    @pytest.mark.parametrize("suite_file", _get_suite_files())
    def test_each_expectation_has_kwargs(self, suite_file: str) -> None:
        """Every expectation must have a kwargs dict."""
        suite = _load_suite(suite_file)
        for i, exp in enumerate(suite["expectations"]):
            assert "kwargs" in exp, (
                f"Expectation #{i} in {suite_file} missing kwargs"
            )
            assert isinstance(exp["kwargs"], dict)


class TestSuiteContent:
    """Validate specific content requirements per suite."""

    def test_bronze_suite_has_column_list(self) -> None:
        """Bronze suite must validate column ordering."""
        suite = _load_suite("bronze_orders_suite.json")
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_table_columns_to_match_ordered_list" in types

    def test_bronze_suite_checks_order_id_unique(self) -> None:
        """Bronze suite must check order_id uniqueness."""
        suite = _load_suite("bronze_orders_suite.json")
        unique_checks = [
            e for e in suite["expectations"]
            if e["expectation_type"] == "expect_column_values_to_be_unique"
            and e["kwargs"].get("column") == "order_id"
        ]
        assert len(unique_checks) > 0

    def test_silver_suite_checks_delivery_days_type(self) -> None:
        """Silver suite must validate delivery_days is IntegerType."""
        suite = _load_suite("silver_orders_suite.json")
        type_checks = [
            e for e in suite["expectations"]
            if e["expectation_type"] == "expect_column_values_to_be_of_type"
            and e["kwargs"].get("column") == "delivery_days"
        ]
        assert len(type_checks) > 0
        assert type_checks[0]["kwargs"]["type_"] == "IntegerType"

    def test_gold_suite_checks_delivery_tier_set(self) -> None:
        """Gold suite must validate delivery_tier values."""
        suite = _load_suite("gold_kpis_suite.json")
        set_checks = [
            e for e in suite["expectations"]
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
            and e["kwargs"].get("column") == "delivery_tier"
        ]
        assert len(set_checks) > 0
        assert set(set_checks[0]["kwargs"]["value_set"]) == {
            "fast", "normal", "slow"
        }

    def test_gold_suite_min_row_count(self) -> None:
        """Gold suite must require at least 1000 rows."""
        suite = _load_suite("gold_kpis_suite.json")
        row_checks = [
            e for e in suite["expectations"]
            if e["expectation_type"] == "expect_table_row_count_to_be_between"
        ]
        assert len(row_checks) > 0
        assert row_checks[0]["kwargs"]["min_value"] == 1000
