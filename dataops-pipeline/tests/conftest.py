"""Shared pytest fixtures for the DataOps Pipeline test suite."""

from __future__ import annotations

import os
import sys
from typing import Generator

import pytest

# Ensure project packages are importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "dags"))


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    Yields:
        A SparkSession configured for local mode with Delta Lake support.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[*]")
        .appName("dataops-pipeline-tests")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def dagbag():
    """Load all DAGs into a DagBag for integrity testing.

    Returns:
        Airflow DagBag populated from the ``dags/`` directory.
    """
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///test_airflow.db"
    )

    from airflow.models import DagBag

    dags_folder = os.path.join(PROJECT_ROOT, "dags")
    bag = DagBag(dag_folder=dags_folder, include_examples=False)
    return bag
