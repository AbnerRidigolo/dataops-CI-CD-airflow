"""Callable wrapper for running Great Expectations checkpoints inside Airflow.

Provides ``run_ge_checkpoint`` which loads a Delta table, runs a GE
checkpoint against it, and returns validation statistics via XCom.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

import structlog

logger = structlog.get_logger(__name__)

GE_ROOT = Path(os.environ.get("GE_ROOT", "/opt/airflow/great_expectations"))
EXPECTATIONS_DIR = Path(os.environ.get("EXPECTATIONS_DIR", "/opt/airflow/expectations"))


def run_ge_checkpoint(
    checkpoint_name: str,
    delta_path: str,
    suite_name: str,
    **context: Any,
) -> Dict[str, Any]:
    """Execute a Great Expectations checkpoint and return statistics.

    This function is designed to be called as a ``python_callable`` from
    an Airflow PythonOperator.  It reads a Delta table into a Spark
    DataFrame, validates it against the named expectation suite, and
    pushes a statistics dict to XCom.

    Args:
        checkpoint_name: Name of the GE checkpoint to run.
        delta_path: S3a path to the Delta table to validate.
        suite_name: Name of the expectation suite (matches JSON filename).
        **context: Airflow context (automatically provided by the operator).

    Returns:
        Dictionary with keys ``evaluated_expectations``,
        ``successful_expectations``, ``unsuccessful_expectations``,
        and ``success`` (bool).
    """
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    # Lazy import to avoid pulling Spark at DAG parse time
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.spark_session import get_spark_session

    spark = get_spark_session(app_name=f"ge-{checkpoint_name}")

    logger.info(
        "running_ge_checkpoint",
        checkpoint=checkpoint_name,
        delta_path=delta_path,
        suite=suite_name,
    )

    # Read the Delta table
    df = spark.read.format("delta").load(delta_path)
    row_count = df.count()
    logger.info("delta_table_loaded", rows=row_count, path=delta_path)

    # Build GE context
    ge_context = gx.get_context(context_root_dir=str(GE_ROOT))

    # Build runtime batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="default_runtime_data_connector",
        data_asset_name=checkpoint_name.replace("_checkpoint", ""),
        batch_identifiers={
            "batch_date": context.get("ds", "manual"),
            "layer": suite_name.split("_")[0],
        },
        runtime_parameters={"batch_data": df},
    )

    # Run checkpoint
    result = ge_context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )

    # Extract statistics
    stats: Dict[str, Any] = {
        "evaluated_expectations": 0,
        "successful_expectations": 0,
        "unsuccessful_expectations": 0,
        "success": result.success,
    }

    for validation_result in result.run_results.values():
        result_stats = validation_result["validation_result"]["statistics"]
        stats["evaluated_expectations"] += result_stats.get(
            "evaluated_expectations", 0
        )
        stats["successful_expectations"] += result_stats.get(
            "successful_expectations", 0
        )
        stats["unsuccessful_expectations"] += result_stats.get(
            "unsuccessful_expectations", 0
        )

    logger.info(
        "ge_checkpoint_complete",
        checkpoint=checkpoint_name,
        **stats,
    )

    # Push to XCom via the task instance
    ti = context.get("ti") or context.get("task_instance")
    if ti:
        ti.xcom_push(key=f"ge_stats_{checkpoint_name}", value=json.dumps(stats))

    if not stats["success"]:
        raise ValueError(
            f"GE checkpoint '{checkpoint_name}' failed: "
            f"{stats['unsuccessful_expectations']} of "
            f"{stats['evaluated_expectations']} expectations failed."
        )

    return stats
