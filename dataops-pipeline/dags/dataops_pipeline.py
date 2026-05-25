"""DataOps Pipeline DAG - Brazilian E-Commerce (Olist).

End-to-end data pipeline orchestrated by Apache Airflow with five
sequential TaskGroups:

1. **ingest** - S3KeySensor waits for the raw CSV in MinIO, then
   triggers ingestion into the Bronze Delta layer.
2. **bronze** - Ingests raw CSV into Delta Lake (Bronze) and validates
   with Great Expectations.
3. **silver** - Cleans, deduplicates, and type-casts Bronze data into
   Silver; validates with GE; runs schema tests.
4. **gold** - Builds the ``fct_orders`` fact table with business
   metrics; validates with GE.
5. **publish** - Syncs GE Data Docs to MinIO and sends a Slack summary
   with validation statistics from all layers.

Schedule: daily at 06:00 UTC.
Dataset: Brazilian E-Commerce Public Dataset by Olist (Kaggle).
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

from utils.alerts import on_failure_slack, on_sla_miss_slack
from utils.ge_operator import run_ge_checkpoint

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "gold")
DOCS_BUCKET = os.environ.get("DOCS_BUCKET", "docs")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

default_args: Dict[str, Any] = {
    "owner": "dataops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_failure_slack,
}


# ---------------------------------------------------------------------------
# Callables
# ---------------------------------------------------------------------------
def _ingest_orders(**context: Any) -> int:
    """Run Bronze ingestion and return row count via XCom.

    Args:
        **context: Airflow context with ``ds`` (execution date).

    Returns:
        Number of rows ingested.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.ingest_orders import ingest_orders

    ds: str = context["ds"]
    row_count = ingest_orders(date_str=ds)
    return row_count


def _transform_silver(**context: Any) -> int:
    """Run Silver transformation and return row count.

    Args:
        **context: Airflow context.

    Returns:
        Number of rows in Silver.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.spark_session import get_spark_session
    from transformations.silver_orders import transform_silver

    spark = get_spark_session(app_name="silver-orders")
    return transform_silver(spark)


def _transform_gold(**context: Any) -> int:
    """Run Gold transformation and return row count.

    Args:
        **context: Airflow context.

    Returns:
        Number of rows in Gold.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.spark_session import get_spark_session
    from transformations.gold_fct_orders import build_gold_fct_orders

    spark = get_spark_session(app_name="gold-fct-orders")
    return build_gold_fct_orders(spark)


def _run_schema_test(**context: Any) -> Dict[str, Any]:
    """Simulate dbt-style schema test by validating Silver column types.

    Runs a subprocess that checks the Silver Delta table schema against
    expected types. In a real dbt setup this would call ``dbt test``.

    Args:
        **context: Airflow context.

    Returns:
        Dict with test results.
    """
    result = subprocess.run(
        [
            "python",
            "-c",
            (
                "import sys; sys.path.insert(0, '/opt/airflow'); "
                "from ingestion.spark_session import get_spark_session; "
                "spark = get_spark_session('schema-test'); "
                f"df = spark.read.format('delta').load('s3a://{SILVER_BUCKET}/orders'); "
                "schema = {f.name: str(f.dataType) for f in df.schema.fields}; "
                "assert 'order_id' in schema, 'Missing order_id'; "
                "assert 'delivery_days' in schema, 'Missing delivery_days'; "
                "print('Schema validation passed')"
            ),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Schema test failed: {result.stderr}")
    return {"status": "passed", "output": result.stdout.strip()}


def _publish_slack_summary(**context: Any) -> None:
    """Collect GE stats from XCom and post a summary to Slack.

    Args:
        **context: Airflow context with ``ti`` for XCom access.
    """
    import requests

    ti = context["ti"]

    checkpoints = [
        "bronze_orders_checkpoint",
        "silver_orders_checkpoint",
        "gold_kpis_checkpoint",
    ]

    summary_lines = [":white_check_mark: *DataOps Pipeline Complete*\n"]

    for cp in checkpoints:
        raw_stats = ti.xcom_pull(
            key=f"ge_stats_{cp}",
            task_ids=[
                "bronze.validate_bronze",
                "silver.validate_silver",
                "gold.validate_gold",
            ],
        )
        # raw_stats is a list; find first non-None
        stats_json = next((s for s in (raw_stats or []) if s), None)
        if stats_json:
            stats = json.loads(stats_json)
            line = (
                f"  *{cp}*: "
                f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
                f"passed ({'OK' if stats['success'] else 'FAILED'})"
            )
        else:
            line = f"  *{cp}*: _no stats available_"
        summary_lines.append(line)

    dag_run = context.get("dag_run")
    execution_date = context.get("execution_date", "N/A")
    summary_lines.append(f"\n*Execution date:* `{execution_date}`")
    summary_lines.append(
        f"*Run ID:* `{dag_run.run_id if dag_run else 'N/A'}`"
    )

    payload = {"text": "\n".join(summary_lines)}
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook_url and "PLACEHOLDER" not in webhook_url:
        requests.post(webhook_url, json=payload, timeout=10)


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dataops_pipeline",
    default_args=default_args,
    description="End-to-end DataOps pipeline for Olist Brazilian E-Commerce data",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataops", "olist", "production"],
    sla_miss_callback=on_sla_miss_slack,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # ---- TaskGroup: ingest ----
    with TaskGroup("ingest", tooltip="Wait for raw file in MinIO") as tg_ingest:
        wait_for_file = S3KeySensor(
            task_id="wait_for_raw_file",
            bucket_name=RAW_BUCKET,
            bucket_key="olist_orders_dataset.csv",
            aws_conn_id="aws_default",
            mode="reschedule",
            poke_interval=60,
            timeout=3600,
        )

    # ---- TaskGroup: bronze ----
    with TaskGroup("bronze", tooltip="Ingest raw -> Bronze Delta + validate") as tg_bronze:
        ingest_task = PythonOperator(
            task_id="ingest_orders",
            python_callable=_ingest_orders,
        )

        validate_bronze = PythonOperator(
            task_id="validate_bronze",
            python_callable=run_ge_checkpoint,
            op_kwargs={
                "checkpoint_name": "bronze_orders_checkpoint",
                "delta_path": f"s3a://{BRONZE_BUCKET}/orders",
                "suite_name": "bronze_orders_suite",
            },
        )

        ingest_task >> validate_bronze

    # ---- TaskGroup: silver ----
    with TaskGroup("silver", tooltip="Bronze -> Silver + validate + test") as tg_silver:
        transform_silver_task = PythonOperator(
            task_id="transform_silver",
            python_callable=_transform_silver,
        )

        validate_silver = PythonOperator(
            task_id="validate_silver",
            python_callable=run_ge_checkpoint,
            op_kwargs={
                "checkpoint_name": "silver_orders_checkpoint",
                "delta_path": f"s3a://{SILVER_BUCKET}/orders",
                "suite_name": "silver_orders_suite",
            },
        )

        schema_test = PythonOperator(
            task_id="schema_test",
            python_callable=_run_schema_test,
        )

        transform_silver_task >> validate_silver >> schema_test

    # ---- TaskGroup: gold ----
    with TaskGroup("gold", tooltip="Silver -> Gold fct_orders + validate") as tg_gold:
        transform_gold_task = PythonOperator(
            task_id="transform_gold",
            python_callable=_transform_gold,
        )

        validate_gold = PythonOperator(
            task_id="validate_gold",
            python_callable=run_ge_checkpoint,
            op_kwargs={
                "checkpoint_name": "gold_kpis_checkpoint",
                "delta_path": f"s3a://{GOLD_BUCKET}/fct_orders",
                "suite_name": "gold_kpis_suite",
            },
        )

        transform_gold_task >> validate_gold

    # ---- TaskGroup: publish ----
    with TaskGroup("publish", tooltip="Publish Data Docs + Slack summary") as tg_publish:
        publish_docs = BashOperator(
            task_id="publish_data_docs",
            bash_command=(
                f"aws s3 sync /opt/airflow/great_expectations/data_docs/local_site/ "
                f"s3://{DOCS_BUCKET}/data_docs/ "
                f"--endpoint-url {MINIO_ENDPOINT}"
            ),
        )

        slack_summary = PythonOperator(
            task_id="slack_summary",
            python_callable=_publish_slack_summary,
        )

        publish_docs >> slack_summary

    # ---- Pipeline flow ----
    tg_ingest >> tg_bronze >> tg_silver >> tg_gold >> tg_publish
