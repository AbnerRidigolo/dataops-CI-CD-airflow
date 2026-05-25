"""DAG integrity tests — validate structure without starting Airflow."""

from __future__ import annotations

from datetime import timedelta
from typing import Any


def test_no_import_errors(dagbag: Any) -> None:
    """All DAG files should import without errors."""
    assert len(dagbag.import_errors) == 0, (
        f"DAG import errors: {dagbag.import_errors}"
    )


def test_dataops_dag_exists(dagbag: Any) -> None:
    """The dataops_pipeline DAG must be present in the DagBag."""
    assert "dataops_pipeline" in dagbag.dags, (
        "dataops_pipeline DAG not found in DagBag"
    )


def test_minimum_task_count(dagbag: Any) -> None:
    """The DAG must have at least 10 tasks."""
    dag = dagbag.dags["dataops_pipeline"]
    task_count = len(dag.tasks)
    assert task_count >= 10, (
        f"Expected >= 10 tasks, found {task_count}"
    )


def test_dag_has_sla_callback(dagbag: Any) -> None:
    """The DAG must have an sla_miss_callback configured."""
    dag = dagbag.dags["dataops_pipeline"]
    assert dag.sla_miss_callback is not None, (
        "sla_miss_callback is not set on the DAG"
    )


def test_all_tasks_have_retries(dagbag: Any) -> None:
    """Every task must have retries >= 1."""
    dag = dagbag.dags["dataops_pipeline"]
    for task in dag.tasks:
        assert task.retries >= 1, (
            f"Task '{task.task_id}' has retries={task.retries}, expected >= 1"
        )


def test_all_tasks_have_timeout(dagbag: Any) -> None:
    """Every task must have an execution_timeout set."""
    dag = dagbag.dags["dataops_pipeline"]
    for task in dag.tasks:
        assert task.execution_timeout is not None, (
            f"Task '{task.task_id}' has no execution_timeout"
        )
        assert task.execution_timeout <= timedelta(hours=2), (
            f"Task '{task.task_id}' timeout too long: {task.execution_timeout}"
        )


def test_task_group_order(dagbag: Any) -> None:
    """Bronze tasks must run before Silver, and Silver before Gold.

    Verifies the dependency chain by checking that downstream task IDs
    of each group contain the expected successors.
    """
    dag = dagbag.dags["dataops_pipeline"]
    task_ids = [t.task_id for t in dag.tasks]

    # Find representative tasks from each group
    bronze_tasks = [t for t in task_ids if "bronze" in t.lower()]
    silver_tasks = [t for t in task_ids if "silver" in t.lower()]
    gold_tasks = [t for t in task_ids if "gold" in t.lower()]

    assert len(bronze_tasks) > 0, "No bronze tasks found"
    assert len(silver_tasks) > 0, "No silver tasks found"
    assert len(gold_tasks) > 0, "No gold tasks found"

    # Check that at least one bronze task is upstream of a silver task
    def get_all_downstream_ids(task_id: str) -> set:
        """Recursively collect all downstream task IDs."""
        task = dag.get_task(task_id)
        downstream = set()
        for dt in task.downstream_list:
            downstream.add(dt.task_id)
            downstream.update(get_all_downstream_ids(dt.task_id))
        return downstream

    # Bronze should eventually lead to Silver
    bronze_downstream = set()
    for bt in bronze_tasks:
        bronze_downstream.update(get_all_downstream_ids(bt))

    has_silver_downstream = any(st in bronze_downstream for st in silver_tasks)
    assert has_silver_downstream, (
        "No Silver task is downstream of any Bronze task"
    )

    # Silver should eventually lead to Gold
    silver_downstream = set()
    for st in silver_tasks:
        silver_downstream.update(get_all_downstream_ids(st))

    has_gold_downstream = any(gt in silver_downstream for gt in gold_tasks)
    assert has_gold_downstream, (
        "No Gold task is downstream of any Silver task"
    )


def test_sensor_uses_reschedule_mode(dagbag: Any) -> None:
    """The S3KeySensor must use mode='reschedule' to free worker slots."""
    dag = dagbag.dags["dataops_pipeline"]
    sensor_tasks = [
        t for t in dag.tasks if "sensor" in type(t).__name__.lower()
        or "wait" in t.task_id.lower()
    ]
    assert len(sensor_tasks) > 0, "No sensor tasks found in DAG"
    for sensor in sensor_tasks:
        assert sensor.mode == "reschedule", (
            f"Sensor '{sensor.task_id}' uses mode='{sensor.mode}', "
            "expected 'reschedule'"
        )
