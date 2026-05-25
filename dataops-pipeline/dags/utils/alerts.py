"""Slack alerting callbacks for Airflow DAGs.

Provides ``on_failure_slack`` (task failure callback) and
``on_sla_miss_slack`` (DAG-level SLA miss callback) that post
structured messages to a Slack webhook.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
import structlog

logger = structlog.get_logger(__name__)


def _get_webhook_url() -> str:
    """Retrieve the Slack webhook URL from environment.

    Returns:
        Slack webhook URL string.
    """
    return os.environ.get(
        "SLACK_WEBHOOK_URL",
        "",
    )


def _post_to_slack(payload: Dict[str, Any]) -> None:
    """Send a JSON payload to the configured Slack webhook.

    Args:
        payload: Slack message payload (blocks or text).
    """
    webhook_url = _get_webhook_url()
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("slack_notification_sent")
    except requests.RequestException as exc:
        logger.error("slack_notification_failed", error=str(exc))


def on_failure_slack(context: Dict[str, Any]) -> None:
    """Airflow on_failure_callback that posts task failure details to Slack.

    Args:
        context: Airflow callback context dictionary containing task
                 instance, DAG run, exception, and other metadata.
    """
    ti = context.get("task_instance")
    dag_id = context.get("dag", None)
    dag_name = dag_id.dag_id if dag_id else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception info")
    log_url = ti.log_url if ti else ""

    logger.error(
        "task_failed",
        dag_id=dag_name,
        task_id=task_id,
        execution_date=str(execution_date),
        exception=str(exception),
    )

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":rotating_light: Task Failed",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* `{dag_name}`"},
                    {"type": "mrkdwn", "text": f"*Task:* `{task_id}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:* `{execution_date}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Exception:* ```{str(exception)[:500]}```",
                    },
                ],
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Logs"},
                        "url": log_url or "https://airflow.example.com",
                    }
                ],
            },
        ]
    }

    _post_to_slack(payload)


def on_sla_miss_slack(
    dag: Any,
    task_list: Optional[str] = None,
    blocking_task_list: Optional[str] = None,
    slas: Optional[List[Any]] = None,
    blocking_tis: Optional[List[Any]] = None,
) -> None:
    """Airflow sla_miss_callback that posts SLA breach details to Slack.

    Args:
        dag: The DAG object that missed its SLA.
        task_list: Comma-separated task IDs that missed SLA.
        blocking_task_list: Comma-separated blocking task IDs.
        slas: List of SLA miss objects.
        blocking_tis: List of blocking TaskInstance objects.
    """
    dag_id = dag.dag_id if dag else "unknown"

    logger.warning(
        "sla_miss",
        dag_id=dag_id,
        task_list=task_list,
        blocking_task_list=blocking_task_list,
    )

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":warning: SLA Miss Detected",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Tasks:* `{task_list or 'N/A'}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Blocking:* `{blocking_task_list or 'N/A'}`",
                    },
                ],
            },
        ]
    }

    _post_to_slack(payload)
