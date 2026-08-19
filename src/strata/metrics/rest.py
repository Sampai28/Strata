"""Spark REST API client.

The driver serves ``/api/v1`` on its own UI port (4040 by default) for as long
as the application is alive. Because our jobs run in client mode inside the
``spark-client`` container, the driver UI is on ``localhost`` from the job's own
point of view — no cluster round trip, no history server dependency, and the
metrics are available immediately rather than after the application ends and its
event log is rolled.

Only the standard library is used. This has to import inside the official
``apache/spark`` image, which has no ``requests``.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

DEFAULT_UI = "http://localhost:4040"


class SparkRestError(RuntimeError):
    pass


def _get(url: str, timeout: float = 10.0) -> Any:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise SparkRestError(f"GET {url} failed: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SparkRestError(f"GET {url} returned non-JSON: {exc}") from exc


def application_id(ui_base: str = DEFAULT_UI) -> str:
    apps = _get(f"{ui_base}/api/v1/applications")
    if not apps:
        raise SparkRestError("Spark REST API reports no applications")
    # A live driver serves exactly one. Taking [0] rather than searching is safe
    # here and would not be against a history server, which lists many.
    return str(apps[0]["id"])


def jobs_in_group(app_id: str, group: str, ui_base: str = DEFAULT_UI) -> list[dict]:
    jobs = _get(f"{ui_base}/api/v1/applications/{app_id}/jobs")
    return [job for job in jobs if job.get("jobGroup") == group]


def stage_attempts(app_id: str, stage_id: int, ui_base: str = DEFAULT_UI) -> list[dict]:
    try:
        return _get(f"{ui_base}/api/v1/applications/{app_id}/stages/{stage_id}")
    except SparkRestError:
        # A stage that was skipped entirely (its result came from cache) has no
        # attempt data. That is a legitimate outcome, not an error.
        return []


def executor_summary(app_id: str, ui_base: str = DEFAULT_UI) -> list[dict]:
    return _get(f"{ui_base}/api/v1/applications/{app_id}/executors")
