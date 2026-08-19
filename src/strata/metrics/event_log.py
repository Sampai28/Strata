"""Spark event-log parsing.

The REST API on a live driver is the primary metric source (see
:mod:`strata.metrics.rest`), because it is available the instant a job finishes
and needs no extra service. This module is the after-the-fact path: it reads the
JSON-lines event log Spark writes to HDFS and reconstructs the same totals.

It exists for three reasons. It lets a run be re-analysed later without
re-executing it. It is what the History Server is reading, so parsing it here
keeps the two views honest with each other. And it is testable against a fixture
file with no Spark session at all, which the REST path is not.

Event log format: one JSON object per line. The events that carry metrics are
``SparkListenerTaskEnd`` — task-level, which is why a stage that retried
contributes its retried tasks too, matching what the REST stage endpoint reports
for bytes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator


@dataclass
class TaskTotals:
    """Aggregated task metrics for one application or job group."""

    task_count: int = 0
    bytes_read: int = 0
    records_read: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    executor_run_time_ms: int = 0
    failed_tasks: int = 0
    # Per-task durations, kept so the skew experiment can report the straggler
    # rather than only the mean. A mean hides exactly the thing skew is about.
    task_durations_ms: list[int] = field(default_factory=list)

    @property
    def max_task_ms(self) -> int:
        return max(self.task_durations_ms) if self.task_durations_ms else 0

    @property
    def median_task_ms(self) -> int:
        if not self.task_durations_ms:
            return 0
        ordered = sorted(self.task_durations_ms)
        return ordered[len(ordered) // 2]

    @property
    def straggler_ratio(self) -> float:
        """Slowest task over median task.

        The single most useful skew number. A balanced stage sits near 1-2x; the
        deliberately skewed join in experiment 5 should be well above that, and
        salting should pull it back down.
        """
        median = self.median_task_ms
        return round(self.max_task_ms / median, 2) if median else 0.0


def iter_events(source: str | Path | Iterable[str]) -> Iterator[dict]:
    """Yield parsed events from a path or an iterable of lines."""
    if isinstance(source, (str, Path)):
        with open(source, "r", encoding="utf-8") as handle:
            yield from _parse_lines(handle)
    else:
        yield from _parse_lines(source)


def _parse_lines(lines: Iterable[str]) -> Iterator[dict]:
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            # Event logs of a still-running application end in a partial line.
            # Skipping it is correct; failing would make live inspection
            # impossible.
            continue


def aggregate_tasks(events: Iterable[dict], stage_ids: set[int] | None = None) -> TaskTotals:
    """Sum task metrics, optionally restricted to a set of stages."""
    totals = TaskTotals()

    for event in events:
        if event.get("Event") != "SparkListenerTaskEnd":
            continue
        if stage_ids is not None and int(event.get("Stage ID", -1)) not in stage_ids:
            continue

        totals.task_count += 1

        reason = event.get("Task End Reason", {})
        if reason.get("Reason") != "Success":
            totals.failed_tasks += 1

        metrics = event.get("Task Metrics") or {}
        totals.executor_run_time_ms += int(metrics.get("Executor Run Time", 0) or 0)
        totals.memory_spilled_bytes += int(metrics.get("Memory Bytes Spilled", 0) or 0)
        totals.disk_spilled_bytes += int(metrics.get("Disk Bytes Spilled", 0) or 0)

        input_metrics = metrics.get("Input Metrics") or {}
        totals.bytes_read += int(input_metrics.get("Bytes Read", 0) or 0)
        totals.records_read += int(input_metrics.get("Records Read", 0) or 0)

        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        # Local and remote are reported separately; a single-worker cluster puts
        # almost everything in "Local", and summing only the remote figure would
        # make every shuffle in this lab look like zero.
        totals.shuffle_read_bytes += int(shuffle_read.get("Local Bytes Read", 0) or 0)
        totals.shuffle_read_bytes += int(shuffle_read.get("Remote Bytes Read", 0) or 0)

        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        totals.shuffle_write_bytes += int(shuffle_write.get("Shuffle Bytes Written", 0) or 0)

        task_info = event.get("Task Info") or {}
        finish = int(task_info.get("Finish Time", 0) or 0)
        launch = int(task_info.get("Launch Time", 0) or 0)
        if finish and launch and finish >= launch:
            totals.task_durations_ms.append(finish - launch)

    return totals


def application_name(events: Iterable[dict]) -> str | None:
    for event in events:
        if event.get("Event") == "SparkListenerApplicationStart":
            return event.get("App Name")
    return None


def stage_ids_for_job_group(events: Iterable[dict], group: str) -> set[int]:
    """Stage ids belonging to jobs tagged with ``group``.

    The job group is carried in the job's properties under
    ``spark.jobGroup.id``, which is how ``setJobGroup`` records it.
    """
    stage_ids: set[int] = set()
    for event in events:
        if event.get("Event") != "SparkListenerJobStart":
            continue
        properties = event.get("Properties") or {}
        if properties.get("spark.jobGroup.id") != group:
            continue
        for stage in event.get("Stage Infos", []):
            stage_ids.add(int(stage.get("Stage ID", -1)))
    stage_ids.discard(-1)
    return stage_ids
