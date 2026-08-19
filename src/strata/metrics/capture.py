"""Per-run metric capture.

Wraps a unit of Spark work in a job group, then reads that group's stages back
out of the driver's REST API and sums the counters that matter for a storage
layout comparison.

**Why a job group rather than "everything since the last checkpoint".** A single
logical experiment step can fire several Spark jobs — one to build a plan, one
per action, sometimes an extra for a broadcast. Diffing global counters before
and after would sweep in anything Spark did concurrently, including work
belonging to a different measurement. ``setJobGroup`` tags every job the calling
thread launches, so attribution is exact.

**Why the maximum, not the sum, for task counts across attempts.** A stage that
is retried reports two attempts; summing them would double-count work that only
happened once from the query's point of view. Bytes are summed because a retry
really does re-read the data.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterator

from strata.metrics import rest


@dataclass
class RunMetrics:
    """Everything captured for one measured step."""

    name: str
    variant: str
    duration_seconds: float = 0.0
    bytes_read: int = 0
    records_read: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    task_count: int = 0
    stage_count: int = 0
    executor_run_time_ms: int = 0
    # Filled by the caller from FsStats where a step produces output.
    output_files: int | None = None
    output_bytes: int | None = None
    output_blocks: int | None = None
    notes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def shuffle_total_bytes(self) -> int:
        return self.shuffle_read_bytes + self.shuffle_write_bytes

    @property
    def spilled(self) -> bool:
        return (self.memory_spilled_bytes + self.disk_spilled_bytes) > 0


def _sum_stage_metrics(app_id: str, stage_ids: list[int], ui_base: str) -> dict[str, int]:
    totals = {
        "bytes_read": 0,
        "records_read": 0,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "memory_spilled_bytes": 0,
        "disk_spilled_bytes": 0,
        "task_count": 0,
        "executor_run_time_ms": 0,
    }
    counted_stages = 0

    for stage_id in stage_ids:
        attempts = rest.stage_attempts(app_id, stage_id, ui_base)
        if not attempts:
            continue
        counted_stages += 1
        task_counts: list[int] = []
        for attempt in attempts:
            totals["bytes_read"] += int(attempt.get("inputBytes", 0) or 0)
            totals["records_read"] += int(attempt.get("inputRecords", 0) or 0)
            totals["shuffle_read_bytes"] += int(attempt.get("shuffleReadBytes", 0) or 0)
            totals["shuffle_write_bytes"] += int(attempt.get("shuffleWriteBytes", 0) or 0)
            totals["memory_spilled_bytes"] += int(attempt.get("memoryBytesSpilled", 0) or 0)
            totals["disk_spilled_bytes"] += int(attempt.get("diskBytesSpilled", 0) or 0)
            totals["executor_run_time_ms"] += int(attempt.get("executorRunTime", 0) or 0)
            task_counts.append(int(attempt.get("numTasks", 0) or 0))
        totals["task_count"] += max(task_counts) if task_counts else 0

    totals["stage_count"] = counted_stages
    return totals


@contextmanager
def measure(
    spark,
    name: str,
    variant: str,
    ui_base: str = rest.DEFAULT_UI,
) -> Iterator[RunMetrics]:
    """Measure every Spark job launched inside the block.

    Usage::

        with measure(spark, "exp1", "partitioned") as m:
            df.where(...).count()
        m.duration_seconds  # populated on exit

    The metrics object is yielded before the work runs and mutated on exit, so
    the caller can attach notes inside the block and read numbers after it.
    """
    metrics = RunMetrics(name=name, variant=variant)
    group = f"strata-{name}-{variant}-{uuid.uuid4().hex[:8]}"
    context = spark.sparkContext

    # interruptOnCancel=False: cancelling a measured run mid-flight would leave
    # partially-written output that a later step might read as complete.
    context.setJobGroup(group, f"{name}/{variant}", False)
    started = time.perf_counter()
    try:
        yield metrics
    finally:
        metrics.duration_seconds = round(time.perf_counter() - started, 4)
        # PySpark has setJobGroup but no clearJobGroup — that method exists only
        # on the Scala SparkContext. Clearing the underlying local properties is
        # exactly what the Scala method does, and leaving them set would tag
        # every subsequent job in this thread with a stale group, silently
        # merging the next experiment's stages into this one's metrics.
        context.setLocalProperty("spark.jobGroup.id", None)
        context.setLocalProperty("spark.job.description", None)
        context.setLocalProperty("spark.job.interruptOnCancel", None)

        try:
            app_id = rest.application_id(ui_base)
            stage_ids: list[int] = []
            for job in rest.jobs_in_group(app_id, group, ui_base):
                stage_ids.extend(int(stage) for stage in job.get("stageIds", []))
            totals = _sum_stage_metrics(app_id, sorted(set(stage_ids)), ui_base)
            for key, value in totals.items():
                setattr(metrics, key, value)
        except rest.SparkRestError as exc:
            # A failure to collect metrics must not fail the experiment, but it
            # must be visible: a run whose counters are silently zero would look
            # like a spectacular optimisation.
            metrics.notes["metrics_error"] = str(exc)


def timed(fn: Callable[[], Any]) -> tuple[Any, float]:
    """Wall-clock a callable. Used where a Spark job group is not involved."""
    started = time.perf_counter()
    result = fn()
    return result, round(time.perf_counter() - started, 4)
