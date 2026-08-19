"""Shared experiment plumbing: plan inspection and result serialisation.

The plan-inspection functions are what separate this from a stopwatch exercise.
A date-range query that runs faster on a partitioned table has *not*
demonstrated partition pruning — it may have been reading a warm page cache, or
a smaller file, or fewer columns. Pruning is demonstrated by reading it out of
the physical plan: ``PartitionFilters`` non-empty and ``PartitionCount`` less
than the total. That evidence is captured into the results file alongside the
timing, so the writeups can cite it rather than assert it.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from strata.metrics.capture import RunMetrics
from strata.metrics.fsstats import FsStats

# Written inside the container to the bind-mounted repo, so results land on the
# host without an extra copy step.
RESULTS_DIR = Path(os.environ.get("STRATA_RESULTS_DIR", "/opt/strata/results"))


@dataclass
class ExperimentRun:
    """One measured variant within an experiment."""

    experiment: str
    variant: str
    metrics: RunMetrics
    fs: FsStats | None = None
    plan: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        payload = {
            "experiment": self.experiment,
            "variant": self.variant,
            **self.metrics.to_dict(),
            "plan": self.plan,
            "extra": self.extra,
        }
        if self.fs is not None:
            payload["fs"] = self.fs.to_dict()
            # Promote the three numbers every writeup quotes, so the summary
            # tables do not have to reach into a nested object.
            payload["output_files"] = self.fs.file_count
            payload["output_bytes"] = self.fs.total_bytes
            payload["output_blocks"] = self.fs.block_count
        return payload


# ---------------------------------------------------------------------------
# Physical plan inspection
# ---------------------------------------------------------------------------

_PARTITION_FILTERS = re.compile(r"PartitionFilters:\s*\[(.*?)\]", re.DOTALL)
_PUSHED_FILTERS = re.compile(r"PushedFilters:\s*\[(.*?)\]", re.DOTALL)
_DATA_FILTERS = re.compile(r"DataFilters:\s*\[(.*?)\]", re.DOTALL)
_READ_SCHEMA = re.compile(r"ReadSchema:\s*struct<(.*?)>", re.DOTALL)

# Spark 3.5 does NOT emit "PartitionCount" in the FileScan node of the executed
# plan — that field appears in some other Spark versions and in the SQL tab, but
# not here, and looking for it just yields a permanent null that reads like
# "pruning did not happen". Verified against a real plan: the scan line carries
# PartitionFilters, PushedFilters and ReadSchema, and nothing else about
# partitions. Kept only so an older or newer Spark that does emit it is picked
# up rather than ignored.
_PARTITION_COUNT = re.compile(r"PartitionCount:\s*(\d+)")

# Counting the predicates pushed into partition elimination is the portable
# evidence. Empty means no pruning; non-empty names the exact predicates the
# scan used to skip directories before opening a single file.
_FILTER_SPLIT = re.compile(r",\s*(?![^()]*\))")


def executed_plan(dataframe) -> str:
    """The post-optimisation physical plan, as Spark will actually run it.

    ``executedPlan`` rather than ``optimizedPlan``: partition pruning and filter
    pushdown are decided during physical planning, so the logical plan does not
    show them and would make a pruned query look identical to an unpruned one.
    """
    return dataframe._jdf.queryExecution().executedPlan().toString()


def plan_evidence(dataframe) -> dict[str, Any]:
    """Extract the pruning and pushdown evidence from a plan."""
    plan = executed_plan(dataframe)

    partition_filters = _first(_PARTITION_FILTERS, plan)
    pushed_filters = _first(_PUSHED_FILTERS, plan)
    data_filters = _first(_DATA_FILTERS, plan)
    read_schema = _first(_READ_SCHEMA, plan)
    partition_count = _PARTITION_COUNT.search(plan)

    return {
        "partition_filters": partition_filters,
        "partition_filters_present": bool(partition_filters and partition_filters.strip()),
        "partition_filter_count": (
            len([f for f in _FILTER_SPLIT.split(partition_filters) if f.strip()])
            if partition_filters.strip() else 0
        ),
        # Null on Spark 3.5; see the note on _PARTITION_COUNT above.
        "partition_count_scanned": int(partition_count.group(1)) if partition_count else None,
        "pushed_filters": pushed_filters,
        "pushed_filters_present": bool(pushed_filters and pushed_filters.strip()),
        "data_filters": data_filters,
        "read_schema_columns": _schema_columns(read_schema),
        # An Exchange node is a shuffle. Its absence after bucketing is the
        # evidence that bucketing worked; timing alone would not distinguish
        # "no shuffle" from "a fast shuffle".
        "exchange_count": plan.count("Exchange"),
        "broadcast_present": "BroadcastHashJoin" in plan or "BroadcastExchange" in plan,
        "sort_merge_join_present": "SortMergeJoin" in plan,
        "plan_text": plan,
    }


def _first(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text)
    return match.group(1).strip() if match else ""


def _schema_columns(read_schema: str) -> int:
    """Count top-level columns in a ReadSchema string.

    Naive comma splitting would miscount nested structs, but the schemas here
    are flat and staying flat; a nested column would need this revisited rather
    than silently miscounted, so it is worth stating.
    """
    if not read_schema:
        return 0
    return len([part for part in read_schema.split(",") if part.strip()])


# ---------------------------------------------------------------------------
# Result serialisation
# ---------------------------------------------------------------------------

def write_results(experiment: str, runs: list[ExperimentRun], scale: str,
                  extra: dict[str, Any] | None = None) -> Path:
    """Write one experiment's raw measurements to results/<experiment>.json."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    target = RESULTS_DIR / f"{experiment}.json"

    payload = {
        "experiment": experiment,
        "scale": scale,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "runs": [run.to_dict() for run in runs],
        "extra": extra or {},
    }
    target.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return target


def load_results(experiment: str) -> dict:
    target = RESULTS_DIR / f"{experiment}.json"
    if not target.is_file():
        raise FileNotFoundError(f"no results for {experiment}; run `make {experiment}` first")
    return json.loads(target.read_text(encoding="utf-8"))


def summarise(runs: list[ExperimentRun]) -> str:
    """A compact console table. Printed by every experiment on completion."""
    header = (
        f"{'variant':<26}{'secs':>9}{'read MB':>11}{'shuffle MB':>12}"
        f"{'files':>8}{'blocks':>8}{'size MB':>10}"
    )
    lines = [header, "-" * len(header)]
    for run in runs:
        metrics = run.metrics
        lines.append(
            f"{run.variant:<26}"
            f"{metrics.duration_seconds:>9.2f}"
            f"{metrics.bytes_read / 1048576:>11.2f}"
            f"{metrics.shuffle_total_bytes / 1048576:>12.2f}"
            f"{(run.fs.file_count if run.fs else 0):>8}"
            f"{(run.fs.block_count if run.fs else 0):>8}"
            f"{(run.fs.total_mb if run.fs else 0):>10.2f}"
        )
    return "\n".join(lines)
