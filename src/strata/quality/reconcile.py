"""Reconciliation: prove nothing was lost or invented between raw and curated.

The identity being checked is deliberately simple, because a control that is
hard to explain is a control nobody trusts:

    raw = curated + quarantine

for row count, for the sum of ``amount``, and for the sum of ``quantity``. Every
raw row goes to exactly one of the two destinations, so the three totals must
agree exactly. Not approximately — **exactly**. This is why money is DECIMAL: on
DOUBLE these sums would differ in the last places for no reason connected to the
data, and the only ways to cope would be an epsilon tolerance (which hides real
breaks of small value) or ignoring the check.

A break blocks promotion. It does not warn.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from strata.config import Config
from strata.quality.validate import read_raw_fact


@dataclass
class ControlTotals:
    row_count: int
    total_amount: Decimal
    total_quantity: int

    def to_dict(self) -> dict:
        return {
            "row_count": self.row_count,
            "total_amount": str(self.total_amount),
            "total_quantity": self.total_quantity,
        }


@dataclass
class ReconciliationResult:
    raw: ControlTotals
    curated: ControlTotals
    quarantine: ControlTotals
    breaks: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.breaks

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "raw": self.raw.to_dict(),
            "curated": self.curated.to_dict(),
            "quarantine": self.quarantine.to_dict(),
            "breaks": self.breaks,
        }


class ReconciliationBreak(RuntimeError):
    """Raised when the totals do not reconcile. Blocks promotion."""


def _control_totals(frame: DataFrame) -> ControlTotals:
    # coalesce to zero: SUM over a column that is entirely null returns null,
    # and null != 0 would report a break on an empty quarantine — the one case
    # that is unambiguously healthy.
    row = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.coalesce(F.sum("amount"), F.lit(Decimal("0"))).alias("total_amount"),
        F.coalesce(F.sum("quantity"), F.lit(0)).alias("total_quantity"),
    ).collect()[0]

    return ControlTotals(
        row_count=int(row["row_count"]),
        total_amount=Decimal(str(row["total_amount"])),
        total_quantity=int(row["total_quantity"]),
    )


def reconcile(spark: SparkSession, cfg: Config, write: bool = True) -> ReconciliationResult:
    raw_frame = read_raw_fact(spark, cfg)
    curated_frame = spark.read.parquet(f"{cfg.path('curated')}/fact_transaction")

    quarantine_path = f"{cfg.path('quarantine')}/fact_transaction"
    try:
        quarantine_frame = spark.read.parquet(quarantine_path)
    except Exception:
        # No quarantine directory means nothing was rejected. That is a valid
        # state, not an error, and it must reconcile against an empty total.
        quarantine_frame = curated_frame.limit(0)

    raw_totals = _control_totals(raw_frame)
    curated_totals = _control_totals(curated_frame)
    quarantine_totals = _control_totals(quarantine_frame)

    breaks: list[str] = []

    expected_rows = curated_totals.row_count + quarantine_totals.row_count
    if raw_totals.row_count != expected_rows:
        breaks.append(
            f"row_count: raw={raw_totals.row_count} != "
            f"curated={curated_totals.row_count} + quarantine={quarantine_totals.row_count} "
            f"({expected_rows}); difference={raw_totals.row_count - expected_rows}"
        )

    expected_amount = curated_totals.total_amount + quarantine_totals.total_amount
    if raw_totals.total_amount != expected_amount:
        breaks.append(
            f"total_amount: raw={raw_totals.total_amount} != "
            f"curated={curated_totals.total_amount} + "
            f"quarantine={quarantine_totals.total_amount} ({expected_amount}); "
            f"difference={raw_totals.total_amount - expected_amount}"
        )

    expected_quantity = curated_totals.total_quantity + quarantine_totals.total_quantity
    if raw_totals.total_quantity != expected_quantity:
        breaks.append(
            f"total_quantity: raw={raw_totals.total_quantity} != "
            f"curated={curated_totals.total_quantity} + "
            f"quarantine={quarantine_totals.total_quantity} ({expected_quantity}); "
            f"difference={raw_totals.total_quantity - expected_quantity}"
        )

    result = ReconciliationResult(
        raw=raw_totals,
        curated=curated_totals,
        quarantine=quarantine_totals,
        breaks=breaks,
    )

    if write:
        _write_result(spark, cfg, result)

    return result


def _write_result(spark: SparkSession, cfg: Config, result: ReconciliationResult) -> None:
    """Persist the reconciliation so it is queryable from Hive, not just logged.

    A control total that only exists in a job's stdout cannot be trended, and
    trending is most of the value: a single run's totals tell you almost
    nothing, while the same totals over thirty runs tell you when a rule started
    firing that never used to.
    """
    measured_at = datetime.now(timezone.utc).isoformat()
    rows = [
        (layer, totals.row_count, totals.total_amount, totals.total_quantity)
        for layer, totals in (
            ("raw", result.raw),
            ("curated", result.curated),
            ("quarantine", result.quarantine),
        )
    ]

    frame = spark.createDataFrame(
        rows,
        schema="layer string, row_count bigint, total_amount decimal(20,2), total_quantity bigint",
    ).withColumn("scale", F.lit(cfg.scale)) \
     .withColumn("passed", F.lit(result.passed)) \
     .withColumn("break_count", F.lit(len(result.breaks))) \
     .withColumn("measured_at", F.lit(measured_at))

    frame.coalesce(1).write.mode("overwrite").parquet(f"{cfg.path('quality')}/reconciliation")


def break_report(spark: SparkSession, cfg: Config, limit: int = 20) -> list[dict]:
    """Which records failed, and why.

    Reconciliation says a break exists; this says which rows to look at. A break
    report that reports only a delta leaves someone to find the rows by hand,
    which in practice means the break is acknowledged rather than investigated.
    """
    quarantine_path = f"{cfg.path('quarantine')}/fact_transaction"
    try:
        frame = spark.read.parquet(quarantine_path)
    except Exception:
        return []

    return [
        row.asDict()
        for row in frame.select(
            "transaction_id", "transaction_date", "store_id", "product_id",
            "member_id", "quantity", "amount", "failed_rules",
        ).limit(limit).collect()
    ]
