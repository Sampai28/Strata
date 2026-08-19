"""The raw → curated promotion.

Order matters and is the point of this module: validate, then reconcile, then
promote. Reconciliation runs against what validation actually wrote, not
against what it reported writing, so a bug in the validator that lost rows is
caught by the control totals rather than trusted away.

Promotion is blocked on a break. The curated data is written before
reconciliation runs — it has to be, since reconciliation reads it — so
"blocking promotion" means marking the dataset unpromoted and exiting non-zero,
not deleting it. The bad output stays on disk for investigation, which is what
you want at 3am, and downstream steps refuse to run because the promotion
marker is absent.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from strata.config import Config
from strata.quality.reconcile import ReconciliationResult, break_report, reconcile
from strata.quality.validate import ValidationResult, validate
from strata.session import build_session, stop_quietly

PROMOTION_MARKER = "_PROMOTED"


def _write_marker(spark, cfg: Config, validation: ValidationResult,
                  reconciliation: ReconciliationResult) -> None:
    """Write the marker that downstream steps check for.

    A single-row Parquet file rather than an empty sentinel, so it carries the
    evidence: when it was promoted, at what scale, how many rows survived, and
    whether reconciliation passed.
    """
    payload = [(
        cfg.scale,
        datetime.now(timezone.utc).isoformat(),
        validation.total_rows,
        validation.curated_rows,
        validation.quarantined_rows,
        reconciliation.passed,
    )]
    frame = spark.createDataFrame(
        payload,
        schema=("scale string, promoted_at string, raw_rows bigint, "
                "curated_rows bigint, quarantined_rows bigint, reconciled boolean"),
    )
    frame.coalesce(1).write.mode("overwrite").parquet(
        f"{cfg.path('curated')}/{PROMOTION_MARKER}"
    )


def is_promoted(spark, cfg: Config) -> bool:
    try:
        return spark.read.parquet(f"{cfg.path('curated')}/{PROMOTION_MARKER}").count() > 0
    except Exception:
        return False


def curate(cfg: Config) -> dict:
    spark = build_session(cfg, "curate")
    try:
        validation = validate(spark, cfg)
        reconciliation = reconcile(spark, cfg)

        report = {
            "validation": validation.to_dict(),
            "reconciliation": reconciliation.to_dict(),
        }

        if not reconciliation.passed:
            report["break_report"] = break_report(spark, cfg)
            return report

        # Dimensions are copied to curated unchanged. They come from the
        # generator already typed, and re-deriving them would risk the curated
        # dimension disagreeing with the one the FK checks were run against.
        raw = cfg.path("raw")
        curated = cfg.path("curated")
        for dim in ("dim_store", "dim_product", "dim_member", "dim_calendar"):
            (
                spark.read.parquet(f"{raw}/{dim}")
                .write.mode("overwrite")
                .parquet(f"{curated}/{dim}")
            )

        _write_marker(spark, cfg, validation, reconciliation)
        report["promoted"] = True
        return report
    finally:
        stop_quietly(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote raw to curated through the quality gates")
    parser.add_argument("--config", default=None)
    parser.add_argument("--json", action="store_true", help="emit the full report as JSON")
    args = parser.parse_args()

    cfg = Config.load(args.config) if args.config else Config.from_env()
    report = curate(cfg)

    validation = report["validation"]
    reconciliation = report["reconciliation"]

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(f"[quality] scale={cfg.scale}")
        print(f"[quality] raw rows         {validation['total_rows']:>12,}")
        print(f"[quality] curated rows     {validation['curated_rows']:>12,}")
        print(f"[quality] quarantined rows {validation['quarantined_rows']:>12,} "
              f"({validation['rejection_rate'] * 100:.3f}%)")
        print(f"[quality] warned rows      {validation['warned_rows']:>12,}")
        print("[quality] rule counts:")
        for name, count in sorted(validation["rule_counts"].items(), key=lambda kv: -kv[1]):
            marker = " " if count else "."
            print(f"[quality]  {marker} {name:28s} {count:>10,}")
        print(f"[recon]   raw        rows={reconciliation['raw']['row_count']:>10,} "
              f"amount={reconciliation['raw']['total_amount']:>18}")
        print(f"[recon]   curated    rows={reconciliation['curated']['row_count']:>10,} "
              f"amount={reconciliation['curated']['total_amount']:>18}")
        print(f"[recon]   quarantine rows={reconciliation['quarantine']['row_count']:>10,} "
              f"amount={reconciliation['quarantine']['total_amount']:>18}")
        print(f"[recon]   status: {'PASS' if reconciliation['passed'] else 'BREAK'}")

    if not reconciliation["passed"]:
        for line in reconciliation["breaks"]:
            print(f"[recon]   BREAK {line}", file=sys.stderr)
        for record in report.get("break_report", []):
            print(f"[recon]   offending {record}", file=sys.stderr)
        # Non-zero exit stops the Makefile chain. Promotion has not happened and
        # nothing downstream should treat curated as trustworthy.
        sys.exit(2)


if __name__ == "__main__":
    main()
