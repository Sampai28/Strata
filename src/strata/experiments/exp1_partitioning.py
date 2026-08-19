"""Experiment 1 — partitioning strategy.

Hypothesis: a date-range query against a table partitioned by date reads
dramatically fewer bytes than the same query against an unpartitioned table,
because partition pruning eliminates whole directories before any file is
opened. Over-partitioning on a high-cardinality key is expected to be *worse*
than no partitioning at all, because the metadata cost of listing thousands of
tiny directories exceeds the scan it saves.

The measurement that matters is not the stopwatch. It is ``PartitionFilters``
appearing in the physical plan and ``PartitionCount`` showing that only the
matching partitions were scanned. Runtime alone cannot distinguish pruning from
a warm cache, and at smoke scale the whole dataset fits in page cache, which is
exactly the condition under which a naive timing comparison lies.
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from strata.config import Config
from strata.experiments.base import ExperimentRun, plan_evidence, summarise, write_results
from strata.metrics.capture import measure
from strata.metrics.fsstats import delete_path, path_stats
from strata.session import build_session, stop_quietly

EXPERIMENT = "exp1"


def _write_variants(spark, cfg: Config) -> dict[str, str]:
    """Materialise the three layouts. Written once, queried repeatedly."""
    curated = f"{cfg.path('curated')}/fact_transaction"
    base = cfg.experiment_path(EXPERIMENT)
    over_key = cfg["experiments.exp1.over_partition_key"]

    fact = spark.read.parquet(curated)
    paths = {
        "unpartitioned": f"{base}/unpartitioned",
        "by_date": f"{base}/by_date",
        "over_partitioned": f"{base}/over_partitioned",
    }

    for path in paths.values():
        delete_path(spark, path)

    fact.write.mode("overwrite").parquet(paths["unpartitioned"])

    # repartition() by the partition column before partitionBy() is not an
    # optimisation, it is a correctness-of-measurement requirement. Without it
    # every one of the N shuffle partitions holds rows for every date, so each
    # task writes a file into each directory and the output is N files per
    # partition — 731 dates x 16 tasks is ~11,000 files. That would silently
    # turn this experiment into a second small-file experiment and make its
    # timings meaningless. Repartitioning first co-locates each key on one task,
    # giving one file per directory.
    (
        fact.repartition(F.col("transaction_date"))
        .write.mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(paths["by_date"])
    )

    # The pathology: one directory per product_id. At smoke scale that is ~5000
    # directories holding ~100 rows each. Even repartitioned this is slow to
    # write, which is itself part of the finding — over-partitioning costs on
    # the write path as well as the read path.
    (
        fact.repartition(F.col(over_key))
        .write.mode("overwrite")
        .partitionBy(over_key)
        .parquet(paths["over_partitioned"])
    )

    return paths


def _date_range_query(spark, path: str, date_from: str, date_to: str):
    return (
        spark.read.parquet(path)
        .where(
            (F.col("transaction_date") >= F.lit(date_from))
            & (F.col("transaction_date") <= F.lit(date_to))
        )
        .agg(F.count(F.lit(1)).alias("rows"), F.sum("amount").alias("amount"))
    )


def run(cfg: Config) -> list[ExperimentRun]:
    spark = build_session(cfg, EXPERIMENT)
    try:
        date_from = str(cfg["experiments.exp1.date_from"])
        date_to = str(cfg["experiments.exp1.date_to"])
        paths = _write_variants(spark, cfg)

        runs: list[ExperimentRun] = []
        for variant, path in paths.items():
            query = _date_range_query(spark, path, date_from, date_to)

            with measure(spark, EXPERIMENT, variant) as metrics:
                result = query.collect()[0]

            # Plan evidence is read AFTER execution, not before. With Adaptive
            # Query Execution the pre-execution plan is a placeholder marked
            # `isFinalPlan=false`, and the scan node in it does not yet carry
            # PartitionCount because the partitions have not been resolved.
            # Reading it early gives an incomplete plan that looks like an
            # absence of pruning.
            evidence = plan_evidence(query)

            stats = path_stats(spark, path)
            runs.append(ExperimentRun(
                experiment=EXPERIMENT,
                variant=variant,
                metrics=metrics,
                fs=stats,
                plan=evidence,
                extra={
                    "date_from": date_from,
                    "date_to": date_to,
                    "matched_rows": int(result["rows"]),
                    "matched_amount": str(result["amount"]),
                    # The ratio that makes pruning legible: how many of the
                    # table's partitions did the query have to touch?
                    "total_partition_dirs": stats.partition_dirs,
                    "partition_scan_ratio": (
                        round(evidence["partition_count_scanned"] / stats.partition_dirs, 4)
                        if evidence["partition_count_scanned"] and stats.partition_dirs
                        else None
                    ),
                },
            ))

        write_results(EXPERIMENT, runs, cfg.scale, extra={
            "hypothesis": (
                "Date partitioning prunes to the queried range; over-partitioning "
                "on a high-cardinality key costs more in metadata than it saves in scan."
            ),
            "date_from": date_from,
            "date_to": date_to,
            "over_partition_key": cfg["experiments.exp1.over_partition_key"],
        })
        return runs
    finally:
        stop_quietly(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 1: partitioning")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = Config.load(args.config) if args.config else Config.from_env()
    runs = run(cfg)

    print(f"[{EXPERIMENT}] partitioning — scale={cfg.scale}")
    print(summarise(runs))
    for item in runs:
        print(
            f"[{EXPERIMENT}] {item.variant:<20} "
            f"pruned={item.plan['partition_filters_present']} "
            f"filters={item.plan['partition_filter_count']} "
            f"cols_read={item.plan['read_schema_columns']} "
            f"dirs={item.extra['total_partition_dirs']} "
            f"read_MB={item.metrics.bytes_read / 1048576:.2f}"
        )


if __name__ == "__main__":
    main()
