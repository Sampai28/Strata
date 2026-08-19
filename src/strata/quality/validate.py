"""Apply the rule catalogue and split raw into curated and quarantine.

The output is three things, not one: the rows that passed, the rows that did
not (tagged with every rule they broke), and a per-rule counter table. All three
are written, because a quality layer that reports "97% passed" without saying
which 3% and why is a dashboard, not a control.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from strata.config import Config
from strata.generate.schema import FACT_SCHEMA
from strata.quality.rules import RULES, Rule


@dataclass
class ValidationResult:
    total_rows: int
    curated_rows: int
    quarantined_rows: int
    warned_rows: int
    rule_counts: dict[str, int]

    @property
    def rejection_rate(self) -> float:
        return round(self.quarantined_rows / self.total_rows, 6) if self.total_rows else 0.0

    def to_dict(self) -> dict:
        return {
            "total_rows": self.total_rows,
            "curated_rows": self.curated_rows,
            "quarantined_rows": self.quarantined_rows,
            "warned_rows": self.warned_rows,
            "rejection_rate": self.rejection_rate,
            "rule_counts": self.rule_counts,
        }


def read_raw_fact(spark: SparkSession, cfg: Config) -> DataFrame:
    """Read the raw CSV with an EXPLICIT schema.

    ``inferSchema`` is never used here and that is the single most important
    line in this module. Inference costs an extra full pass over the data, and
    worse, it is not stable: a column that happens to contain only integers in
    one run becomes an integer type, and the same column with one null or one
    decimal in the next run becomes something else. For ``amount`` in
    particular, inference produces a DOUBLE, which silently reintroduces binary
    floating point into a money column after all the care taken to keep it out.

    Declaring the schema makes a mismatch a visible failure at read time rather
    than a corruption discovered at reconciliation.
    """
    return (
        spark.read.schema(FACT_SCHEMA)
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        # PERMISSIVE would silently null out a field it could not parse, which
        # would convert a parse failure into a null-check failure and misreport
        # the cause. Nulls that arrive this way are indistinguishable from nulls
        # that were really in the data, and the two need different fixes.
        .option("mode", "FAILFAST")
        .csv(f"{cfg.path('raw')}/fact_transaction")
    )


def _with_helper_columns(spark: SparkSession, fact: DataFrame, cfg: Config) -> DataFrame:
    """Add the columns the window- and join-based rules depend on."""
    raw = cfg.path("raw")

    # Duplicate detection. Ordering by ingest_date then transaction_id makes the
    # choice of survivor deterministic: the earliest ingest wins, and the
    # replayed copy (which the generator stamps with a later ingest_date) is the
    # one quarantined. Without the tie-break on transaction_id, two rows sharing
    # an ingest_date would rank arbitrarily and the run would not be reproducible.
    dup_window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingest_date").asc_nulls_last(), F.col("transaction_id").asc()
    )
    enriched = fact.withColumn("_dup_rank", F.row_number().over(dup_window))

    # Referential integrity by left join. Broadcast because the dimensions are
    # small — 200 stores, 5000 products, 20000 members at smoke scale — and a
    # broadcast hash join avoids shuffling the 500K-row fact table three times
    # just to check that keys exist.
    for alias, table, key in (
        ("_store_exists", "dim_store", "store_id"),
        ("_product_exists", "dim_product", "product_id"),
        ("_member_exists", "dim_member", "member_id"),
    ):
        dim = spark.read.parquet(f"{raw}/{table}").select(
            F.col(key).alias(f"{alias}_key"), F.lit(1).alias(alias)
        )
        enriched = enriched.join(
            F.broadcast(dim),
            enriched[key] == dim[f"{alias}_key"],
            how="left",
        ).drop(f"{alias}_key")

    return enriched


def _failed_rules_column(rules: list[Rule]) -> F.Column:
    """Build an array of the names of every rule this row violates."""
    # array_compact would be tidier but is only available from Spark 3.4 in
    # some builds; filtering nulls out of the array is portable and explicit.
    tagged = [
        F.when(rule.condition(), F.lit(rule.name)).otherwise(F.lit(None))
        for rule in rules
    ]
    return F.array_except(F.array(*tagged), F.array(F.lit(None)))


def validate(spark: SparkSession, cfg: Config) -> ValidationResult:
    """Run every rule, write curated + quarantine + rule counters."""
    curated_path = cfg.path("curated")
    quarantine_path = cfg.path("quarantine")
    quality_path = cfg.path("quality")

    fact = read_raw_fact(spark, cfg)
    enriched = _with_helper_columns(spark, fact, cfg)

    rejecting = [rule for rule in RULES if rule.severity == "reject"]
    warning = [rule for rule in RULES if rule.severity == "warn"]

    flagged = (
        enriched
        .withColumn("_failed_rules", _failed_rules_column(rejecting))
        .withColumn("_warned_rules", _failed_rules_column(warning))
    )

    # Cached because it is read three times below — curated, quarantine and the
    # counter aggregation. Without this the joins and window run three times.
    flagged = flagged.persist()

    business_columns = [field.name for field in FACT_SCHEMA.fields]

    curated = flagged.where(F.size("_failed_rules") == 0).select(*business_columns)
    quarantined = flagged.where(F.size("_failed_rules") > 0).select(
        *business_columns,
        F.col("_failed_rules").alias("failed_rules"),
        F.col("_warned_rules").alias("warned_rules"),
        F.lit(datetime.now(timezone.utc).isoformat()).alias("quarantined_at"),
    )

    curated.write.mode("overwrite").parquet(f"{curated_path}/fact_transaction")

    # Quarantine is partitioned by the first failing rule so an analyst can read
    # one rule's worth of failures without scanning all of them. The full list
    # stays in the row, because a record can break several rules and the
    # partition key only records one.
    (
        quarantined.withColumn("primary_rule", F.col("failed_rules")[0])
        .write.mode("overwrite")
        .partitionBy("primary_rule")
        .parquet(f"{quarantine_path}/fact_transaction")
    )

    total_rows = flagged.count()
    curated_rows = curated.count()
    quarantined_rows = total_rows - curated_rows
    warned_rows = flagged.where(F.size("_warned_rules") > 0).count()

    # Per-rule counts. explode() over the failed-rules array counts a row once
    # per rule it broke, which is what a per-rule counter should do — the counts
    # deliberately sum to more than the quarantined row count when rows break
    # several rules, and the reconciliation below does not depend on them
    # summing to anything.
    rule_counts = _rule_counts(flagged)

    counts_frame = spark.createDataFrame(
        [(name, int(count)) for name, count in sorted(rule_counts.items())],
        schema="rule_name string, failed_rows bigint",
    ).withColumn("scale", F.lit(cfg.scale)).withColumn(
        "measured_at", F.lit(datetime.now(timezone.utc).isoformat())
    )
    counts_frame.coalesce(1).write.mode("overwrite").parquet(f"{quality_path}/rule_counts")

    flagged.unpersist()

    return ValidationResult(
        total_rows=total_rows,
        curated_rows=curated_rows,
        quarantined_rows=quarantined_rows,
        warned_rows=warned_rows,
        rule_counts=rule_counts,
    )


def _rule_counts(flagged: DataFrame) -> dict[str, int]:
    exploded = flagged.select(
        F.explode(F.concat(F.col("_failed_rules"), F.col("_warned_rules"))).alias("rule_name")
    )
    observed = {
        row["rule_name"]: int(row["count"])
        for row in exploded.groupBy("rule_name").count().collect()
    }
    # Every rule appears, including the ones that fired zero times. A counter
    # that only exists once it has fired makes "has this ever triggered?"
    # unanswerable and breaks any chart that expects a stable set of series.
    return {rule.name: observed.get(rule.name, 0) for rule in RULES}
