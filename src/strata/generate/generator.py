"""The synthetic star-schema generator.

Produces a retail/pharmacy-style schema with three properties that matter more
than realism for its own sake:

1. **It is reproducible.** Every value derives from ``hash(seed, salt, row_id)``,
   so the output does not depend on parallelism. See :mod:`strata.generate.det`.
2. **It is skewed.** A handful of stores hold ~40% of rows. Uniformly
   distributed synthetic data produces no stragglers, and experiment 5 would
   have nothing to measure.
3. **It is dirty on purpose.** Duplicates, orphan keys, negative quantities,
   null members and out-of-range dates are injected at configured rates, so the
   quality layer's rejection paths are exercised on every run rather than only
   in unit tests.

Every distributional choice is documented in ``docs/data-model.md``.

The fact table is written as **CSV**, deliberately. Raw landing data in the real
world arrives untyped from an upstream extract, and writing Parquet here would
carry the schema along with it and make the curated layer's explicit-schema
enforcement a no-op. CSV forces the curated read to declare DECIMAL for money
and to fail loudly if the declaration and the data disagree — which is the whole
point of that gate.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from strata.config import Config
from strata.generate import det
from strata.generate.schema import MONEY
from strata.session import build_session, stop_quietly

PAYMENT_TYPES = ["CARD", "CASH", "MOBILE", "VOUCHER", "INSURANCE"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
STORE_TYPES = ["FLAGSHIP", "STANDARD", "EXPRESS", "PHARMACY_ONLY"]
CATEGORIES = ["OTC", "PRESCRIPTION", "BEAUTY", "GROCERY", "HOUSEHOLD", "SEASONAL"]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

def build_dim_store(spark: SparkSession, cfg: Config) -> DataFrame:
    seed = cfg.seed
    count = int(cfg["data.stores"])
    ids = spark.range(1, count + 1).withColumnRenamed("id", "store_id_long")
    key = F.col("store_id_long")
    return (
        ids.select(
            key.cast("int").alias("store_id"),
            F.concat(F.lit("STORE-"), F.lpad(key.cast("string"), 5, "0")).alias("store_name"),
            det.pick(key, "region", seed, REGIONS).alias("region"),
            det.pick(key, "store_type", seed, STORE_TYPES).alias("store_type"),
            F.date_add(F.lit(date(2005, 1, 1)), det.int_between(key, "opened", seed, 0, 6000))
            .alias("opened_date"),
        )
    )


def build_dim_product(spark: SparkSession, cfg: Config) -> DataFrame:
    seed = cfg.seed
    count = int(cfg["data.products"])
    ids = spark.range(1, count + 1).withColumnRenamed("id", "product_id_long")
    key = F.col("product_id_long")
    return (
        ids.select(
            key.cast("int").alias("product_id"),
            F.concat(F.lit("SKU-"), F.lpad(key.cast("string"), 8, "0")).alias("product_name"),
            det.pick(key, "category", seed, CATEGORIES).alias("category"),
            F.concat(F.lit("SUB-"), det.int_between(key, "subcat", seed, 1, 40).cast("string"))
            .alias("subcategory"),
            # Rounded to two places and cast to DECIMAL immediately. Leaving it
            # as a double even briefly is how a float sneaks into a money column.
            F.round(det.long_tail(key, "price", seed, 1.5, 3.2), 2).cast(MONEY).alias("unit_price"),
        )
    )


def build_dim_member(spark: SparkSession, cfg: Config) -> DataFrame:
    seed = cfg.seed
    count = int(cfg["data.members"])
    stores = int(cfg["data.stores"])
    ids = spark.range(1, count + 1).withColumnRenamed("id", "member_id_long")
    key = F.col("member_id_long")
    return (
        ids.select(
            key.cast("int").alias("member_id"),
            F.date_add(F.lit(date(2015, 1, 1)), det.int_between(key, "join", seed, 0, 3600))
            .alias("join_date"),
            det.pick(key, "tier", seed, TIERS).alias("tier"),
            det.int_between(key, "home_store", seed, 1, stores + 1).alias("home_store_id"),
        )
    )


def build_dim_calendar(spark: SparkSession, cfg: Config) -> DataFrame:
    start = _parse_date(cfg["data.start_date"])
    end = _parse_date(cfg["data.end_date"])
    days = (end - start).days + 1
    ids = spark.range(0, days).withColumnRenamed("id", "offset")
    cal_date = F.date_add(F.lit(start), F.col("offset").cast("int"))
    return ids.select(
        cal_date.alias("cal_date"),
        F.year(cal_date).alias("year"),
        F.quarter(cal_date).alias("quarter"),
        F.month(cal_date).alias("month"),
        F.dayofmonth(cal_date).alias("day_of_month"),
        F.dayofweek(cal_date).alias("day_of_week"),
        F.dayofweek(cal_date).isin(1, 7).alias("is_weekend"),
        # November and December. The generator pushes a quarter of all
        # transactions into this window, so any date-partitioned experiment sees
        # genuinely uneven partition sizes rather than a flat distribution.
        F.month(cal_date).isin(11, 12).alias("is_holiday_season"),
    )


# ---------------------------------------------------------------------------
# Fact
# ---------------------------------------------------------------------------

def build_fact(spark: SparkSession, cfg: Config) -> DataFrame:
    seed = cfg.seed
    rows = int(cfg["data.fact_rows"])
    stores = int(cfg["data.stores"])
    products = int(cfg["data.products"])
    members = int(cfg["data.members"])

    hot_stores = int(cfg["data.skew.hot_stores"])
    hot_share = float(cfg["data.skew.hot_share"])

    start = _parse_date(cfg["data.start_date"])
    end = _parse_date(cfg["data.end_date"])
    total_days = (end - start).days + 1
    years = list(range(start.year, end.year + 1))

    base = spark.range(0, rows).withColumnRenamed("id", "row_id")
    key = F.col("row_id")

    # -- Date, with a holiday-season bump ------------------------------------
    flat_offset = det.int_between(key, "day", seed, 0, total_days)

    # Nov 15 → Dec 31 of a deterministically chosen year in range.
    year_index = det.int_between(key, "season_year", seed, 0, len(years))
    holiday_offset = F.lit(0)
    for index, year in enumerate(years):
        window_start = date(year, 11, 15)
        if window_start < start:
            window_start = start
        offset_to_window = (window_start - start).days
        holiday_offset = F.when(
            year_index == F.lit(index),
            F.lit(offset_to_window) + det.int_between(key, "season_day", seed, 0, 47),
        ).otherwise(holiday_offset)

    is_seasonal = det.uniform(key, "season", seed) < F.lit(0.25)
    day_offset = F.least(
        F.when(is_seasonal, holiday_offset).otherwise(flat_offset),
        F.lit(total_days - 1),
    )
    transaction_date = F.date_add(F.lit(start), day_offset.cast("int"))

    # -- Late arrival ---------------------------------------------------------
    late_rate = float(cfg["data.dirt.late_arriving_rate"])
    is_late = det.uniform(key, "late", seed) < F.lit(late_rate)
    lag_days = F.when(is_late, det.int_between(key, "lag", seed, 1, 11)).otherwise(F.lit(0))
    ingest_date = F.date_add(transaction_date, lag_days.cast("int"))

    # -- Skewed store distribution -------------------------------------------
    # hot_share of rows land on the first `hot_stores` ids. With the smoke
    # defaults that is 40% of 500K rows across 4 stores, versus 60% across 196 —
    # roughly a 50x density difference, which is enough to produce a visible
    # straggler task without being a degenerate single-key case.
    is_hot = det.uniform(key, "hot", seed) < F.lit(hot_share)
    store_id = F.when(
        is_hot, det.int_between(key, "hot_store", seed, 1, hot_stores + 1)
    ).otherwise(det.int_between(key, "cold_store", seed, hot_stores + 1, stores + 1))

    product_id = det.int_between(key, "product", seed, 1, products + 1)
    member_id = det.int_between(key, "member", seed, 1, members + 1)
    quantity = det.int_between(key, "quantity", seed, 1, 9)
    unit = det.long_tail(key, "amount", seed, 1.5, 3.4)
    amount = F.round(unit * quantity.cast("double"), 2).cast(MONEY)
    payment_type = det.pick(key, "payment", seed, PAYMENT_TYPES)

    fact = base.select(
        key.alias("transaction_id"),
        transaction_date.alias("transaction_date"),
        ingest_date.alias("ingest_date"),
        store_id.alias("store_id"),
        product_id.alias("product_id"),
        member_id.alias("member_id"),
        quantity.alias("quantity"),
        amount.alias("amount"),
        payment_type.alias("payment_type"),
    )

    return _inject_defects(fact, cfg)


def _inject_defects(fact: DataFrame, cfg: Config) -> DataFrame:
    """Corrupt a configured share of rows, each defect independently drawn.

    Each defect uses its own salt, so a row unlucky enough to get two is a
    genuine coincidence at the product of the rates rather than a guaranteed
    pile-up. That matters for the quality report: rules must be able to fire
    independently, or the per-rule quarantine counts are not interpretable.
    """
    seed = cfg.seed
    products = int(cfg["data.products"])
    key = F.col("transaction_id")

    orphan_rate = float(cfg["data.dirt.orphan_product_rate"])
    negative_rate = float(cfg["data.dirt.negative_quantity_rate"])
    null_member_rate = float(cfg["data.dirt.null_member_rate"])
    out_of_range_rate = float(cfg["data.dirt.out_of_range_date_rate"])

    # Orphans point at product ids beyond the dimension's range, so the
    # referential-integrity check has something real to fail against rather than
    # a null it could have caught with a null check.
    orphan = det.uniform(key, "d_orphan", seed) < F.lit(orphan_rate)
    product_id = F.when(
        orphan, F.lit(products) + det.int_between(key, "d_orphan_id", seed, 1000, 1500)
    ).otherwise(F.col("product_id"))

    negative = det.uniform(key, "d_negative", seed) < F.lit(negative_rate)
    quantity = F.when(negative, -F.col("quantity")).otherwise(F.col("quantity"))

    null_member = det.uniform(key, "d_null_member", seed) < F.lit(null_member_rate)
    member_id = F.when(null_member, F.lit(None).cast("int")).otherwise(F.col("member_id"))

    # Split between absurdly old and absurdly future, because a range check that
    # only ever sees one side is half-tested.
    out_of_range = det.uniform(key, "d_date", seed) < F.lit(out_of_range_rate)
    far_past = det.uniform(key, "d_date_side", seed) < F.lit(0.5)
    transaction_date = F.when(
        out_of_range,
        F.when(far_past, F.lit(date(1899, 12, 31))).otherwise(F.lit(date(2099, 1, 1))),
    ).otherwise(F.col("transaction_date"))

    corrupted = fact.select(
        F.col("transaction_id"),
        transaction_date.alias("transaction_date"),
        F.col("ingest_date"),
        F.col("store_id"),
        product_id.alias("product_id"),
        member_id.alias("member_id"),
        quantity.alias("quantity"),
        F.col("amount"),
        F.col("payment_type"),
    )

    # Duplicates are a union of re-emitted rows, not a mutation, because that is
    # how they actually occur: an upstream job reruns and replays part of its
    # output. The replayed copy carries a later ingest_date, which is what makes
    # "keep the latest" a defensible dedup rule rather than an arbitrary one.
    duplicate_rate = float(cfg["data.dirt.duplicate_rate"])
    replays = (
        corrupted.where(det.uniform(F.col("transaction_id"), "d_dup", seed) < F.lit(duplicate_rate))
        .withColumn("ingest_date", F.date_add(F.col("ingest_date"), 1))
    )

    return corrupted.unionByName(replays)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_date(value: str) -> date:
    year, month, day = (int(part) for part in str(value).split("-"))
    return date(year, month, day)


def generate(cfg: Config) -> dict[str, int]:
    spark = build_session(cfg, "generate")
    try:
        raw = cfg.path("raw")

        dims = {
            "dim_store": build_dim_store(spark, cfg),
            "dim_product": build_dim_product(spark, cfg),
            "dim_member": build_dim_member(spark, cfg),
            "dim_calendar": build_dim_calendar(spark, cfg),
        }

        counts: dict[str, int] = {}
        for name, frame in dims.items():
            # Dimensions are small and land as Parquet: they are reference data
            # that a real platform would already hold in a typed store, and
            # round-tripping them through CSV would add nothing but parsing.
            frame.write.mode("overwrite").parquet(f"{raw}/{name}")
            counts[name] = frame.count()

        fact = build_fact(spark, cfg)
        (
            fact.write.mode("overwrite")
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .csv(f"{raw}/fact_transaction")
        )
        counts["fact_transaction"] = spark.read.option("header", "true").csv(
            f"{raw}/fact_transaction"
        ).count()

        return counts
    finally:
        stop_quietly(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Strata star schema")
    parser.add_argument("--config", default=None, help="path to a config yaml")
    args = parser.parse_args()

    cfg = Config.load(args.config) if args.config else Config.from_env()
    counts = generate(cfg)

    print(f"[generate] scale={cfg.scale} seed={cfg.seed}")
    for name, count in sorted(counts.items()):
        print(f"[generate] {name:20s} {count:>12,} rows")


if __name__ == "__main__":
    main()
