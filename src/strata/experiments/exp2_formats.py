"""Experiment 2 — file format and compression codec.

Hypothesis: the columnar formats (Parquet, ORC) beat the row-oriented ones
(Avro, CSV) heavily on projection and moderately on filtered scan, and the gap
is *largest* on projection because a columnar reader can skip whole column
chunks without decoding them. CSV should be worst on every axis and largest on
disk. ZSTD should be smaller than Snappy and slightly slower to read.

Three query shapes are measured against each format, because "which format is
faster" has no answer without saying faster at what:

* **full scan** — read every column, aggregate. Dominated by decompression and
  total bytes.
* **projection** — read one column out of nine. This is where columnar layout
  earns its keep: Parquet and ORC read one column chunk per row group, while
  Avro and CSV must read and parse every byte of every row to reach one field.
* **filtered scan** — a selective predicate. Columnar formats can additionally
  skip row groups whose min/max statistics exclude the predicate, so the win
  compounds.
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from strata.config import Config
from strata.experiments.base import ExperimentRun, plan_evidence, summarise, write_results
from strata.metrics.capture import measure
from strata.metrics.fsstats import delete_path, path_stats
from strata.session import build_session, stop_quietly

EXPERIMENT = "exp2"

# format name -> (spark format, write options)
FORMATS: dict[str, tuple[str, dict[str, str]]] = {
    "parquet-snappy": ("parquet", {"compression": "snappy"}),
    "parquet-zstd": ("parquet", {"compression": "zstd"}),
    "orc-snappy": ("orc", {"compression": "snappy"}),
    "orc-zstd": ("orc", {"compression": "zstd"}),
    # Avro is row-oriented binary with a schema. It is the honest middle
    # ground: compact and typed, but a projection still has to walk every record.
    "avro": ("avro", {"compression": "snappy"}),
    # Uncompressed CSV is the control. Everything about it is bad and it is
    # still extremely common in real landing zones.
    "csv": ("csv", {"header": "true"}),
}


def _write_variants(spark, cfg: Config, formats: list[str]) -> dict[str, str]:
    curated = f"{cfg.path('curated')}/fact_transaction"
    base = cfg.experiment_path(EXPERIMENT)
    fact = spark.read.parquet(curated)

    # One partition per variant keeps file count constant across formats, so the
    # size and scan comparisons are about the format and not about how many
    # files each happened to produce.
    fact = fact.coalesce(4).persist()
    fact.count()

    paths: dict[str, str] = {}
    for name in formats:
        spark_format, options = FORMATS[name]
        path = f"{base}/{name}"
        delete_path(spark, path)
        writer = fact.write.mode("overwrite").format(spark_format)
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.save(path)
        paths[name] = path

    fact.unpersist()
    return paths


def _reader(spark, name: str, path: str):
    spark_format, _ = FORMATS[name]
    reader = spark.read.format(spark_format)
    if spark_format == "csv":
        # CSV carries no schema, so it needs one supplied or every column comes
        # back as a string and the aggregate below would be measuring string
        # parsing rather than the format. This is itself part of the finding:
        # the other formats are self-describing and CSV is not.
        from strata.generate.schema import FACT_SCHEMA
        reader = reader.option("header", "true").schema(FACT_SCHEMA)
    return reader.load(path)


def run(cfg: Config) -> list[ExperimentRun]:
    spark = build_session(cfg, EXPERIMENT)
    try:
        formats = list(cfg["experiments.exp2.formats"])
        projection_column = str(cfg["experiments.exp2.projection_column"])
        filter_column = str(cfg["experiments.exp2.filter_column"])
        paths = _write_variants(spark, cfg, formats)

        runs: list[ExperimentRun] = []
        for name in formats:
            path = paths[name]
            stats = path_stats(spark, path)

            # -- full scan --------------------------------------------------
            full = _reader(spark, name, path).agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum("amount").alias("amount"),
                F.sum("quantity").alias("quantity"),
            )
            with measure(spark, EXPERIMENT, f"{name}:full") as full_metrics:
                full.collect()

            # -- single-column projection ------------------------------------
            projection = _reader(spark, name, path).agg(
                F.sum(projection_column).alias("total")
            )
            with measure(spark, EXPERIMENT, f"{name}:projection") as proj_metrics:
                projection.collect()
            projection_plan = plan_evidence(projection)

            # -- predicate-filtered scan --------------------------------------
            filtered = (
                _reader(spark, name, path)
                .where(F.col(filter_column) <= F.lit(4))
                .agg(F.count(F.lit(1)).alias("rows"))
            )
            with measure(spark, EXPERIMENT, f"{name}:filtered") as filter_metrics:
                filtered.collect()
            filtered_plan = plan_evidence(filtered)

            for suffix, metrics, plan in (
                ("full", full_metrics, {}),
                ("projection", proj_metrics, projection_plan),
                ("filtered", filter_metrics, filtered_plan),
            ):
                runs.append(ExperimentRun(
                    experiment=EXPERIMENT,
                    variant=f"{name}:{suffix}",
                    metrics=metrics,
                    # Filesystem stats are identical across the three query
                    # shapes for a format; attached to each so the report can
                    # chart size against any of them without a join.
                    fs=stats,
                    plan=plan,
                    extra={
                        "format": name,
                        "query": suffix,
                        "columnar": name.startswith(("parquet", "orc")),
                        "projection_column": projection_column,
                        "filter_column": filter_column,
                    },
                ))

        write_results(EXPERIMENT, runs, cfg.scale, extra={
            "hypothesis": (
                "Columnar formats win largest on single-column projection because "
                "they read one column chunk instead of every row; CSV is worst "
                "everywhere and largest on disk."
            ),
            "formats": formats,
        })
        return runs
    finally:
        stop_quietly(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 2: file format")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = Config.load(args.config) if args.config else Config.from_env()
    runs = run(cfg)

    print(f"[{EXPERIMENT}] file format — scale={cfg.scale}")
    print(summarise(runs))


if __name__ == "__main__":
    main()
