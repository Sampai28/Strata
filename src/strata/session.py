"""Spark session construction.

One place that builds sessions, so every job in the project runs with the same
configuration and a measurement taken in one experiment is comparable with one
taken in another. A job that quietly set its own shuffle partitions would make
its numbers incomparable with everything else, and nothing about the output
would reveal it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from strata.config import Config

if TYPE_CHECKING:  # pragma: no cover - import only for type checking
    from pyspark.sql import SparkSession


def build_session(cfg: Config, app_suffix: str, extra_conf: dict[str, str] | None = None):
    """Create (or attach to) a Spark session for one job.

    ``app_suffix`` becomes part of the application name, which is how
    :mod:`strata.metrics` finds the right event log afterwards. Two jobs sharing
    a name is not an error Spark will report — it just makes the metrics
    ambiguous — so callers pass something specific.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(f"{cfg.get('spark.app_name', 'strata')}-{app_suffix}")
        .config("spark.sql.shuffle.partitions", str(cfg["spark.shuffle_partitions"]))
        .config("spark.executor.memory", str(cfg["spark.executor_memory"]))
        .config("spark.executor.cores", str(cfg["spark.executor_cores"]))
        .config("spark.driver.memory", str(cfg["spark.driver_memory"]))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", str(cfg["spark.event_log_dir"]))
        # Needed by experiment 4. Spark's bucketing is a catalog feature — the
        # bucket count and key live in table metadata, so bucketBy only works
        # through saveAsTable, never through a plain path write. This project
        # uses Spark's built-in catalog rather than the Hive Metastore (see
        # docs/BUILD_NOTES.md), which means bucketed tables are session-scoped:
        # fine for an experiment that creates and joins them in one run, and one
        # of the practical constraints the experiment writeup calls out.
        .config("spark.sql.warehouse.dir", f"{cfg.path('root')}/warehouse")
        # Commit algorithm v2 rather than v1.
        #
        # v1 has the driver rename every output file from a task-attempt
        # directory into place, sequentially, at job commit. On a table with
        # 5000 partition directories that is 5000 serial NameNode round trips
        # and it dominated the runtime of experiment 1 — minutes of commit for
        # sub-second queries.
        #
        # v2 lets each task commit its own output directly, which is much
        # faster and is why it is the common choice on HDFS. The trade is real
        # and worth stating: a job that fails midway under v2 leaves partial
        # output visible, whereas v1 is all-or-nothing. For a lab that rewrites
        # its variants from scratch on every run, partial output on failure
        # costs nothing; on a production table feeding downstream consumers the
        # calculus is the opposite.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()

    # WARN rather than INFO. At INFO, Spark emits several thousand lines per job
    # and the timing output this project actually cares about is lost in it.
    session.sparkContext.setLogLevel("WARN")
    return session


def stop_quietly(session) -> None:
    """Stop a session, ignoring the shutdown races that are not worth reporting."""
    try:
        session.stop()
    except Exception:  # pragma: no cover - shutdown only
        pass
