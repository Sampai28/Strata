# Strata

A local Hadoop stack — HDFS, Hive Metastore, Spark — used as a laboratory for measuring how physical data layout affects query performance. Five experiments, each with a hypothesis, a controlled configuration, and a measurement.

## Why layout dominates

On a data platform of any size, the query engine is rarely the bottleneck. What decides whether a report runs in eight seconds or eight minutes is almost always physical: how the data is partitioned, what format it's in, how large the files are, whether the join key is co-located. A well-written query against badly laid-out data loses to a naive query against well-laid-out data, consistently and by margins that are hard to believe until you've measured them. Partition pruning turns a full scan into a directory listing. Columnar formats let a projection read one column's worth of bytes instead of the whole row. A table split across ten thousand small files spends more time scheduling tasks than reading data, and the NameNode carries the memory cost of every one of those blocks. None of this is subtle once measured, and almost all of it is invisible from the query text alone. Strata exists to put numbers on it.

## Architecture

```
                    ┌──────────────────────────┐
                    │  generator (PySpark)     │
                    │  seeded star schema      │
                    │  + injected skew & dirt  │
                    └───────────┬──────────────┘
                                │ raw
                                ▼
   ┌────────────────────────────────────────────────────────┐
   │  storage layer:  HDFS   (or MinIO + S3A on small RAM)   │
   │                                                         │
   │   /raw/…        /curated/…       /quarantine/…          │
   │   /experiments/{partitioning,formats,smallfiles,…}      │
   └──────────┬─────────────────────────┬───────────────────┘
              │                         │
              │                         │  table metadata
              │                         ▼
              │              ┌──────────────────────┐
              │              │  Hive Metastore      │
              │              │  (Postgres backend)  │
              │              └──────┬───────────────┘
              │                     │
              ▼                     ▼
   ┌──────────────────────────────────────────┐
   │  Spark  (master + workers)               │
   │                                          │
   │   ingest  →  quality gates  →  curated   │
   │                    │                     │
   │                    └──► quarantine       │
   │                         + recon tables   │
   │                                          │
   │   experiments 1–5, metrics captured      │
   │   from the REST API / event logs         │
   └───────┬──────────────────┬───────────────┘
           │                  │
           │                  ▼
           │        ┌──────────────────────┐
           │        │ Spark History Server │
           │        └──────────────────────┘
           ▼
   results/*.json ──► reports/strata.html
                 └──► Streamlit dashboard
                      · experiment comparison
                      · query-plan / pruning viewer
                      · quality + reconciliation panel

   HiveServer2 ──► beeline, sql/ example queries
```

Records that fail a quality gate branch out to quarantine rather than being dropped. Every rule has a name and a counter, and both the counters and the reconciliation results are written to Hive tables so they can be queried like anything else — a break report has to say *which* rows failed, not just that something did.

## Memory tiers

The stack is sized against available RAM, because a full Hadoop pseudo-cluster plus Spark workers will thrash a machine that can't hold it. The tier is selected through an env file rather than hard-coded, and every service in the Compose file carries an explicit `mem_limit` and `cpus` so nothing runs unbounded.

**24GB or more.** Everything: NameNode and DataNode, Postgres, Hive Metastore, HiveServer2, Spark master with two 4GB workers, History Server.

**Roughly 10 to 24GB.** Full HDFS, but a single Spark worker and reduced JVM heaps on the Hadoop and Hive daemons. History Server optional.

**Under 10GB.** HDFS is dropped and MinIO with the S3A connector takes its place, keeping Hive Metastore and Spark. This is a real substitution with real consequences. The storage-layout lessons all still hold — partitioning, file format, file size, and bucketing behave the same way against object storage — but the HDFS-specific parts don't. There are no blocks, so `hdfs fsck` block counts become object counts, and the NameNode memory argument in the small-file experiment becomes an argument about request overhead instead. The HDFS variant stays available as an opt-in Compose profile for a larger machine.

With the WSL2 backend, container memory is governed by `.wslconfig` rather than by Docker Desktop's settings sliders, which catches people out. A recommended `.wslconfig` for the selected tier lives in `docs/` for you to install yourself.

## Quickstart

```bash
docker compose -f docker/docker-compose.yml up -d
make bench-smoke
```

`bench-smoke` runs the whole thing end to end — generate, load, quality gates, all five experiments, report — against `configs/smoke.yaml`, at a scale sized to finish in a few minutes on a laptop.

Individual stages:

```bash
make generate      # synthetic star schema
make load          # raw → curated, through the quality gates
make quality       # validation + reconciliation only
make exp1          # … through exp5
make report        # reports/strata.html
make dashboard     # Streamlit
make hdfs-report   # fsck, du, count, dfsadmin -report
make test          # pytest
make down
```

- Dashboard — http://localhost:8501
- Spark master — http://localhost:8080
- History Server — http://localhost:18080
- NameNode — http://localhost:9870

```bash
beeline -u jdbc:hive2://localhost:10000
```

Full scale is a separate target, deliberately not wired into anything else. It wants 50M–200M rows and considerably more time and disk than the smoke config:

```bash
make bench-full CONFIG=configs/full.yaml
```

## The experiments

Each one writes raw timings to `results/` and has a writeup in `docs/experiments/` covering hypothesis, configuration, result, and verdict. Every run captures wall-clock duration, bytes read, shuffle read and write, spill, output file count, and total size on disk — pulled from the Spark REST API and event logs rather than read off the UI.

Results that contradict the hypothesis are reported as such. Five experiments that all confirm what the author expected would say more about the author than about the data.

**1 — Partitioning.** Unpartitioned versus partitioned by date versus deliberately over-partitioned on a high-cardinality key, measured with a date-range query. Pruning is proved by inspecting the physical plan for `PartitionFilters` and counting partitions scanned, not inferred from the fact that it ran faster.

**2 — File format.** Parquet, ORC, Avro, and uncompressed CSV, plus Snappy against ZSTD within the columnar formats. Compared on size, full-scan time, single-column projection, and predicate-filtered scan. The projection case is the interesting one, and the writeup explains why columnar wins there specifically.

**3 — Small files.** Write the fact table as thousands of tiny files, measure job duration, task count, and block count via `hdfs fsck`. Then compact to roughly 128MB targets and measure it all again. This is the most valuable of the five for anyone who has run a production cluster, because small files are the single most common pathology there is.

**4 — Bucketing.** A repeated fact-to-fact join on `member_id`, unbucketed against bucketed with matching bucket counts. Measures shuffle bytes and confirms shuffle elimination in the plan. The writeup is honest about where bucketing's constraints make it impractical — matching bucket counts across tables is a real coupling, and it doesn't survive schema evolution gracefully.

**5 — Pushdown and skew.** Predicate and projection pushdown verified through explain plans and bytes-read deltas. Then the injected `store_id` skew: measure the straggler, apply salting and broadcast joins for the small dimensions, measure the improvement, and compare Adaptive Query Execution on versus off.

## Data quality

The curated path uses explicit schemas and never `inferSchema`, which is an availability decision as much as a correctness one: schema inference reads the data twice and will happily change a column's type between runs because the sample happened to differ.

Validation covers nulls on all keys, referential integrity of the fact table's foreign keys against the dimensions, range checks on quantity, amount and date, and duplicate `transaction_id` detection. Monetary columns are DECIMAL throughout, with a test that reflects over the schemas and fails if a float type appears in any money field — the same reason every serious ledger avoids binary floating point, which cannot represent 0.10 and will not sum to what you expect.

Reconciliation compares row counts and control totals — sum of amount, sum of quantity — between raw and curated. A break blocks promotion rather than logging a warning, and the break report identifies the offending records.

The generator injects the failures on purpose: duplicate transaction ids, orphan product ids, negative quantities, null member ids, out-of-range dates. A quality layer that has never rejected anything hasn't been tested.

## HDFS

`docs/hdfs-cheatsheet.md` documents the commands the project uses and what each one tells you — `hdfs dfs -ls -R`, `-du -h`, `-count`, `hdfs fsck / -files -blocks`, and `hdfs dfsadmin -report`. The block count from `fsck` is the number to watch in experiment 3; it's the direct measure of what small files cost the NameNode.

The common ones are wrapped in Make targets so they're reproducible rather than remembered.

## Design decisions

**Partition granularity.** Daily partitions on a fact table are the usual default and are usually wrong at small scale — a year of data becomes 365 directories, and if each holds a few megabytes you've traded a scan problem for a metadata problem. Monthly is often the better choice below a few hundred GB. Experiment 1 includes a deliberately over-partitioned variant specifically to show the cost of getting this wrong in the other direction.

**Why 128MB.** It's the default HDFS block size, and a file materially smaller than one block wastes the scheduling overhead of a task that has almost nothing to read. Materially larger and you lose parallelism, since a single task reads the whole file. The target is a rule of thumb rather than a law, but it's a good one, and it's where compaction aims.

**ORC versus Parquet.** Parquet is the safer default in a Spark-centric stack. ORC's advantages show up with Hive ACID tables, with its built-in indexes and bloom filters on high-selectivity predicates, and sometimes on compression ratio for particular column types. Experiment 2 measures both rather than asserting a winner.

**Bucketing's constraints.** Bucketing eliminates the shuffle on a join, which is a large win when it applies. It applies less often than it seems: bucket counts have to match across both tables, the bucketing column has to be the join key, changing the bucket count means rewriting the table, and Spark and Hive have historically disagreed about bucketing metadata. The experiment measures the benefit; the writeup covers the cost.

## Known limitations

Single-node pseudo-cluster, so anything about network shuffle across real nodes, rack awareness, or data locality is unobservable here. The data is synthetic — the distributions are modelled deliberately and documented in `docs/data-model.md`, but they're still a model. There's no YARN and no multi-tenancy, so nothing about resource contention between competing jobs applies. And at smoke scale some effects are dominated by fixed overheads, which is part of why the full-scale config exists.

## Layout

```
src/strata/generate/      synthetic data generator
src/strata/ingest/        raw → curated pipelines
src/strata/quality/       validation rules, quarantine, reconciliation
src/strata/experiments/   one module per experiment, metric capture
src/strata/metrics/       Spark event-log / REST API parsing
app/                      Streamlit dashboard
sql/                      Hive DDL + example analytic queries
tests/                    pytest suite
configs/                  smoke.yaml (default), full.yaml, tier env files
docker/                   docker-compose.yml, hadoop/hive/spark confs
results/                  raw timings
reports/                  strata.html
docs/                     data-model.md, experiments/, hdfs-cheatsheet.md,
                          BUILD_NOTES.md
```
