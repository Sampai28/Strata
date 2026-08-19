# Strata

A local Hadoop stack — HDFS, Hive Metastore, Spark — used as a laboratory for measuring how physical data layout affects query performance. Two experiments, each with a hypothesis, a controlled configuration, and measured results.

## Why layout dominates

On a data platform of any size, the query engine is rarely the bottleneck. What decides whether a report runs in eight seconds or eight minutes is almost always physical: how the data is partitioned, what format it's in, how large the files are. A well-written query against badly laid-out data loses to a naive query against well-laid-out data, consistently and by margins that are hard to believe until you've measured them. Partition pruning turns a full scan into a directory listing. Columnar formats let a projection read one column's worth of bytes instead of the whole row. A table split across thousands of small files spends more time scheduling tasks and opening handles than reading data, and the NameNode carries the memory cost of every one of those blocks. None of this is subtle once measured, and almost all of it is invisible from the query text alone. Strata puts numbers on it.

## What runs

```
                    ┌──────────────────────────┐
                    │  generator (PySpark)     │
                    │  seeded star schema      │
                    │  + injected skew & dirt  │
                    └───────────┬──────────────┘
                                │ raw (CSV, untyped)
                                ▼
   ┌─────────────────────────────────────────────────────────┐
   │  HDFS                                                    │
   │   /strata/raw   /strata/curated   /strata/quarantine     │
   │   /strata/quality   /strata/experiments/{exp1,exp2}      │
   └──────────┬────────────────────────────┬─────────────────┘
              │                            │ table metadata
              │                            ▼
              │                 ┌──────────────────────┐
              │                 │  Hive Metastore      │
              │                 │  (Postgres backend)  │
              │                 └──────┬───────────────┘
              ▼                        │
   ┌──────────────────────────────┐    ▼
   │  Spark master + worker       │  HiveServer2 ──► beeline
   │                              │
   │   read raw (explicit schema) │
   │        │                     │
   │        ├─► quality gates ────┼──► quarantine (by rule)
   │        │                     │
   │        └─► curated ──────────┼──► reconciliation
   │                              │
   │   exp1, exp2                 │
   │   metrics from the REST API  │
   └───────────┬──────────────────┘
               ▼
          results/*.json
```

Records failing a quality gate branch to quarantine rather than being dropped, tagged with every rule they broke. Rule counters and reconciliation totals are written to HDFS as Parquet so they can be queried rather than only read in a log.

Spark reads and writes HDFS paths directly rather than using the Metastore as its catalog — the reasoning is in `docs/BUILD_NOTES.md`. Hive external tables over the same paths give beeline access.

## Memory tier

Sized against detected memory, because a Hadoop pseudo-cluster plus Spark workers will thrash a machine that can't hold them.

This machine has 31.3 GB and 16 logical CPUs, but WSL2 defaults to half the host, so Docker reports **15.27 GiB**. That selects the middle tier: **full HDFS, a single Spark worker at 4 GB, reduced JVM heaps on the Hadoop and Hive daemons**, History Server behind a Compose profile. Committed budget is about 12.3 GiB, leaving headroom deliberately — page cache directly affects the bytes-read figures below, and a VM with no free memory measures swapping.

HDFS is real here, which is why the `fsck` block counts mean what they say. Every service carries an explicit `mem_limit` and `cpus`; nothing runs unbounded.

`docs/wslconfig-recommended` holds a `.wslconfig` that would raise the VM to 20 GB and unlock the larger tier. It is not installed — copy it yourself, then switch `configs/tier.env`.

## Quickstart

Docker is the only prerequisite. Everything builds and runs in containers.

```bash
make up
make bench-smoke
```

`bench-smoke` runs generate → quality → exp1 → exp2 → hdfs-report against `configs/smoke.yaml` and writes raw measurements to `results/`.

```bash
make generate      # synthetic star schema
make quality       # raw → curated through the gates
make exp1          # partitioning
make exp2          # file format
make hdfs-report   # dfsadmin, fsck, du
make beeline       # connect to HiveServer2
make down          # stop
make clean         # stop and drop the HDFS volumes
```

- NameNode — http://localhost:9870
- Spark master — http://localhost:8080
- History Server (`make up-history`) — http://localhost:18080

Full scale is a separate config and a separate target, not wired into anything else:

```bash
make bench-full
```

**On Windows, run these from Git Bash.** The Makefile exports `MSYS_NO_PATHCONV=1`; without it Git Bash rewrites `/strata` into a Windows path and HDFS answers `No FileSystem for scheme "C"`, which points nowhere near the cause.

## Measured results

Smoke scale: 501,989 raw fact rows across 200 stores, 5,000 products, 20,000 members and 731 days. Seed 20240917. Single Spark worker, 4 cores, 16 shuffle partitions.

### Experiment 1 — partitioning

Same query on all three layouts: sum `amount` over a one-month date range. All three return **15,610 rows**, which is the check that makes the comparison meaningful.

| Layout | Query | Bytes read | Files | Blocks | Size on disk | Pruned |
|---|---:|---:|---:|---:|---:|:--:|
| unpartitioned | 0.76 s | 1.71 MB | 2 | 2 | 7.65 MB | no |
| partitioned by date | **0.27 s** | **0.11 MB** | 731 | 731 | 11.06 MB | **yes** |
| over-partitioned by product_id | 18.54 s | 12.82 MB | 5,000 | 5,000 | 23.56 MB | no |

Pruning is proved from the physical plan, not inferred from the clock. The date-partitioned scan carries four `PartitionFilters` (`isnotnull`, `>= 2024-03-01`, `<= 2024-03-31`) and a `ReadSchema` of two columns; the other two carry none. Bytes read drops **15×**, from 1.71 MB to 0.11 MB.

Two findings worth more than the headline:

**Partitioning made the table 45% bigger on disk.** 7.65 MB unpartitioned against 11.06 MB by date. Splitting 500K rows across 731 daily directories means 731 Parquet files averaging ~15 KB, and each one carries a footer, schema and dictionary pages. The per-file overhead is a fixed cost the unpartitioned layout pays twice and the partitioned layout pays 731 times. Partitioning is not free, and at this scale daily granularity is already past the point where it pays for itself in storage.

**Over-partitioning was 24× slower than not partitioning at all.** 18.54 s against 0.76 s, reading 7.5× more bytes and occupying 3× the disk. The predicate is on date, so 5,000 product directories prune nothing — the query opens every one of 5,000 files to answer a question a single 8 MB file answered in under a second. This is the clearest result in the project and it is a negative one: a layout decision made without reference to the query pattern actively destroyed performance.

### Experiment 2 — file format and codec

Same 497,057 curated rows written six ways, each read three ways.

| Format | Size on disk | Full-scan read | Projection read | Full | Projection | Filtered |
|---|---:|---:|---:|---:|---:|---:|
| parquet-snappy | 7.65 MB | 1.28 MB | 1.10 MB | 0.34 s | 0.24 s | 0.38 s |
| parquet-zstd | **5.32 MB** | 1.17 MB | **0.99 MB** | 0.18 s | 0.14 s | 0.17 s |
| orc-snappy | 6.61 MB | 1.55 MB | 1.20 MB | 0.74 s | 0.19 s | 0.26 s |
| orc-zstd | 5.34 MB | 1.32 MB | 1.08 MB | 0.20 s | 0.14 s | 0.14 s |
| avro | 11.37 MB | 11.38 MB | 11.38 MB | 0.85 s | 0.35 s | 0.25 s |
| csv | 27.50 MB | 27.50 MB | 27.50 MB | 0.58 s | 0.56 s | 0.30 s |

The projection column is the point. Summing one column out of nine, Parquet-ZSTD reads **0.99 MB** and CSV reads **27.50 MB** — a 28× difference for identical output. Look at the Avro and CSV rows: their projection read equals their full-scan read *exactly*. A row-oriented format cannot skip a column; reaching one field means reading and parsing every byte of every record. The columnar formats read less on projection than on full scan because they fetch only the column chunks they need.

ZSTD is worth taking: 5.32 MB against 7.65 MB for Parquet, **30% smaller**, with no read penalty visible here.

**An honest caveat on the timings.** At 500K rows every query in this table completes between 0.14 s and 0.85 s, which is the range where JVM warmup, task scheduling and page cache dominate. The wall-clock column should not be read as a format ranking — ORC-Snappy's 0.74 s full scan against Parquet-Snappy's 0.34 s is almost certainly noise rather than signal. The conclusions here rest on **bytes read and size on disk**, which are counted exactly and are not sensitive to what else the machine was doing. Separating those two is most of what the full-scale config exists for.

### HDFS

From `make hdfs-report` after the run:

```
Status: HEALTHY
 Total size:    150248801 B
 Total files:   5848
 Total blocks (validated):  5848 (avg. block size 25692 B)
 Minimally replicated blocks: 5848 (100.0 %)
```

Average block size **25 KB against a 128 MB block size** — every file in this dataset is three orders of magnitude smaller than the block it occupies. Per-variant:

```
   DIR_COUNT  FILE_COUNT  CONTENT_SIZE  PATH
           1           2       8024447  /strata/experiments/exp1/unpartitioned
         732         731      11602211  /strata/experiments/exp1/by_date
        5001        5000      24702758  /strata/experiments/exp1/over_partitioned
```

That last line is the small-file problem in one row: 5,000 files, 5,000 blocks, 5,000 NameNode inodes and 5,000 scheduler tasks to hold 24 MB of data.

### Data quality

| | Rows | |
|---|---:|---|
| raw | 501,989 | |
| curated | 497,057 | promoted |
| quarantined | 4,932 | 0.983% |
| warned | 2,559 | kept, counted |

Reconciliation **passed** exactly: `28,620,951.75 + 276,541.82 = 28,897,493.57`. Row counts and both control totals balance to the cent, which is only possible because money is DECIMAL end to end — on DOUBLE these sums would differ in the last places for reasons unconnected to the data.

Rules that fired:

| Rule | Rows | Severity |
|---|---:|---|
| null_member_id | 2,559 | warn |
| duplicate_transaction_id | 1,989 | reject |
| orphan_product_id | 1,496 | reject |
| non_positive_quantity | 971 | reject |
| date_out_of_range | 494 | reject |

Twelve further rules were evaluated and fired zero times. They are still reported at zero rather than omitted — a counter that only appears once it has triggered makes "has this ever fired?" unanswerable.

The generator injects these defects deliberately, at configured rates. A quality layer that has never rejected anything has not been tested.

## Design decisions

**Partition granularity.** Daily partitions are the reflexive default and experiment 1 shows the cost: 731 directories holding 15 KB each, and a table 45% larger than the same data unpartitioned. Below a few hundred GB, monthly is usually the better choice. The over-partitioned variant exists to show the other end of the same mistake.

**Why raw is CSV.** Landing data in the real world arrives untyped from an upstream extract. Writing raw as Parquet would carry the schema along with it and make the curated layer's explicit-schema enforcement a no-op. CSV forces the curated read to declare DECIMAL for money and to fail loudly if declaration and data disagree.

**Never `inferSchema`.** Inference costs an extra pass and is not stable across runs — a column that holds only integers today becomes a different type tomorrow when one null arrives, and `amount` in particular infers as DOUBLE, silently undoing the care taken to keep floating point out of money.

**ORC versus Parquet.** Measured rather than asserted, and at this scale the answer is that they are close: 5.32 MB against 5.34 MB with ZSTD, with read differences inside the noise floor. Parquet remains the safer default in a Spark-centric stack; ORC's real advantages are with Hive ACID tables and its built-in indexes on high-selectivity predicates, neither of which this measures.

**Deterministic generation.** All randomness derives from `xxhash64(seed, salt, row_id)` rather than `F.rand(seed)`, which Spark seeds per partition — meaning its output changes with parallelism. Hashing row identity makes the data a pure function of the seed, identical on one executor or fifty.

**Output committer v2.** v1 has the driver rename every file into place sequentially at commit; on 5,000 directories that dominated the runtime of sub-second queries. v2 trades all-or-nothing commit for speed, which is right for a lab that rewrites its variants each run and wrong for a production table with live consumers.

## Known limitations

Single-node pseudo-cluster, so network shuffle across real nodes, rack awareness and data locality are unobservable. The data is synthetic — distributions are modelled deliberately and the choices are in the generator, but they are still a model. No YARN and no multi-tenancy, so nothing here says anything about resource contention between competing jobs. And at smoke scale several effects sit inside the noise floor, which the results tables above call out where it matters rather than glossing.

## Layout

```
src/strata/generate/      synthetic data generator
src/strata/ingest/        raw → curated pipeline
src/strata/quality/       validation rules, quarantine, reconciliation
src/strata/experiments/   one module per experiment, metric capture
src/strata/metrics/       Spark REST + event-log parsing, HDFS stats
configs/                  smoke.yaml (default), full.yaml, tier env files
docker/                   compose, Hadoop/Hive/Spark images and conf
results/                  raw measurements as JSON
docs/                     BUILD_NOTES.md, wslconfig-recommended
Makefile, pyproject.toml, README.md, .gitignore
```
