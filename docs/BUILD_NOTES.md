# Build notes

Everything here was worked out against the real stack rather than from
documentation, including the parts that went wrong.

## Memory tier

Detected before writing the Compose file:

```
Host physical      31.3 GB
Logical CPUs       16
WSL2 VM (free -m)  15642 MB total, ~15000 MB available
docker info        MemTotal 16402022400 (15.27 GiB), NCPU 16
~/.wslconfig       absent
```

WSL2 defaults to half of host RAM, which is why a 31 GB machine presents ~15 GB
to Docker. That lands in the 10–24 GB band, so **tier 2** was selected: full
HDFS, a single Spark worker at 4 GB, reduced JVM heaps on the Hadoop and Hive
daemons, History Server behind a profile.

No MinIO substitution was needed. HDFS is real here, which is why the `fsck`
block counts below mean what they say.

Committed budget is about 12.3 GiB of the 15.27 GiB available:

| Service | mem_limit | cpus |
|---|---|---|
| namenode | 1200m | 1.0 |
| datanode | 1200m | 1.0 |
| postgres | 384m | 0.5 |
| metastore | 1024m | 1.0 |
| hiveserver2 | 1024m | 1.0 |
| spark-master | 768m | 0.5 |
| spark-worker | 4608m | 4.0 |
| spark-client | 1536m | 2.0 |

The remaining ~3 GB is left deliberately unallocated. Page cache is not
overhead here — it directly affects the "bytes read" figures the experiments
report, and a VM with no free memory produces timings that measure swapping.

`SPARK_WORKER_MEMORY` (4g) is intentionally lower than the worker's container
limit (4608m). Setting them equal is how an executor gets OOM-killed by the
kernel — which Spark reports as a lost executor with no explanation — rather
than failing cleanly with a Spark-level memory error.

### Raising the tier

`docs/wslconfig-recommended` holds a `.wslconfig` that allocates 20 GB and 12
processors, which would move this machine into the top tier. **It is not
installed** — copy it to `%USERPROFILE%\.wslconfig`, run `wsl --shutdown`, then
`cp configs/tier-large.env configs/tier.env`.

With the WSL2 backend, `.wslconfig` governs container resources. Docker
Desktop's Resources sliders are disabled in that mode, which catches people out:
changing them does nothing, and the memory Docker reports is whatever WSL2 was
given.

## Assumed versions

Nothing here was resolved against a lockfile. These are the versions that were
pulled and actually ran.

| Component | Version | Where to change |
|---|---|---|
| Apache Hadoop | 3.3.6 | `docker/hadoop/Dockerfile`, compose `image:` |
| Apache Hive | 4.0.0 | `docker/hive/Dockerfile` |
| Apache Spark | 3.5.3 | `docker/spark/Dockerfile` |
| PostgreSQL | 16-alpine | compose |
| spark-avro | 3.5.3 / Scala 2.12 | `docker/spark/Dockerfile` build arg |
| PostgreSQL JDBC | 42.7.4 | `docker/hive/Dockerfile` build arg |
| Python (in Spark image) | 3.8 | fixed by the base image |

Python 3.8 inside the `apache/spark:3.5.3` image is worth knowing about: the
host has 3.14, but every Spark job runs against 3.8. That is why the source
avoids newer syntax in the modules that execute inside the cluster.

## One deliberate architectural deviation

**Spark does not use the Hive Metastore as its catalog.**

Spark 3.5 bundles a Hive 2.3.9 metastore client, and the official `apache/hive`
images begin at 4.0. Pointing one at the other means overriding
`spark.sql.hive.metastore.version` and supplying a matching jar set, which is a
well-known source of long, opaque failures. Instead:

- Spark reads and writes HDFS **paths** directly. The layout experiments operate
  on physical paths anyway, so nothing is lost — partitioning, file format and
  file size are properties of what is written, not of what the catalog thinks.
- The Metastore and HiveServer2 still run, and Hive **external** tables over
  those same paths give `beeline` access.
- Experiment 4 (bucketing, not built — see README) would use Spark's built-in
  catalog, which makes bucketed tables session-scoped. That is a real constraint
  and would belong in that experiment's writeup.

## Four things that went wrong

Recorded because each cost real time and none of them is obvious from the error.

### 1. The NameNode never formatted

`InconsistentFSStateException: Directory /hadoop/dfs/name is in an inconsistent
state` on every start.

The image's entrypoint formats only when the directory is **absent**:

```sh
if [ ! -d "$ENSURE_NAMENODE_DIR" ]; then
  /opt/hadoop/bin/hdfs namenode -format -force
fi
```

Mounting a named volume directly onto `dfs.namenode.name.dir` **creates** that
directory, so the check sees it, skips the format, and the NameNode then dies
complaining about corruption. Compounding it: `/hadoop` does not exist in the
base image, so Docker created the volume root-owned while the container runs as
uid 1000.

Fix: `docker/hadoop/Dockerfile` pre-creates an empty `/hadoop/dfs` owned by
`hadoop`, and the volume mounts one level up at `/hadoop/dfs`. The volume
inherits the ownership, `name/` stays absent inside a fresh volume, and the
format fires exactly once.

### 2. `env_file` does not drive Compose interpolation

`failed to cast to expected type: strconv.ParseFloat: parsing "": invalid syntax`

`env_file:` on a service sets variables **inside that container**. It does not
supply the `${VAR}` substitutions Compose performs while parsing the file, so
every `mem_limit` and `cpus` interpolated to an empty string. Those come from
the shell or from `--env-file` only.

Fix: always invoke through the Makefile, which passes
`--env-file configs/tier.env`.

### 3. `partitionBy` without a preceding `repartition`

Experiment 1 took 8m24s on its first run.

With N shuffle partitions, every task holds rows for every partition value, so
each task writes a file into each directory — 731 dates × 16 tasks is roughly
11,000 files, and the over-partitioned variant was far worse. The experiment had
accidentally become a small-file experiment.

Fix: `df.repartition(F.col(key))` before `partitionBy(key)` co-locates each key
on one task, giving one file per directory. Runtime fell to 2m50s.

### 4. `clearJobGroup` does not exist in PySpark

`AttributeError: 'SparkContext' object has no attribute 'clearJobGroup'`

It is on the Scala `SparkContext` but was never exposed in PySpark. Clearing the
underlying local properties (`spark.jobGroup.id` and friends) is exactly what
the Scala method does. Leaving them set would tag later jobs with a stale group
and silently merge one experiment's stages into another's metrics.

## Output committer: v2, not v1

`spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` is set to **2**.

v1 has the driver rename every output file into place sequentially at job
commit. On the 5,000-directory over-partitioned table that is 5,000 serial
NameNode round trips, and it dominated the runtime of sub-second queries.

v2 lets each task commit its own output. The trade is real: a job that fails
midway under v2 leaves partial output visible, where v1 is all-or-nothing. For a
lab that rewrites its variants from scratch every run that costs nothing; on a
production table with live downstream consumers the calculus reverses.

## Windows / Git Bash

`MSYS_NO_PATHCONV=1` is exported at the top of the Makefile. Without it, Git
Bash rewrites `/strata` in a `docker exec` argument into
`C:/Program Files/Git/strata`, and HDFS reports:

```
ls: No FileSystem for scheme "C"
```

which points nowhere near the actual cause.

## Reproducibility

All randomness derives from `xxhash64(seed, salt, row_id)` rather than
`F.rand(seed)`. Spark seeds `rand` per partition, so its output changes with
parallelism — a different worker count or shuffle-partition setting produces
different data. Hashing row identity makes the generated data a pure function of
the seed, identical on one executor or fifty.

Verified: the smoke config at seed 20240917 produces 501,989 fact rows
(500,000 plus 1,989 injected replays), 731 calendar days, 20,000 members, 5,000
products and 200 stores.
