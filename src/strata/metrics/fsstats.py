"""Filesystem statistics: file count, bytes on disk, and HDFS block count.

Reached through the Hadoop ``FileSystem`` API over py4j rather than by shelling
out to ``hdfs dfs``. The official ``apache/spark`` image ships the Hadoop client
*jars* but not the ``hdfs`` shell script, so a subprocess call would fail inside
the very container the jobs run in. Going through the JVM that Spark has already
started costs nothing and works everywhere the driver works.

Block count is the number that makes experiment 3 legible. File count alone
understates the problem: what the NameNode holds in heap is one object per
block plus one per file, and what the scheduler pays is one task per split. A
600-file table of 4KB files and a 4-file table of 128MB files can hold identical
bytes and behave nothing alike.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class FsStats:
    """What a path costs, physically."""

    path: str
    file_count: int
    total_bytes: int
    block_count: int
    min_file_bytes: int
    max_file_bytes: int
    partition_dirs: int

    @property
    def avg_file_bytes(self) -> float:
        return self.total_bytes / self.file_count if self.file_count else 0.0

    @property
    def total_mb(self) -> float:
        return round(self.total_bytes / (1024 * 1024), 3)

    @property
    def avg_file_mb(self) -> float:
        return round(self.avg_file_bytes / (1024 * 1024), 4)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["avg_file_bytes"] = round(self.avg_file_bytes, 1)
        data["total_mb"] = self.total_mb
        data["avg_file_mb"] = self.avg_file_mb
        return data


# Files Spark writes that are not data. Counting them would inflate the file
# count of a well-compacted table by a constant and make small-file ratios wrong
# at low file counts, which is exactly where experiment 3 measures.
_IGNORED_PREFIXES = ("_", ".")


def _is_data_file(name: str) -> bool:
    return not name.startswith(_IGNORED_PREFIXES)


def path_stats(spark, path: str) -> FsStats:
    """Walk ``path`` recursively and total up what is physically there."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    root = jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(root):
        return FsStats(path, 0, 0, 0, 0, 0, 0)

    file_count = 0
    total_bytes = 0
    block_count = 0
    min_bytes = None
    max_bytes = 0
    partition_dirs = set()

    # listFiles(recursive=True) returns a RemoteIterator, which is lazy — it
    # pages through the NameNode rather than materialising a listing of a table
    # with twenty thousand files into driver memory.
    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        name = status.getPath().getName()
        if not _is_data_file(name):
            continue

        length = int(status.getLen())
        file_count += 1
        total_bytes += length
        max_bytes = max(max_bytes, length)
        min_bytes = length if min_bytes is None else min(min_bytes, length)

        # A zero-length file occupies no blocks but still costs a NameNode
        # inode and a scheduler task; getBlockLocations returns an empty array
        # for it, which would otherwise silently under-count.
        locations = status.getBlockLocations()
        block_count += len(locations) if locations is not None else 0

        parent = status.getPath().getParent().toString()
        if parent != root.toString():
            partition_dirs.add(parent)

    return FsStats(
        path=path,
        file_count=file_count,
        total_bytes=total_bytes,
        block_count=block_count,
        min_file_bytes=int(min_bytes or 0),
        max_file_bytes=int(max_bytes),
        partition_dirs=len(partition_dirs),
    )


def delete_path(spark, path: str) -> bool:
    """Recursively remove a path. Used to make experiment runs idempotent."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hadoop_conf)
    target = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(target):
        return False
    return bool(fs.delete(target, True))
