"""Measurement: Spark job metrics and filesystem statistics.

Numbers come from Spark's own accounting — the REST API on the running driver,
or the event log after the fact — never from wall-clock inference or from
reading the web UI. Wall clock alone cannot distinguish "read less data" from
"the page cache was warm", and those are exactly the two explanations that
matter when comparing storage layouts.
"""

from strata.metrics.capture import RunMetrics, measure
from strata.metrics.fsstats import FsStats, path_stats

__all__ = ["RunMetrics", "measure", "FsStats", "path_stats"]
