"""Deterministic pseudo-randomness derived from row identity.

**Why not ``F.rand(seed)``.** Spark seeds ``rand`` per partition, mixing the
seed with the partition index. Change the parallelism — a different worker
count, a different ``spark.sql.shuffle.partitions``, an extra repartition
upstream — and every value changes. For a generator whose whole claim is
reproducibility, and whose output must be byte-comparable across the smoke and
full configs, that is disqualifying.

Hashing the row id instead makes each value a pure function of ``(seed, salt,
id)``. The same row produces the same number on one executor or fifty, today or
next year. The ``salt`` is what keeps independent draws independent: without it
every derived column for a given row would be perfectly correlated, and the
"random" store and product for a transaction would move together.

``xxhash64`` is used rather than ``hash`` because Spark's ``hash`` is a 32-bit
Murmur3 whose output space is small enough to show visible clustering when
divided down to a unit interval at tens of millions of rows.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# A large prime. Taking the modulus against a prime avoids the low-bit
# periodicity you get from a power of two, which would make `u < 0.004`-style
# rate tests systematically biased.
_MODULUS = 1_000_000_007


def uniform(id_col: Column, salt: str, seed: int) -> Column:
    """A deterministic double in [0, 1) for this row and this salt."""
    hashed = F.xxhash64(F.lit(seed), F.lit(salt), id_col)
    return F.pmod(hashed, F.lit(_MODULUS)).cast("double") / F.lit(float(_MODULUS))


def int_between(id_col: Column, salt: str, seed: int, low: int, high: int) -> Column:
    """A deterministic integer in [low, high). ``high`` is exclusive."""
    if high <= low:
        raise ValueError(f"empty range: [{low}, {high})")
    span = high - low
    return (F.lit(low) + F.floor(uniform(id_col, salt, seed) * F.lit(span))).cast("int")


def pick(id_col: Column, salt: str, seed: int, choices: list[str]) -> Column:
    """Deterministically choose one of ``choices``.

    Weighted by position rather than uniformly: the first entry is drawn most
    often. Real payment-type mixes are nothing like uniform — card dominates
    everywhere — and a uniform mix would make the payment_type column useless as
    a filter predicate in the pushdown experiment, because every value would
    select the same fraction of rows.
    """
    if not choices:
        raise ValueError("choices must not be empty")
    draw = uniform(id_col, salt, seed)
    # Geometric-ish decay: 1/2, 1/4, 1/8 ... with the remainder on the last.
    expression = F.lit(choices[-1])
    cumulative = 0.0
    for index, choice in enumerate(choices[:-1]):
        cumulative += 1.0 / (2 ** (index + 1))
        expression = F.when(draw < F.lit(cumulative), F.lit(choice)).otherwise(expression)
    return expression


def long_tail(id_col: Column, salt: str, seed: int, minimum: float, decay: float) -> Column:
    """An exponentially distributed positive value, for money-like quantities.

    ``minimum * exp(decay * u)`` gives a distribution with a dense floor and a
    thin, long upper tail — most baskets are small, a few are very large. A
    uniform amount column would make every predicate on amount select a
    predictable slice and would hide the row-group skipping that makes
    columnar formats worth measuring, because min/max statistics on a uniform
    column are useless for pruning.
    """
    return F.lit(minimum) * F.exp(F.lit(decay) * uniform(id_col, salt, seed))
