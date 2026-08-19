"""The validation rule catalogue.

Every rule has a name, a severity, and a condition that is TRUE when the row is
**bad**. Expressing the condition as "what is wrong" rather than "what is
acceptable" is deliberate: a rule written as a negated acceptance test has to
get null handling right twice, and in SQL three-valued logic a null quietly
satisfies neither branch. Here a null that should have failed a check fails it
explicitly, because the null test is its own rule.

**Nothing is ever silently dropped.** A row failing any rule goes to quarantine
tagged with every rule it failed, so a break report can say which records and
why. A pipeline that filters bad rows with a `where` clause and moves on has
destroyed the evidence needed to explain the row-count difference it just
created.

Rules are ordered from cheapest to most expensive so the catalogue reads in the
order the cost is incurred: row-wise expressions first, then the window
function, then the joins.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F

# Anything outside this window is a data error rather than a late arrival. The
# generator injects 1899-12-31 and 2099-01-01 specifically to exercise both ends.
MIN_VALID_DATE = date(2015, 1, 1)
MAX_VALID_DATE = date(2035, 12, 31)

# A single transaction of more than 500 units, or more than 100,000 in value, is
# not a retail sale. Range checks like these are judgement calls and should be
# stated as such rather than presented as physics.
MAX_QUANTITY = 500
MAX_AMOUNT = 100_000


@dataclass(frozen=True)
class Rule:
    """One validation rule.

    ``condition`` returns a Column that is TRUE for a row that VIOLATES the rule.
    ``severity`` is ``reject`` (quarantine the row) or ``warn`` (count it, keep
    the row). Warn exists so that observations which are suspicious but not
    disqualifying — an anonymous transaction — are measured rather than either
    ignored or thrown away.
    """

    name: str
    description: str
    condition: Callable[[], Column]
    severity: str = "reject"

    def __post_init__(self) -> None:
        if self.severity not in ("reject", "warn"):
            raise ValueError(f"{self.name}: severity must be 'reject' or 'warn'")


# ---------------------------------------------------------------------------
# Conditions
# ---------------------------------------------------------------------------

def _null_transaction_id() -> Column:
    return F.col("transaction_id").isNull()


def _null_transaction_date() -> Column:
    return F.col("transaction_date").isNull()


def _date_out_of_range() -> Column:
    # isNull() is excluded here so that a null date fails exactly one rule
    # (null_transaction_date) rather than two. Otherwise every null row would
    # inflate the out-of-range counter and the quarantine report would
    # misattribute the cause.
    return F.col("transaction_date").isNotNull() & (
        (F.col("transaction_date") < F.lit(MIN_VALID_DATE))
        | (F.col("transaction_date") > F.lit(MAX_VALID_DATE))
    )


def _null_store_id() -> Column:
    return F.col("store_id").isNull()


def _null_product_id() -> Column:
    return F.col("product_id").isNull()


def _null_quantity() -> Column:
    return F.col("quantity").isNull()


def _non_positive_quantity() -> Column:
    return F.col("quantity").isNotNull() & (F.col("quantity") <= F.lit(0))


def _quantity_out_of_range() -> Column:
    return F.col("quantity").isNotNull() & (F.col("quantity") > F.lit(MAX_QUANTITY))


def _null_amount() -> Column:
    return F.col("amount").isNull()


def _non_positive_amount() -> Column:
    return F.col("amount").isNotNull() & (F.col("amount") <= F.lit(0))


def _amount_out_of_range() -> Column:
    return F.col("amount").isNotNull() & (F.col("amount") > F.lit(MAX_AMOUNT))


def _null_payment_type() -> Column:
    return F.col("payment_type").isNull() | (F.trim(F.col("payment_type")) == F.lit(""))


def _null_member_id() -> Column:
    # WARN, not reject. An anonymous transaction is a real transaction and
    # dropping it would break the control totals for a reason that is not a
    # data defect. It is still counted, because a sudden rise in anonymous
    # transactions is worth knowing about.
    return F.col("member_id").isNull()


def _duplicate_transaction_id() -> Column:
    # Depends on the `_dup_rank` column added by the validator's window pass.
    # Keeping the rule declarative here, rather than embedding the window, means
    # the catalogue stays a flat list that can be enumerated and tested.
    return F.col("_dup_rank") > F.lit(1)


def _orphan_store_id() -> Column:
    return F.col("store_id").isNotNull() & F.col("_store_exists").isNull()


def _orphan_product_id() -> Column:
    return F.col("product_id").isNotNull() & F.col("_product_exists").isNull()


def _orphan_member_id() -> Column:
    return F.col("member_id").isNotNull() & F.col("_member_exists").isNull()


# ---------------------------------------------------------------------------
# The catalogue
# ---------------------------------------------------------------------------

RULES: list[Rule] = [
    Rule("null_transaction_id", "Primary key is null", _null_transaction_id),
    Rule("null_transaction_date", "Transaction date is null", _null_transaction_date),
    Rule("date_out_of_range",
         f"Transaction date outside [{MIN_VALID_DATE}, {MAX_VALID_DATE}]", _date_out_of_range),
    Rule("null_store_id", "Store foreign key is null", _null_store_id),
    Rule("null_product_id", "Product foreign key is null", _null_product_id),
    Rule("null_quantity", "Quantity is null", _null_quantity),
    Rule("non_positive_quantity", "Quantity is zero or negative", _non_positive_quantity),
    Rule("quantity_out_of_range", f"Quantity exceeds {MAX_QUANTITY}", _quantity_out_of_range),
    Rule("null_amount", "Amount is null", _null_amount),
    Rule("non_positive_amount", "Amount is zero or negative", _non_positive_amount),
    Rule("amount_out_of_range", f"Amount exceeds {MAX_AMOUNT}", _amount_out_of_range),
    Rule("null_payment_type", "Payment type is null or blank", _null_payment_type),

    Rule("null_member_id", "Member id is null (anonymous transaction)",
         _null_member_id, severity="warn"),

    Rule("duplicate_transaction_id",
         "transaction_id seen more than once; the earliest ingest is kept",
         _duplicate_transaction_id),

    Rule("orphan_store_id", "store_id has no matching dim_store row", _orphan_store_id),
    Rule("orphan_product_id", "product_id has no matching dim_product row", _orphan_product_id),
    Rule("orphan_member_id", "member_id has no matching dim_member row", _orphan_member_id),
]


def rule_names() -> list[str]:
    return [rule.name for rule in RULES]


def rejecting_rules() -> list[Rule]:
    return [rule for rule in RULES if rule.severity == "reject"]


def warning_rules() -> list[Rule]:
    return [rule for rule in RULES if rule.severity == "warn"]


def _assert_unique_names() -> None:
    names = rule_names()
    if len(set(names)) != len(names):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise ValueError(f"duplicate rule names: {duplicates}")


_assert_unique_names()
