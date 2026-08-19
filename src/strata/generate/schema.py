"""Explicit schemas for every table.

These are used on read in the curated path — never ``inferSchema``. Inference
reads the data an extra time and, worse, can pick a different type between runs
because the sample happened to differ: a column of integers that acquires one
null becomes a double, and a monetary column read from CSV becomes a float
silently. Declaring the schema makes a type change a loud failure at read time
instead of a quiet corruption downstream.

**Money is DECIMAL, everywhere, without exception.** ``DecimalType(12, 2)``
holds up to 10 digits before the point, which is more than any single retail
transaction needs, and it is exact. ``DoubleType`` cannot represent 0.10; sum a
few million of them and the control total will not reconcile, which is precisely
what the reconciliation layer exists to catch and precisely the bug it should
never have to catch. ``tests/test_money_types.py`` walks every schema here and
fails if a float type appears in any monetary field.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Columns whose name marks them as monetary. The float-type test uses this list,
# so a new money column must be named consistently or added here.
MONETARY_COLUMNS = {"amount", "unit_price", "control_total_amount", "line_total"}

MONEY = DecimalType(12, 2)


FACT_SCHEMA = StructType([
    StructField("transaction_id", LongType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=True),
    # When the record reached us, as distinct from when it happened. A small
    # share of rows arrive days late; that is what makes date-partitioned
    # ingestion non-trivial in reality and it is modelled here rather than
    # assumed away.
    StructField("ingest_date", DateType(), nullable=True),
    StructField("store_id", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    # Nullable on purpose: anonymous transactions are legitimate, and the
    # generator also injects nulls as a defect. Telling those apart is the
    # quality layer's problem, which is the point.
    StructField("member_id", IntegerType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("amount", MONEY, nullable=True),
    StructField("payment_type", StringType(), nullable=True),
])


DIM_STORE_SCHEMA = StructType([
    StructField("store_id", IntegerType(), nullable=False),
    StructField("store_name", StringType(), nullable=False),
    StructField("region", StringType(), nullable=False),
    StructField("store_type", StringType(), nullable=False),
    StructField("opened_date", DateType(), nullable=False),
])


DIM_PRODUCT_SCHEMA = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("subcategory", StringType(), nullable=False),
    StructField("unit_price", MONEY, nullable=False),
])


DIM_MEMBER_SCHEMA = StructType([
    StructField("member_id", IntegerType(), nullable=False),
    StructField("join_date", DateType(), nullable=False),
    StructField("tier", StringType(), nullable=False),
    StructField("home_store_id", IntegerType(), nullable=False),
])


DIM_CALENDAR_SCHEMA = StructType([
    StructField("cal_date", DateType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("quarter", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("day_of_month", IntegerType(), nullable=False),
    StructField("day_of_week", IntegerType(), nullable=False),
    StructField("is_weekend", BooleanType(), nullable=False),
    StructField("is_holiday_season", BooleanType(), nullable=False),
])


ALL_SCHEMAS = {
    "fact_transaction": FACT_SCHEMA,
    "dim_store": DIM_STORE_SCHEMA,
    "dim_product": DIM_PRODUCT_SCHEMA,
    "dim_member": DIM_MEMBER_SCHEMA,
    "dim_calendar": DIM_CALENDAR_SCHEMA,
}
