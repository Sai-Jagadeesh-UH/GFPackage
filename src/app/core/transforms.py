"""Shared Polars batch transformation functions.

`batch` wraps any element-wise function into a pl.Series → pl.Series
function for use with map_batches.

Usage:
    # Fixed transform — decorate once, use directly:
    @batch
    def batch_float_parse(x: str) -> float:
        return float(str(x).replace(",", ""))

    pl.col("Val").map_batches(batch_float_parse, return_dtype=pl.Float64)

    # Parameterised transform — call batch() on a lambda inline:
    def batch_date_parse(fmt: str):
        return batch(lambda x: datetime.strptime(x, fmt).date())

    pl.col("Date").map_batches(batch_date_parse("%m-%d-%Y"), return_dtype=pl.Date)
"""

import functools
import operator
from datetime import datetime, date
from typing import Callable

import polars as pl


# ---------------------------------------------------------------------------
# Core decorator
# ---------------------------------------------------------------------------

def batch(fn: Callable) -> Callable:
    """Wrap an element-wise function into a pl.Series → pl.Series function."""
    @functools.wraps(fn)
    def _wrap(series: pl.Series) -> pl.Series:
        return pl.Series(map(fn, series))
    return _wrap


# ---------------------------------------------------------------------------
# Fixed batch transforms  (decorate with @batch, pass directly to map_batches)
# ---------------------------------------------------------------------------

@batch
def batch_ymonth_parse(d: date) -> int:
    """date → integer YYYYMM for partitioning."""
    return int(f"{d.year}{str(d.month).rjust(2, '0')}")


@batch
def batch_float_parse(x: str | None) -> float | None:
    """Numeric string with optional commas → float."""
    if x is None:
        return None
    return float(str(x).replace(",", ""))


@batch
def batch_absolute(x : int | float | None) -> float | None:
    """Absolute value as float."""
    if x is None:
        return None
    return float(abs(x))


@batch
def gf_padded_loc(v : str | int) -> str:
    """Sequence number → GF-prefixed 7-char LocID.  e.g. 1 → 'GF00001'"""
    return f"GF{str(int(v)):0>5}"


# ---------------------------------------------------------------------------
# Parameterised batch transforms  (call with config → returns a batch fn)
# ---------------------------------------------------------------------------

def batch_date_parse(fmt: str) -> Callable[[pl.Series], pl.Series]:
    """Date string parser for the given strptime format."""
    return batch(lambda x: datetime.strptime(x, fmt).date())


def padded_string(width: int = 7) -> Callable[[pl.Series], pl.Series]:
    """Zero-pad values to `width` chars (default 7 for LocID)."""
    return batch(lambda x: str(x).rjust(width, "0"))


def batch_fi_mapper(flow_map: dict[str, str]) -> Callable[[pl.Series], pl.Series]:
    """Map flow indicator descriptions to codes using the given flow_map."""
    return batch(lambda x: flow_map.get(x, "D"))


# ---------------------------------------------------------------------------
# DataFrame-level helpers
# ---------------------------------------------------------------------------

def build_gfloc_id(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add GFLocID = 3-digit zero-padded GFPipeID + 7-char LocID (10 chars total)."""
    return df.with_columns(
        pl.concat_str([pl.col("GFPipeID"), pl.col("LocID")], separator="").alias("GFLocID")
    )


def add_timestamp(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add Timestamp column with current datetime."""
    return df.with_columns(
        pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp")
    )


def filter_all_null(df: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    """Remove rows where ALL specified columns are null."""
    condition = functools.reduce(
        operator.and_,
        map(lambda col: pl.col(col).is_null(), columns),
    )
    return df.filter(~condition)
