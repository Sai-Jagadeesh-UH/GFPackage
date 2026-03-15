"""Shared Polars batch transformation functions.

These are extracted from the 3 duplicated copies across OAMunge, OCMunge, NNMunge.
Each function operates on a pl.Series via map_batches.
"""

from datetime import datetime

import polars as pl


# ---------------------------------------------------------------------------
# Batch parsers (used with pl.col(...).map_batches(...))
# ---------------------------------------------------------------------------

def batch_date_parse(series: pl.Series, fmt: str) -> pl.Series:
    """Parse date strings into date objects using the given format.

    Common formats across pipelines:
      OA:  "%m-%d-%Y"
      SG:  "%Y%m%d"
      NN:  "%m/%d/%Y %H:%M"
    """
    return pl.Series(
        map(lambda s: datetime.strptime(s, fmt).date(), series)
    )


def batch_ymonth_parse(series: pl.Series) -> pl.Series:
    """Convert date series to integer YYYYMM format for partitioning."""
    return pl.Series(
        map(
            lambda d: int(f"{d.year}{str(d.month).rjust(2, '0')}"),
            series,
        )
    )


def padded_string(series: pl.Series, width: int = 6) -> pl.Series:
    """Left-pad values with zeros to a fixed width."""
    return pl.Series(
        map(lambda s: str(s).rjust(width, "0"), series)
    )


def batch_float_parse(series: pl.Series) -> pl.Series:
    """Parse numeric strings with commas into floats."""
    return pl.Series(
        map(lambda s: float(str(s).replace(",", "")), series)
    )


def batch_absolute(series: pl.Series) -> pl.Series:
    """Take absolute value and cast to float."""
    return pl.Series(map(float, map(abs, series)))


def batch_fi_mapper(series: pl.Series, flow_map: dict[str, str]) -> pl.Series:
    """Map flow indicator descriptions to single-char codes.

    Each pipeline provides its own flow_map. Common examples:
      OA:  {"Delivery": "D", "Receipt": "R", "Storage Injection": "D", "Storage Withdrawal": "R"}
      SG:  {"TD1": "F", "TD2": "B"}
      NN:  {"D": "D", "R": "R", "B": "B", "Delivery": "D", "Receipt": "R", ...}
    """
    return pl.Series(
        map(lambda s: flow_map.get(s, "D"), series)
    )


# ---------------------------------------------------------------------------
# Column composition helpers (used with .with_columns())
# ---------------------------------------------------------------------------

def compose_gfloc(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add GFLOC column = concat(GFPipeID, RowType, GFLocID, FlowInd)."""
    return df.with_columns(
        pl.concat_str(
            [pl.col("GFPipeID"), pl.col("RowType"),
             pl.col("GFLocID"), pl.col("FlowInd")],
            separator="",
        ).alias("GFLOC")
    )


def add_modeling_columns(
    df: pl.LazyFrame,
    row_type: str,
    parent_pipe: str,
) -> pl.LazyFrame:
    """Add the standard modeling columns: EffGasMonth, GFLocID, RowType.

    Expects df already has: EffGasDay, Loc, GFPipeID.
    """
    return df.with_columns(
        pl.col("EffGasDay")
        .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
        .alias("EffGasMonth"),

        pl.col("GFPipeID").cast(pl.Int64),

        pl.col("Loc")
        .map_batches(lambda s: padded_string(s), return_dtype=pl.String)
        .alias("GFLocID"),

        pl.lit(row_type).alias("RowType"),
    )


def add_timestamp(df: pl.LazyFrame) -> pl.LazyFrame:
    """Add Timestamp column with current datetime."""
    return df.with_columns(
        pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp")
    )


def filter_all_null(df: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    """Remove rows where ALL specified columns are null."""
    import functools
    import operator

    condition = functools.reduce(
        operator.and_,
        map(lambda col: pl.col(col).is_null(), columns),
    )
    return df.filter(~condition)
