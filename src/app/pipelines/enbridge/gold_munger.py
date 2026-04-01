"""Enbridge gold munger — implements BaseGoldMunger.

Reads silver parquets (raw cols + GFLocID) and transforms each dataset
into the gold fact schema, then concatenates into one gold DataFrame.

Silver → Gold transforms per dataset:
  OA  : date parse, float parse, flow map, GasMonth → fact_cols
  NN  : datetime parse, float parse, flow map, GasMonth → fact_cols
  SG  : TD1/TD2 split, abs, join LocConfigs for GFLocID, GasMonth → fact_cols
"""

import asyncio
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl

from app.base.munger import BaseGoldMunger
from app.base.types import RowType
from app.core.transforms import batch_absolute
from app.core.logging import logger

from . import config as cfg

FACT_COLS = [
    "GasMonth", "GasDay", "Dataset", "GFLocID", "LocName",
    "DesignCapacity", "OperatingCapacity", "TotalScheduledQuantity",
    "OperationallyAvailableCapacity", "IT", "FlowDirection", "Timestamp",
]


def _fill_fact_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Add any missing FACT_COLS as typed nulls, select FACT_COLS."""
    _null_types = {
        "GasMonth": pl.Int64, "GasDay": pl.Date, "Dataset": pl.String,
        "GFLocID": pl.String, "LocName": pl.String,
        "DesignCapacity": pl.Float64, "OperatingCapacity": pl.Float64,
        "TotalScheduledQuantity": pl.Float64, "OperationallyAvailableCapacity": pl.Float64,
        "IT": pl.String, "FlowDirection": pl.String, "Timestamp": pl.Datetime,
    }
    existing = set(df.collect_schema().names())
    missing = [pl.lit(None).cast(_null_types[c]).alias(c) for c in FACT_COLS if c not in existing]
    if missing:
        df = df.with_columns(*missing)
    return df.select(FACT_COLS)


class EnbridgeGoldMunger(BaseGoldMunger):
    """Silver → gold transformation for Enbridge pipeline data."""

    def __init__(self, paths):
        self._paths = paths

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    async def merge(self, silver_dirs: dict[RowType, Path], target_day: datetime | None = None) -> pl.DataFrame:
        """Transform each silver dataset to gold fact schema, concat, save."""
        gold_df = await asyncio.to_thread(self._build_gold, silver_dirs)
        await self._save_gold_output(gold_df, silver_dirs, target_day)
        return gold_df

    def _build_gold(self, silver_dirs: dict[RowType, Path]) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []

        oa_dir = silver_dirs.get(RowType.OA)
        if oa_dir and oa_dir.exists() and any(oa_dir.glob("*.parquet")):
            try:
                frames.append(self._gold_oa(pl.read_parquet(oa_dir / "*.parquet")))
            except Exception as e:
                logger.error(f"Gold OA transform failed: {e}")

        nn_dir = silver_dirs.get(RowType.NN)
        if nn_dir and nn_dir.exists() and any(nn_dir.glob("*.parquet")):
            try:
                frames.append(self._gold_nn(pl.read_parquet(nn_dir / "*.parquet")))
            except Exception as e:
                logger.error(f"Gold NN transform failed: {e}")

        sg_dir = silver_dirs.get(RowType.SG)
        if sg_dir and sg_dir.exists() and any(sg_dir.glob("*.parquet")):
            try:
                loc_path = self._paths.config_files / "LocConfigs.parquet"
                locs_df = pl.read_parquet(loc_path)
                frames.append(self._gold_sg(pl.read_parquet(sg_dir / "*.parquet"), locs_df))
            except Exception as e:
                logger.error(f"Gold SG transform failed: {e}")

        if not frames:
            raise ValueError("No silver data to merge into gold")

        return (
            pl.concat(frames, how="diagonal")
            .unique()
            .with_columns(pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"))
        )

    def _gold_oa(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.lazy()
            .with_columns(
                pl.col("Eff_Gas_Day").str.to_date(format=cfg.OA_DATE_FORMAT).alias("GasDay"),
                pl.col("Flow_Ind_Desc").str.strip_chars().replace_strict(cfg.OA_FLOW_MAP, default="Z").alias("FlowDirection"),
                pl.col("Loc_Name").str.strip_chars().alias("LocName"),
                pl.lit("OA").alias("Dataset"),
                pl.col("Total_Design_Capacity").str.replace_all(",", "").str.to_decimal(scale=2).cast(pl.Float64).alias("DesignCapacity"),
                pl.col("Operating_Capacity").str.replace_all(",", "").str.to_decimal(scale=2).cast(pl.Float64).alias("OperatingCapacity"),
                pl.col("Total_Scheduled_Quantity").str.replace_all(",", "").str.to_decimal(scale=2).cast(pl.Float64).alias("TotalScheduledQuantity"),
                pl.col("Operationally_Available_Capacity").str.replace_all(",", "").str.to_decimal(scale=2).cast(pl.Float64).alias("OperationallyAvailableCapacity"),
            )
            .with_columns(
                pl.concat_str([
                    pl.col("GasDay").dt.year().cast(pl.String),
                    pl.col("GasDay").dt.month().cast(pl.String).str.zfill(2),
                ]).str.to_integer().alias("GasMonth"),
            )
            .collect()
            .pipe(_fill_fact_schema)
        )

    def _gold_nn(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.lazy()
            .with_columns(
                pl.col("Effective_From_DateTime").str.to_datetime(format=cfg.NN_DATE_FORMAT).dt.date().alias("GasDay"),
                pl.col("Loc_Name").str.strip_chars().alias("LocName"),
                pl.col("Allocated_Qty").str.replace_all(",", "").str.to_decimal(scale=2).cast(pl.Float64).alias("TotalScheduledQuantity"),
                pl.col("Direction_Of_Flow").str.strip_chars().replace("B", "N").alias("FlowDirection"),
                pl.lit("NN").alias("Dataset"),
            )
            .with_columns(
                pl.concat_str([
                    pl.col("GasDay").dt.year().cast(pl.String),
                    pl.col("GasDay").dt.month().cast(pl.String).str.zfill(2),
                ]).cast(pl.Int64).alias("GasMonth"),
            )
            .collect()
            .pipe(_fill_fact_schema)
        )

    def _gold_sg(self, df: pl.DataFrame, locs_df: pl.DataFrame) -> pl.DataFrame:
        _sel = ["GFPipeID", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"]

        lz = (
            df.lazy()
            .filter(
                ~(
                    pl.col("Station Name").is_null()
                    & pl.col("Cap").is_null()
                    & pl.col("Nom").is_null()
                    & pl.col("Cap2").is_null()
                )
            )
            .with_columns(
                pl.col("Cap").cast(pl.Float64),
                pl.col("Cap2").cast(pl.Float64),
                pl.col("Nom").cast(pl.Float64),
                pl.col("CycleDesc").cast(pl.String),
            )
        )

        df_td2 = lz.filter(~pl.col("Cap2").is_null())
        lazy_list = [
            # No Cap2 — single direction
            lz.filter(pl.col("Cap2").is_null()).with_columns(
                pl.col("Cap").alias("OpCap"),
                pl.col("Nom").map_batches(
                    lambda s: pl.Series(map(lambda v: "TD2" if v < 0 else "TD1", s)),
                    return_dtype=pl.String,
                ).alias("FlowInd"),
            ).select(_sel),
            # Cap1 + Nom<0 → Nom=0, TD1
            df_td2.filter(pl.col("Nom") < 0).with_columns(
                pl.col("Cap").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD1").alias("FlowInd")
            ).select(_sel),
            # Cap2 + Nom<0 → Nom unchanged, TD2
            df_td2.filter(pl.col("Nom") < 0).with_columns(
                pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd")
            ).select(_sel),
            # Cap1 + Nom>0 → Nom unchanged, TD1
            df_td2.filter(pl.col("Nom") > 0).with_columns(
                pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd")
            ).select(_sel),
            # Cap2 + Nom>0 → Nom=0, TD2
            df_td2.filter(pl.col("Nom") > 0).with_columns(
                pl.col("Cap2").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD2").alias("FlowInd")
            ).select(_sel),
            # Cap1 + Nom==0, TD1
            df_td2.filter(pl.col("Nom") == 0).with_columns(
                pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd")
            ).select(_sel),
            # Cap2 + Nom==0, TD2
            df_td2.filter(pl.col("Nom") == 0).with_columns(
                pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd")
            ).select(_sel),
        ]

        sg_df = (
            pl.concat(lazy_list, how="vertical")
            .with_columns(
                pl.col("OpCap").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("Nom").map_batches(batch_absolute, return_dtype=pl.Float64),
            )
            .collect()
            .rename({"Station Name": "LocName"})
            .join(locs_df.select(["LocName", "GFPipeID", "GFLocID"]), on=["LocName", "GFPipeID"], how="inner")
            .lazy()
            .with_columns(
                pl.col("EffGasDate").cast(pl.Date).alias("GasDay"),
                pl.col("FlowInd").replace_strict(cfg.SG_FLOW_MAP, default="N").alias("FlowDirection"),
                pl.lit("SG").alias("Dataset"),
                pl.lit(None).cast(pl.Float64).alias("DesignCapacity"),
                pl.col("OpCap").alias("OperatingCapacity"),
                pl.col("Nom").alias("TotalScheduledQuantity"),
                (pl.col("OpCap") - pl.col("Nom")).alias("OperationallyAvailableCapacity"),
                pl.lit(None).cast(pl.String).alias("IT"),
            )
            .with_columns(
                pl.concat_str([
                    pl.col("GasDay").dt.year().cast(pl.String),
                    pl.col("GasDay").dt.month().cast(pl.String).str.zfill(2),
                ]).str.to_integer().alias("GasMonth"),
            )
            .collect()
        )
        return _fill_fact_schema(sg_df)

    async def _save_gold_output(
        self, gold_df: pl.DataFrame, silver_dirs: dict[RowType, Path],
        target_day: datetime | None = None,
    ) -> None:
        """Save gold parquet and silver copies to gold_dir for the day."""
        gold_dir = self._paths.gold_dir
        run_ts = datetime.now().strftime("%m%d%Y_%H%M%S")
        tgt_suffix = f"_tgt_{target_day.strftime('%m%d%Y')}" if target_day else ""

        def _save():
            gold_path = gold_dir / f"Gold_Enbridge_{run_ts}{tgt_suffix}.parquet"
            gold_df.write_parquet(gold_path)
            logger.info(f"Gold parquet saved: {gold_path.name} ({len(gold_df):,} rows)")

            ds_labels = {RowType.OA: "OA", RowType.SG: "SG", RowType.NN: "NN"}
            for row_type, silver_dir in silver_dirs.items():
                if not silver_dir or not silver_dir.exists():
                    continue
                label = ds_labels.get(row_type, str(row_type))
                for f in silver_dir.iterdir():
                    if f.is_file() and f.suffix == ".parquet":
                        dest = gold_dir / f"{label}_{f.name}"
                        shutil.copy2(f, dest)
                        logger.info(f"Silver copy saved: {dest.name}")

        await asyncio.to_thread(_save)

    def discover_locations(self, gold_df, segment_configs_df, pipe_configs_df):
        return None

    async def clean_directories(self) -> None:
        """Delete all raw and silver files after a successful push."""
        dirs_to_clean = [
            self._paths.oa_raw, self._paths.sg_raw, self._paths.nn_raw,
            self._paths.meta_raw,
            self._paths.oa_silver, self._paths.sg_silver, self._paths.nn_silver,
        ]

        def _do_clean():
            for d in dirs_to_clean:
                if not d.exists():
                    continue
                for f in d.iterdir():
                    if f.is_file():
                        try:
                            f.unlink()
                        except OSError as e:
                            logger.error(f"Cleanup failed: {f.name} — {e}")
            logger.info("Directory cleanup complete")

        await asyncio.to_thread(_do_clean)
