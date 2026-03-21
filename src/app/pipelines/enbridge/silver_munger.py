"""Enbridge silver munger — implements BaseSilverMunger.

Converts raw CSVs into a single normalized silver parquet per dataset type
conforming to the gold schema. Uses shared batch transforms from core.transforms
and Enbridge-specific column mappings from config.

Key LocID rules:
  OA / NN : Loc always present → LocID = zeroPadded7(Loc)
  SG      : no raw Loc → LocID generated as "GF" + 5-digit sequence per pipeline
  ST      : same as SG (generate GF-prefix) unless confirmed otherwise

GFLocID = str(GFPipeID).zfill(3) + LocID  (10-char surrogate key)

Gold schema columns:
  GasMonth, GFLocID, Dataset, GasDay, LocName,
  DesignCapacity, OperatingCapacity, TotalScheduledQuantity,
  OperationallyAvailableCapacity, IT, FlowDirection, Timestamp

Silver file naming: {DS}_Enbridge_{mmddyyyy}_{hhmmss}.parquet
"""

import asyncio
import functools
import operator
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from app.base.munger import BaseSilverMunger
from app.base.types import RowType, GOLD_SCHEMA
from app.core.logging import logger
from app.core.transforms import (
    batch_date_parse,
    batch_ymonth_parse,
    padded_string,
    gf_padded_loc,
    batch_float_parse,
    batch_absolute,
    batch_fi_mapper,
)
from app.core.azure_tables import dump_Loc_configs, update_Loc_configs
from app.core.settings import settings

from . import config as cfg


def _silver_filename(dataset_type: str, run_dt: datetime) -> str:
    """Generate silver parquet filename: {DS}_Enbridge_{mmddyyyy}_{hhmmss}.parquet"""
    return f"{dataset_type}_Enbridge_{run_dt.strftime('%m%d%Y')}_{run_dt.strftime('%H%M%S')}.parquet"


class EnbridgeSilverMunger(BaseSilverMunger):
    """Raw → silver transformation for Enbridge pipeline data.

    OA and NN are batch-processed: all CSVs concatenated into one silver parquet.
    SG is batch-processed with LocMetaData lookup for generated LocIDs.
    New locations are upserted to the LocMetaData Azure Table after each run.
    """

    def __init__(self, paths):
        self._paths = paths
        self.new_oa_locations: int = 0
        self.new_sg_locations: int = 0
        self.new_nn_locations: int = 0

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def process(
        self,
        raw_dir: Path,
        silver_dir: Path,
        dataset_type: RowType,
        pipe_configs_df: pd.DataFrame,
        **kwargs,
    ) -> list[str]:
        """Convert raw CSVs → single silver parquet per dataset type."""
        if dataset_type == RowType.OA:
            return await self._process_oa(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.SG:
            return await self._process_sg(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.ST:
            return await self._process_st(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.NN:
            return await self._process_nn(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.META:
            await self._process_meta(raw_dir, pipe_configs_df)
            return []
        else:
            logger.warning(f"Unknown dataset_type {dataset_type} — skipping")
            return []

    async def cleanse(self, file_path: Path, dataset_type: RowType, **kwargs) -> None:
        """No-op — batch processing is handled entirely within process()."""
        pass

    # ------------------------------------------------------------------
    # OA processing — batch concat, LocID from raw Loc
    # ------------------------------------------------------------------

    async def _process_oa(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("OA", run_dt)
        try:
            raw_df = await asyncio.to_thread(self._concat_point_csvs, raw_dir, "OA")
            if raw_df.empty:
                return []

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            # _munge_oa returns intermediate (includes GFPipeID/LocID/Loc for sync)
            intermediate = await asyncio.to_thread(
                self._munge_oa, pl.from_pandas(raw_df.astype(str)), enb_configs
            )
            await self._sync_point_locations(intermediate, RowType.OA)
            await asyncio.to_thread(intermediate.select(GOLD_SCHEMA).write_parquet, output_path)
        except Exception as e:
            logger.error(f"processOA failed: {e}")
        return []

    def _munge_oa(self, df: pl.DataFrame, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """OA transform: filter → parse → LocID/GFLocID.

        Returns intermediate DataFrame (superset of GOLD_SCHEMA) so that
        _sync_point_locations can read GFPipeID/LocID/Loc before final select.
        """
        null_check = functools.reduce(
            operator.and_, [pl.col(c).is_null() for c in cfg.OA_RAW_COLUMNS]
        )
        df = (
            df.select([*cfg.OA_RAW_COLUMNS, "PipeCode"])
            .filter(~null_check)
            .with_columns(
                pl.col("Eff_Gas_Day")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.OA_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("GasDay"),
                pl.col("Total_Design_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("DesignCapacity"),
                pl.col("Operating_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("OperatingCapacity"),
                pl.col("Total_Scheduled_Quantity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("TotalScheduledQuantity"),
                pl.col("Operationally_Available_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("OperationallyAvailableCapacity"),
                pl.col("Flow_Ind_Desc")
                .map_batches(
                    lambda s: batch_fi_mapper(s, cfg.OA_FLOW_MAP),
                    return_dtype=pl.String,
                )
                .alias("FlowDirection"),
                pl.col("IT").cast(pl.String),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                # Loc = raw; LocID = 7-digit zero-padded
                pl.col("Loc").cast(pl.String).alias("Loc"),
                pl.col("Loc")
                .cast(pl.String)
                .map_batches(lambda s: padded_string(s, 7), return_dtype=pl.String)
                .alias("LocID"),
            )
            .join(pl.from_pandas(enb_configs).lazy().collect(), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GFPipeID").cast(pl.Int64),
                pl.col("GasDay")
                .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
                .alias("GasMonth"),
                pl.lit("OA").alias("Dataset"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        # GFLocID = 3-digit PipeID + 7-digit LocID = 10-char surrogate key
        return df.with_columns(
            pl.concat_str(
                [pl.col("GFPipeID").cast(pl.String).str.zfill(3), pl.col("LocID")],
                separator="",
            ).alias("GFLocID")
        )

    # ------------------------------------------------------------------
    # ST (Storage Capacity) — same OA structure, may generate GF-prefix LocID
    # ------------------------------------------------------------------

    async def _process_st(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("ST", run_dt)
        try:
            raw_df = await asyncio.to_thread(self._concat_point_csvs, raw_dir, "ST")
            if raw_df.empty:
                return []

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            intermediate = await asyncio.to_thread(
                self._munge_st, pl.from_pandas(raw_df.astype(str)), enb_configs
            )
            await asyncio.to_thread(intermediate.select(GOLD_SCHEMA).write_parquet, output_path)
        except Exception as e:
            logger.error(f"processST failed: {e}")
        return []

    def _munge_st(self, df: pl.DataFrame, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """ST transform — same structure as OA but uses ST date/flow formats."""
        null_check = functools.reduce(
            operator.and_, [pl.col(c).is_null() for c in cfg.OA_RAW_COLUMNS]
        )
        df = (
            df.select([*cfg.OA_RAW_COLUMNS, "PipeCode"])
            .filter(~null_check)
            .with_columns(
                pl.col("Eff_Gas_Day")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.ST_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("GasDay"),
                pl.col("Total_Design_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("DesignCapacity"),
                pl.col("Operating_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("OperatingCapacity"),
                pl.col("Total_Scheduled_Quantity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("TotalScheduledQuantity"),
                pl.col("Operationally_Available_Capacity")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("OperationallyAvailableCapacity"),
                pl.col("Flow_Ind_Desc")
                .map_batches(
                    lambda s: batch_fi_mapper(s, cfg.ST_FLOW_MAP),
                    return_dtype=pl.String,
                )
                .alias("FlowDirection"),
                pl.col("IT").cast(pl.String),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                pl.col("Loc").cast(pl.String).alias("Loc"),
                pl.col("Loc")
                .cast(pl.String)
                .map_batches(lambda s: padded_string(s, 7), return_dtype=pl.String)
                .alias("LocID"),
            )
            .join(pl.from_pandas(enb_configs).lazy().collect(), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GFPipeID").cast(pl.Int64),
                pl.col("GasDay")
                .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
                .alias("GasMonth"),
                pl.lit("ST").alias("Dataset"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        return df.with_columns(
            pl.concat_str(
                [pl.col("GFPipeID").cast(pl.String).str.zfill(3), pl.col("LocID")],
                separator="",
            ).alias("GFLocID")
        )

    # ------------------------------------------------------------------
    # SG (Segment Capacity) — no raw Loc, generate GF-prefix LocIDs
    # ------------------------------------------------------------------

    async def _process_sg(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("SG", run_dt)

        # Ensure LocMetaData is cached locally
        await asyncio.to_thread(dump_Loc_configs, self._paths.config_files)

        try:
            raw_df = await asyncio.to_thread(self._concat_sg_csvs, raw_dir)
            if raw_df.empty:
                return []

            # Load LocMetaData, filter to Enbridge SG entries
            loc_path = self._paths.config_files / "LocConfigs.parquet"
            loc_df = await asyncio.to_thread(pd.read_parquet, loc_path)
            enb_loc_df = loc_df[
                (loc_df.get("PartitionKey", pd.Series(dtype=str)) == cfg.PARENT_PIPE)
                & (loc_df.get("RowType", pd.Series(dtype=str)) == "SG")
            ].copy() if "RowType" in loc_df.columns else loc_df[
                loc_df.get("PartitionKey", pd.Series(dtype=str)) == cfg.PARENT_PIPE
            ].copy()

            # Discover new station names → generate new GF-prefix LocIDs
            new_locs_df = await asyncio.to_thread(
                self._discover_new_sg_locs, raw_df, enb_loc_df, pipe_configs_df
            )
            if not new_locs_df.empty:
                await asyncio.to_thread(update_Loc_configs, new_locs_df)
                self.new_sg_locations = len(new_locs_df)
                enb_loc_df = pd.concat([enb_loc_df, new_locs_df], ignore_index=True)

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            intermediate = await asyncio.to_thread(
                self._munge_sg,
                pl.from_pandas(raw_df),
                pl.from_pandas(enb_loc_df),
                pl.from_pandas(enb_configs),
            )
            await asyncio.to_thread(intermediate.select(GOLD_SCHEMA).write_parquet, output_path)
        except Exception as e:
            logger.error(f"processSG failed: {e}")
        return []

    def _concat_sg_csvs(self, raw_dir: Path) -> pd.DataFrame:
        """Read all SG raw CSVs (skip header row), return combined DataFrame."""
        frames: list[pd.DataFrame] = []
        for fp in sorted(raw_dir.iterdir()):
            if fp.suffix != ".csv":
                continue
            try:
                pipe_code, _, eff_gas_day, cycle_desc = fp.name.split("_", 3)
                cycle_desc = cycle_desc[:-4]
                df = pd.read_csv(fp, skiprows=1, usecols=cfg.SG_RAW_COLUMNS)
                if not df.empty:
                    frames.append(
                        df.assign(
                            PipeCode=pipe_code,
                            EffGasDate=eff_gas_day,
                            CycleDesc=cycle_desc,
                        )
                    )
            except Exception as e:
                logger.error(f"SG CSV read failed: {fp.name} — {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _discover_new_sg_locs(
        self,
        raw_df: pd.DataFrame,
        loc_df: pd.DataFrame,
        pipe_configs_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Find SG station names not in LocMetaData, assign GF-prefix LocIDs."""
        records: list[dict] = []

        for pipe_code in raw_df["PipeCode"].unique():
            gf_pipe_id = pipe_configs_df.loc[
                (pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE)
                & (pipe_configs_df["PipeCode"] == pipe_code),
                "GFPipeID",
            ].squeeze()
            if pd.isna(gf_pipe_id):
                continue
            gf_pipe_id = int(gf_pipe_id)

            known = set(
                loc_df.loc[
                    loc_df.get("GFPipeID", pd.Series(dtype=float)).apply(
                        lambda x: int(x) if pd.notna(x) else -1
                    ) == gf_pipe_id,
                    "LocName",
                ]
                if "LocName" in loc_df.columns and "GFPipeID" in loc_df.columns
                else []
            )
            seen = set(raw_df.loc[raw_df["PipeCode"] == pipe_code, "Station Name"].dropna())
            new_stations = seen - known

            start_seq = len(known) + 1
            for seq, station_name in enumerate(sorted(new_stations), start=start_seq):
                loc_id = f"GF{seq:0>5}"
                gf_loc_id = f"{gf_pipe_id:0>3}{loc_id}"
                records.append({
                    "PartitionKey": cfg.PARENT_PIPE,
                    "RowKey": gf_loc_id,
                    "GFPipeID": gf_pipe_id,
                    "GFLocID": gf_loc_id,
                    "LocID": loc_id,
                    "Loc": None,
                    "LocName": station_name,
                    "PipeCode": pipe_code,
                    "RowType": "SG",
                    "UpdatedTime": datetime.now().strftime("%Y%m%d"),
                })

        return pd.DataFrame(records) if records else pd.DataFrame()

    def _munge_sg(
        self,
        raw: pl.DataFrame,
        loc_configs: pl.DataFrame,
        enb_configs: pl.DataFrame,
    ) -> pl.DataFrame:
        """SG transform: TD1/TD2 split → join LocMetaData for LocID → gold schema."""
        lz = (
            raw.lazy()
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

        lazy_list: list[pl.LazyFrame] = []
        df_td2 = lz.filter(~pl.col("Cap2").is_null())

        # Single-direction rows (no Cap2)
        lazy_list.append(
            lz.filter(pl.col("Cap2").is_null())
            .with_columns(
                pl.col("Cap").alias("OpCap"),
                pl.col("Nom")
                .map_batches(
                    lambda s: pl.Series(map(lambda v: "TD2" if v < 0 else "TD1", s)),
                    return_dtype=pl.String,
                )
                .alias("FlowInd"),
            )
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap1 + Nom<0 → Nom=0, TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") < 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD1").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap2 + Nom<0 → Nom unchanged, TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") < 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap1 + Nom>0 → Nom unchanged, TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") > 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap2 + Nom>0 → Nom=0, TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") > 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD2").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap1 + Nom==0 → TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") == 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )
        # Cap2 + Nom==0 → TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") == 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd"))
            .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
        )

        df = (
            pl.concat(lazy_list, how="vertical")
            .with_columns(
                pl.col("OpCap").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("Nom").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("EffGasDate")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.SG_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("GasDay"),
                pl.col("FlowInd").map_batches(
                    lambda s: batch_fi_mapper(s, cfg.SG_FLOW_MAP),
                    return_dtype=pl.String,
                )
                .alias("FlowDirection"),
            )
            # Join pipe configs for GFPipeID + PipeName
            .join(enb_configs.lazy(), on="PipeCode", how="inner")
            # Join LocMetaData by (GFPipeID, Station Name) → get LocID, GFLocID
            .join(
                loc_configs.lazy()
                .with_columns(pl.col("GFPipeID").cast(pl.Int64))
                .rename({"LocName": "Station Name"})
                .select(["GFPipeID", "Station Name", "LocID", "GFLocID"]),
                on=["GFPipeID", "Station Name"],
                how="inner",
            )
            .with_columns(
                pl.col("GFPipeID").cast(pl.Int64),
                pl.col("GasDay")
                .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
                .alias("GasMonth"),
                pl.lit("SG").alias("Dataset"),
                pl.col("Station Name").alias("LocName"),
                pl.lit(None).cast(pl.Float64).alias("DesignCapacity"),
                pl.col("OpCap").alias("OperatingCapacity"),
                pl.col("Nom").alias("TotalScheduledQuantity"),
                (pl.col("OpCap") - pl.col("Nom")).alias("OperationallyAvailableCapacity"),
                pl.lit(None).cast(pl.String).alias("IT"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        return df.collect()

    # ------------------------------------------------------------------
    # NN processing — batch concat, LocID from raw Loc
    # ------------------------------------------------------------------

    async def _process_nn(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("NN", run_dt)
        try:
            raw_df = await asyncio.to_thread(self._concat_point_csvs, raw_dir, "NN")
            if raw_df.empty:
                return []

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            intermediate = await asyncio.to_thread(
                self._munge_nn, pl.from_pandas(raw_df.astype(str)), enb_configs
            )
            await self._sync_point_locations(intermediate, RowType.NN)
            await asyncio.to_thread(intermediate.select(GOLD_SCHEMA).write_parquet, output_path)
        except Exception as e:
            logger.error(f"processNN failed: {e}")
        return []

    def _munge_nn(self, df: pl.DataFrame, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """NN transform: filter → parse → LocID/GFLocID.

        Returns intermediate DataFrame (superset of GOLD_SCHEMA) so that
        _sync_point_locations can read GFPipeID/LocID/Loc before final select.
        """
        null_check = functools.reduce(
            operator.and_, [pl.col(c).is_null() for c in cfg.NN_RAW_COLUMNS]
        )
        df = (
            df.select([*cfg.NN_RAW_COLUMNS, "PipeCode"])
            .filter(~null_check)
            .with_columns(
                pl.col("Effective_From_DateTime")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.NN_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("GasDay"),
                # Loc = raw; LocID = 7-digit zero-padded
                pl.col("Loc_Prop").cast(pl.String).alias("Loc"),
                pl.col("Loc_Prop")
                .cast(pl.String)
                .map_batches(lambda s: padded_string(s, 7), return_dtype=pl.String)
                .alias("LocID"),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                pl.col("Allocated_Qty")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("TotalScheduledQuantity"),
                pl.col("Direction_Of_Flow")
                .map_batches(
                    lambda s: batch_fi_mapper(s, cfg.NN_FLOW_MAP),
                    return_dtype=pl.String,
                )
                .alias("FlowDirection"),
            )
            .join(pl.from_pandas(enb_configs).lazy().collect(), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GFPipeID").cast(pl.Int64),
                pl.col("GasDay")
                .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
                .alias("GasMonth"),
                pl.lit("NN").alias("Dataset"),
                pl.lit(None).cast(pl.Float64).alias("DesignCapacity"),
                pl.lit(None).cast(pl.Float64).alias("OperatingCapacity"),
                pl.lit(None).cast(pl.Float64).alias("OperationallyAvailableCapacity"),
                pl.lit(None).cast(pl.String).alias("IT"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        return df.with_columns(
            pl.concat_str(
                [pl.col("GFPipeID").cast(pl.String).str.zfill(3), pl.col("LocID")],
                separator="",
            ).alias("GFLocID")
        )

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _concat_point_csvs(self, raw_dir: Path, label: str) -> pd.DataFrame:
        """Read all CSVs in raw_dir, tag with PipeCode, return combined DataFrame."""
        frames: list[pd.DataFrame] = []
        for fp in sorted(raw_dir.iterdir()):
            if fp.suffix != ".csv":
                continue
            pipe_code = fp.name.split("_", 1)[0]
            try:
                df = pd.read_csv(fp, header=0)
                if not df.empty:
                    frames.append(df.assign(PipeCode=pipe_code))
            except Exception as e:
                logger.error(f"{label} CSV read failed: {fp.name} — {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    async def _sync_point_locations(
        self, silver_df: pl.DataFrame, row_type: RowType
    ) -> None:
        """Upsert any new GFLocIDs to the LocMetaData Azure Table."""
        try:
            loc_df = await asyncio.to_thread(dump_Loc_configs, self._paths.config_files)
            if loc_df is None or loc_df.empty:
                existing_ids: set[str] = set()
            else:
                existing_ids = set(loc_df.get("GFLocID", pd.Series(dtype=str)).dropna())

            new_locs = (
                silver_df.select(["GFPipeID", "GFLocID", "LocID", "Loc", "LocName"])
                .unique(subset=["GFLocID"])
                .filter(~pl.col("GFLocID").is_in(list(existing_ids)))
            )

            if new_locs.is_empty():
                return

            count = len(new_locs)
            if row_type == RowType.OA:
                self.new_oa_locations = count
            elif row_type == RowType.NN:
                self.new_nn_locations = count

            records = new_locs.to_pandas()
            records["PartitionKey"] = cfg.PARENT_PIPE
            records["RowKey"] = records["GFLocID"]
            records["RowType"] = row_type.value
            records["UpdatedTime"] = datetime.now().strftime("%Y%m%d")

            await asyncio.to_thread(update_Loc_configs, records)
            logger.info(f"{row_type} new locations upserted to LocMetaData: {count}")

        except Exception as e:
            logger.error(f"syncPointLocations failed ({row_type}): {e}")

    # ------------------------------------------------------------------
    # META processing
    # ------------------------------------------------------------------

    async def _process_meta(self, raw_dir: Path, pipe_configs_df: pd.DataFrame) -> None:
        """Read metadata CSVs, find new rows, upsert to Azure Table."""
        from app.core.azure_tables import get_table

        def _read_csvs():
            df_list = []
            for file_path in raw_dir.iterdir():
                pipe_code = file_path.name.split("_", 2)[0]
                temp_df = pd.read_csv(file_path, dtype=str).assign(PipeCode=pipe_code)
                df_list.append(temp_df)
            return df_list

        df_list = await asyncio.to_thread(_read_csvs)

        if not df_list:
            return

        cur_df = (
            pl.from_pandas(pd.concat(df_list))
            .unique()
            .rename(lambda col_name: re.sub(r"[^a-zA-Z0-9]", "", col_name))
            .fill_null("")
        )
        cur_df = cur_df.with_columns(
            *[pl.col(c).str.strip_chars() for c in cur_df.columns],
            pl.lit(cfg.PARENT_PIPE).alias("PartitionKey"),
        )

        def _query_table():
            with get_table(settings.enbridge_metadata_table) as table_client:
                return pl.DataFrame(
                    table_client.query_entities(
                        f"PartitionKey eq '{cfg.PARENT_PIPE}'"
                    )
                ).select(pl.exclude(["RowKey", "ChangedOn"]))

        meta_df = await asyncio.to_thread(_query_table)

        new_rows = cur_df.join(meta_df, how="anti", on=cur_df.columns).with_columns(
            pl.concat_str(
                [
                    pl.lit(datetime.now().strftime("%Y%m%d")),
                    pl.col("PartitionKey"),
                    pl.col("Loc"),
                ]
            ).alias("RowKey"),
            pl.lit(datetime.now().strftime("%Y%m%d")).alias("ChangedOn"),
        )

        if not new_rows.is_empty():
            operations = [("upsert", row) for row in new_rows.iter_rows(named=True)]

            def _upsert():
                with get_table(settings.enbridge_metadata_table) as table_client:
                    for i in range(0, len(operations), 90):
                        try:
                            table_client.submit_transaction(
                                operations[i : min(i + 90, len(operations))]
                            )
                        except Exception as e:
                            logger.error(f"EnbridgeMetadata upsert failed: {e}")

            await asyncio.to_thread(_upsert)
