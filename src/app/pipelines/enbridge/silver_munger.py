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
from app.core.azure_tables import dump_Loc_configs, update_Loc_configs, dump_pipe_configs
from app.core.settings import settings

from . import config as cfg


# Columns for the LocMetaData Azure table — nothing else goes in
META_COLS = [
    "GF_PipeID", "PipeName", "GFLocID", "GF_LocID_Tag",
    "LocID", "Loc", "GF_LocName", "LocName",
    "GF_FacilityType", "GF_FacilityTypeGroup", "ReportedFacilityType",
    "LocSegment", "LocZone", "State", "County",
    "InterconnectingEntity", "UpdatedTime",
]

# Raw AllPoints CSV columns → MetaCols schema
_META_RAW_RENAME = {
    "Loc Zone": "LocZone",
    "Loc Cnty": "County",
    "Loc St Abbrev": "State",
    "Loc Type Ind": "ReportedFacilityType",
}


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
                pl.col("Eff_Gas_Day").map_batches(batch_date_parse(cfg.OA_DATE_FORMAT), return_dtype=pl.Date).alias("GasDay"),
                
                pl.col("Total_Design_Capacity").map_batches(batch_float_parse, return_dtype=pl.Float64).alias("DesignCapacity"),
                
                pl.col("Operating_Capacity").map_batches(batch_float_parse, return_dtype=pl.Float64).alias("OperatingCapacity"),
                
                pl.col("Total_Scheduled_Quantity").map_batches(batch_float_parse, return_dtype=pl.Float64).alias("TotalScheduledQuantity"),
                
                pl.col("Operationally_Available_Capacity").map_batches(batch_float_parse, return_dtype=pl.Float64).alias("OperationallyAvailableCapacity"),
                
                pl.col("Flow_Ind_Desc").map_batches(batch_fi_mapper(cfg.OA_FLOW_MAP), return_dtype=pl.String).alias("FlowDirection"),
                               pl.col("IT").cast(pl.String),
                
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                
                # Loc = raw; LocID = 7-digit zero-padded
                pl.col("Loc").cast(pl.String).alias("Loc"),
                
                pl.col("Loc").cast(pl.String).map_batches(padded_string(7), return_dtype=pl.String).alias("LocID"),
            )
            .join(pl.from_pandas(enb_configs), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GasDay").map_batches(batch_ymonth_parse, return_dtype=pl.Int64).alias("GasMonth"),
                
                pl.lit("OA").alias("Dataset"),
                
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        # GFLocID = 3-digit PipeID + 7-digit LocID = 10-char surrogate key
        return df.with_columns(
            pl.concat_str([pl.col("GFPipeID"), pl.col("LocID")], separator="").alias("GFLocID")
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
                .map_batches(batch_date_parse(cfg.ST_DATE_FORMAT), return_dtype=pl.Date)
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
                .map_batches(batch_fi_mapper(cfg.ST_FLOW_MAP), return_dtype=pl.String)
                .alias("FlowDirection"),
                pl.col("IT").cast(pl.String),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                pl.col("Loc").cast(pl.String).alias("Loc"),
                pl.col("Loc")
                .cast(pl.String)
                .map_batches(padded_string(7), return_dtype=pl.String)
                .alias("LocID"),
            )
            .join(pl.from_pandas(enb_configs).lazy().collect(), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GasDay")
                .map_batches(batch_ymonth_parse, return_dtype=pl.Int64)
                .alias("GasMonth"),
                pl.lit("ST").alias("Dataset"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
        )
        return df.with_columns(
            pl.concat_str([pl.col("GFPipeID"), pl.col("LocID")], separator="").alias("GFLocID")
        )

    # ------------------------------------------------------------------
    # SG (Segment Capacity) — no raw Loc, GF-prefix LocIDs assigned per pipe
    # ------------------------------------------------------------------

    async def _process_sg(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("SG", run_dt)

        # Ensure LocMetaData is cached locally
        await asyncio.to_thread(dump_Loc_configs, self._paths.config_files)

        try:
            # Step 1: Read raw CSVs — gas date + cycle from first line
            raw_df = await asyncio.to_thread(self._concat_sg_csvs, raw_dir)
            if raw_df.empty:
                return []

            # Step 2: TD split + parse + join PipeConfigs
            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            munged = await asyncio.to_thread(
                self._munge_sg, pl.from_pandas(raw_df), pl.from_pandas(enb_configs)
            )

            # Step 3: Load SG LocMetaData — entries with empty Loc (GF-prefix LocIDs)
            loc_path = self._paths.config_files / "LocConfigs.parquet"
            loc_df = await asyncio.to_thread(pd.read_parquet, loc_path)
            if "Loc" in loc_df.columns and "PartitionKey" in loc_df.columns:
                sg_loc_df = loc_df[
                    (loc_df["PartitionKey"] == cfg.PARENT_PIPE)
                    & (loc_df["Loc"].isna() | (loc_df["Loc"].astype(str).str.strip() == ""))
                ].copy()
            else:
                sg_loc_df = pd.DataFrame()

            # Step 4: Assign LocID/GFLocID — existing from LocMetaData, new generated alphabetically
            munged, new_locs_df = await asyncio.to_thread(
                self._assign_sg_loc_ids, munged, sg_loc_df
            )

            # Step 5: Push new locations to LocConfigs Azure Table
            if not new_locs_df.empty:
                self.new_sg_locations = len(new_locs_df)
                to_upsert = new_locs_df.reindex(columns=META_COLS).copy()
                to_upsert["PartitionKey"] = cfg.PARENT_PIPE
                to_upsert["RowKey"] = to_upsert["GFLocID"]
                await asyncio.to_thread(update_Loc_configs, to_upsert)
                await asyncio.to_thread(dump_Loc_configs, self._paths.config_files, True)
                logger.info(f"SG {len(new_locs_df)} new locations pushed to LocConfigs")

            await asyncio.to_thread(munged.select(GOLD_SCHEMA).write_parquet, output_path)
        except Exception as e:
            logger.error(f"processSG failed: {e}")
        return []

    def _concat_sg_csvs(self, raw_dir: Path) -> pd.DataFrame:
        """Read all SG raw CSVs — parse gas date and cycle from first line."""
        frames: list[pd.DataFrame] = []
        for fp in sorted(raw_dir.iterdir()):
            if fp.suffix != ".csv":
                continue
            pipe_code = fp.name.split("_", 1)[0].strip()
            try:
                first_line = fp.open().readline().strip()
                if not first_line:
                    continue
                gdate_part, cycle_part = first_line.split("  Cycle: ")
                gas_date = datetime.strptime(gdate_part.split("Gas Date: ")[1].strip(), "%Y-%m-%d")
                cycle = cycle_part.strip()
                df = pd.read_csv(fp, header=1, usecols=cfg.SG_RAW_COLUMNS)
                if not df.empty:
                    frames.append(df.assign(
                        PipeCode=pipe_code,
                        EffGasDate=gas_date.strftime("%Y%m%d"),
                        CycleDesc=cycle,
                    ))
            except Exception as e:
                logger.error(f"SG CSV read failed: {fp.name} — {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _munge_sg(self, raw: pl.DataFrame, enb_configs: pl.DataFrame) -> pl.DataFrame:
        """SG transform: TD1/TD2 split → parse → join PipeConfigs → gold schema (no LocID yet)."""
        _sel = ["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"]

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
            .select(_sel)
        )
        # Cap1 + Nom<0 → Nom=0, TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") < 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD1").alias("FlowInd"))
            .select(_sel)
        )
        # Cap2 + Nom<0 → Nom unchanged, TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") < 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd"))
            .select(_sel)
        )
        # Cap1 + Nom>0 → Nom unchanged, TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") > 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd"))
            .select(_sel)
        )
        # Cap2 + Nom>0 → Nom=0, TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") > 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit(float(0)).alias("Nom"), pl.lit("TD2").alias("FlowInd"))
            .select(_sel)
        )
        # Cap1 + Nom==0 → TD1
        lazy_list.append(
            df_td2.filter(pl.col("Nom") == 0)
            .with_columns(pl.col("Cap").alias("OpCap"), pl.lit("TD1").alias("FlowInd"))
            .select(_sel)
        )
        # Cap2 + Nom==0 → TD2
        lazy_list.append(
            df_td2.filter(pl.col("Nom") == 0)
            .with_columns(pl.col("Cap2").alias("OpCap"), pl.lit("TD2").alias("FlowInd"))
            .select(_sel)
        )

        return (
            pl.concat(lazy_list, how="vertical")
            .with_columns(
                pl.col("OpCap").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("Nom").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("EffGasDate")
                .map_batches(batch_date_parse(cfg.SG_DATE_FORMAT), return_dtype=pl.Date)
                .alias("GasDay"),
                pl.col("FlowInd")
                .map_batches(batch_fi_mapper(cfg.SG_FLOW_MAP), return_dtype=pl.String)
                .alias("FlowDirection"),
            )
            .join(enb_configs.lazy(), on="PipeCode", how="inner")
            .with_columns(
                pl.col("GasDay").map_batches(batch_ymonth_parse, return_dtype=pl.Int64).alias("GasMonth"),
                pl.lit("SG").alias("Dataset"),
                pl.col("Station Name").alias("LocName"),
                pl.lit(None).cast(pl.Float64).alias("DesignCapacity"),
                pl.col("OpCap").alias("OperatingCapacity"),
                pl.col("Nom").alias("TotalScheduledQuantity"),
                (pl.col("OpCap") - pl.col("Nom")).alias("OperationallyAvailableCapacity"),
                pl.lit(None).cast(pl.String).alias("IT"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )
            .collect()
        )

    def _assign_sg_loc_ids(
        self,
        munged: pl.DataFrame,
        sg_loc_df: pd.DataFrame,
    ) -> tuple[pl.DataFrame, pd.DataFrame]:
        """Assign LocID/GFLocID to each (GFPipeID, Station Name) in the munged SG df.

        Existing locs → looked up from LocMetaData by (GFPipeID, LocName).
        New locs → GF + 5-digit sequence, assigned alphabetically per pipe,
                   continuing from the highest existing sequence for that pipe.
        Returns (munged df with LocID/GFLocID added, new_locs_df with META_COLS).
        """
        required = ("GFPipeID", "LocName", "LocID", "GFLocID")
        existing_locs: dict[tuple, tuple] = {}  # (GFPipeID, LocName) → (LocID, GFLocID)
        existing_seqs: dict[str, int] = {}       # GFPipeID → max GF sequence used

        if not sg_loc_df.empty and all(c in sg_loc_df.columns for c in required):
            for _, row in sg_loc_df.iterrows():
                gfpipe = str(row["GFPipeID"])
                locname = row.get("LocName")
                locid = str(row.get("LocID") or "")
                gflocid = str(row.get("GFLocID") or "")
                if locname and locid:
                    existing_locs[(gfpipe, locname)] = (locid, gflocid)
                if locid.startswith("GF"):
                    try:
                        seq = int(locid[2:])
                        existing_seqs[gfpipe] = max(existing_seqs.get(gfpipe, 0), seq)
                    except ValueError:
                        pass

        # Extract unique (GFPipeID, Station Name, PipeName) from munged
        pairs = munged.select(["GFPipeID", "Station Name", "PipeName"]).unique().to_pandas()

        new_records: list[dict] = []
        assignment_rows: list[dict] = []

        # Carry existing assignments into the join table
        for (gfpipe, station), (locid, gflocid) in existing_locs.items():
            assignment_rows.append({"GFPipeID": gfpipe, "Station Name": station,
                                    "LocID": locid, "GFLocID": gflocid})

        # Collect new stations per pipe (sorted alphabetically)
        new_by_pipe: dict[str, list[tuple]] = {}
        for _, row in pairs.iterrows():
            gfpipe = str(row["GFPipeID"])
            station = row["Station Name"]
            pipe_name = row["PipeName"]
            if (gfpipe, station) not in existing_locs:
                new_by_pipe.setdefault(gfpipe, []).append((station, pipe_name))

        for gfpipe in sorted(new_by_pipe):
            start_seq = existing_seqs.get(gfpipe, 0) + 1
            for seq, (station, pipe_name) in enumerate(
                sorted(new_by_pipe[gfpipe], key=lambda x: x[0]), start=start_seq
            ):
                loc_id = f"GF{seq:0>5}"
                gf_loc_id = f"{gfpipe}{loc_id}"
                assignment_rows.append({"GFPipeID": gfpipe, "Station Name": station,
                                        "LocID": loc_id, "GFLocID": gf_loc_id})
                new_records.append({
                    "GF_PipeID": gfpipe,
                    "PipeName": pipe_name,
                    "GFLocID": gf_loc_id,
                    "GF_LocID_Tag": "",
                    "LocID": loc_id,
                    "Loc": "",
                    "GF_LocName": "",
                    "LocName": station,
                    "GF_FacilityType": "",
                    "GF_FacilityTypeGroup": "",
                    "ReportedFacilityType": "",
                    "LocSegment": "",
                    "LocZone": "",
                    "State": "",
                    "County": "",
                    "InterconnectingEntity": "",
                    "UpdatedTime": datetime.now(),
                })

        # Join assignment back to munged on (GFPipeID, Station Name)
        assignment_pl = pl.DataFrame(assignment_rows, schema={
            "GFPipeID": pl.String, "Station Name": pl.String,
            "LocID": pl.String, "GFLocID": pl.String,
        }) if assignment_rows else pl.DataFrame(schema={
            "GFPipeID": pl.String, "Station Name": pl.String,
            "LocID": pl.String, "GFLocID": pl.String,
        })
        munged = munged.join(assignment_pl, on=["GFPipeID", "Station Name"], how="left")

        new_locs_df = pd.DataFrame(new_records) if new_records else pd.DataFrame()
        return munged, new_locs_df

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
                .map_batches(batch_date_parse(cfg.NN_DATE_FORMAT), return_dtype=pl.Date)
                .alias("GasDay"),
                # Loc = raw; LocID = 7-digit zero-padded
                pl.col("Loc_Prop").cast(pl.String).alias("Loc"),
                pl.col("Loc_Prop")
                .cast(pl.String)
                .map_batches(padded_string(7), return_dtype=pl.String)
                .alias("LocID"),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                pl.col("Allocated_Qty")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("TotalScheduledQuantity"),
                pl.col("Direction_Of_Flow")
                .map_batches(batch_fi_mapper(cfg.NN_FLOW_MAP), return_dtype=pl.String)
                .alias("FlowDirection"),
            )
            .join(pl.from_pandas(enb_configs).lazy().collect(), on="PipeCode", how="inner")
            .with_columns(
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
            pl.concat_str([pl.col("GFPipeID"), pl.col("LocID")], separator="").alias("GFLocID")
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
        """Find new GFLocIDs and save them to a CSV for review.

        Does NOT write to Azure — files are saved to config_files/pending_locs_*.csv
        so they can be reviewed and manually uploaded once verified.
        """
        try:
            loc_df = await asyncio.to_thread(dump_Loc_configs, self._paths.config_files)
            existing_ids: set[str] = (
                set(loc_df.get("GFLocID", pd.Series(dtype=str)).dropna())
                if loc_df is not None and not loc_df.empty
                else set()
            )

            new_locs = (
                silver_df.select(["GFPipeID", "GFLocID", "LocID", "Loc", "LocName"])
                .unique(subset=["GFLocID"])
                .filter(~pl.col("GFLocID").is_in(existing_ids))
            )

            if new_locs.is_empty():
                return

            count = len(new_locs)
            if row_type == RowType.OA:
                self.new_oa_locations = count
            elif row_type == RowType.NN:
                self.new_nn_locations = count

            # Base record — rename GFPipeID → GF_PipeID
            records = new_locs.to_pandas().rename(columns={"GFPipeID": "GF_PipeID"})

            # PipeName from pipe_configs (GFPipeID is now 3-digit string key)
            pipe_configs = await asyncio.to_thread(dump_pipe_configs, self._paths.config_files)
            if pipe_configs is not None:
                name_map = pipe_configs.set_index("GFPipeID")["PipeName"].to_dict()
                records["PipeName"] = records["GF_PipeID"].map(name_map)
            else:
                records["PipeName"] = ""

            # GF-managed fields — left empty for manual curation
            for col in ["GF_LocID_Tag", "GF_LocName", "GF_FacilityType", "GF_FacilityTypeGroup"]:
                records[col] = ""

            # Enrichment columns — empty by default, filled from AllPoints meta if available
            enrich_cols = ["LocZone", "State", "County", "ReportedFacilityType",
                           "LocSegment", "InterconnectingEntity"]
            for col in enrich_cols:
                records[col] = ""

            meta_df = await asyncio.to_thread(self._build_meta_df, pipe_configs)
            if meta_df is not None and not meta_df.empty and "GFLocID" in meta_df.columns:
                available = [c for c in enrich_cols if c in meta_df.columns]
                if available:
                    merged = records.merge(
                        meta_df[["GFLocID"] + available],
                        on="GFLocID",
                        how="left",
                        suffixes=("", "_m"),
                    )
                    for col in available:
                        records[col] = merged[f"{col}_m"].fillna("").values

            records["UpdatedTime"] = datetime.now()

            # Push to LocConfigs Azure Table
            to_upsert = records.reindex(columns=META_COLS).copy()
            to_upsert["PartitionKey"] = cfg.PARENT_PIPE
            to_upsert["RowKey"] = to_upsert["GFLocID"]
            await asyncio.to_thread(update_Loc_configs, to_upsert)
            await asyncio.to_thread(dump_Loc_configs, self._paths.config_files, True)
            logger.info(f"{row_type} {count} new locations pushed to LocConfigs")

        except Exception as e:
            logger.error(f"syncPointLocations failed ({row_type}): {e}")

    def _build_meta_df(self, pipe_configs: pd.DataFrame | None) -> pd.DataFrame | None:
        """Build enrichment DataFrame from AllPoints meta CSVs.

        Reads all *_AllPoints.csv files from meta_raw, strips strings,
        joins with pipe_configs for GFPipeID, builds LocID/GFLocID,
        and renames raw columns to the MetaCols schema.
        """
        if pipe_configs is None:
            return None

        frames = []
        for fp in self._paths.meta_raw.iterdir():
            if fp.suffix != ".csv":
                continue
            try:
                pipe_code = fp.name.split("_", 1)[0]
                frames.append(pd.read_csv(fp, dtype=str).assign(PipeCode=pipe_code))
            except Exception as e:
                logger.error(f"MetaDf build failed: {fp.name} — {e}")

        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)
        # Strip whitespace from all string columns
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

        if "Loc" not in df.columns:
            return None

        df = df.merge(pipe_configs[["PipeCode", "GFPipeID"]], on="PipeCode", how="inner")
        df["LocID"] = df["Loc"].apply(
            lambda x: str(x).zfill(7) if pd.notna(x) and str(x).strip() else None
        )
        df["GFLocID"] = df["GFPipeID"] + df["LocID"].fillna("")

        return df.rename(columns=_META_RAW_RENAME)

    # ------------------------------------------------------------------
    # META processing
    # ------------------------------------------------------------------

    async def _process_meta(self, raw_dir: Path, pipe_configs_df: pd.DataFrame) -> None:
        """Read metadata CSVs, find new rows, upsert to Azure Table.- EnbridgeMetadata"""
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
                    table_client.query_entities("")
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
