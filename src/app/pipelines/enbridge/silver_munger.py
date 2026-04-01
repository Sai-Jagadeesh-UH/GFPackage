"""Enbridge silver munger — implements BaseSilverMunger.

Silver = raw concat + GFLocID added + unique(). No gold schema transforms.
Gold transforms (date parsing, flow mapping, fact schema) live in gold_munger.

Silver file naming: {DS}_Enbridge_{mmddyyyy}_{hhmmss}.parquet
"""

import asyncio
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from app.base.munger import BaseSilverMunger
from app.base.types import RowType
from app.core.logging import logger
from app.core.azure_tables import dump_Loc_configs, update_Loc_configs, dump_pipe_configs
from app.core.settings import settings

from . import config as cfg


# Columns for the LocMetaData Azure table
META_COLS = [
    "GFPipeID", "PipeName", "GFLocID", "GF_LocID_Tag",
    "LocID", "Loc", "GF_LocName", "LocName",
    "GF_FacilityType", "GF_FacilityTypeGroup", "ReportedFacilityType",
    "LocSegment", "LocZone", "State", "County",
    "InterconnectingEntity", "UpdatedTime",
]

# AllPoints meta CSV rename map
_META_RAW_RENAME = {
    "Loc St Abbrev": "State",
    "Loc Cnty": "County",
    "Loc Zone": "LocZone",
    "Loc Type Ind": "ReportedFacilityType",
}
_META_LOC_COLUMNS = ["Loc", "Loc Name", "Loc St Abbrev", "Loc Cnty", "Loc Zone", "Dir Flo", "Loc Type Ind", "PipeCode"]


def _silver_filename(dataset_type: str, run_dt: datetime, target_day: datetime | None = None) -> str:
    """Generate silver parquet filename: {DS}_Enbridge_{mmddyyyy}_{hhmmss}[_tgt_{mmddyyyy}].parquet"""
    base = f"{dataset_type}_Enbridge_{run_dt.strftime('%m%d%Y')}_{run_dt.strftime('%H%M%S')}"
    if target_day is not None:
        base += f"_tgt_{target_day.strftime('%m%d%Y')}"
    return base + ".parquet"


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
        target_day: datetime | None = None,
        **kwargs,
    ) -> list[str]:
        """Convert raw CSVs → single silver parquet per dataset type.

        Silver = raw concat + GFLocID added + unique(). No gold schema transforms here.
        """
        if dataset_type == RowType.OA:
            return await self._process_oa(raw_dir, silver_dir, pipe_configs_df, target_day)
        elif dataset_type == RowType.SG:
            return await self._process_sg(raw_dir, silver_dir, pipe_configs_df, target_day)
        elif dataset_type == RowType.NN:
            return await self._process_nn(raw_dir, silver_dir, pipe_configs_df, target_day)
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
    # OA silver — concat raw, add GFLocID, update new locs
    # ------------------------------------------------------------------

    async def _process_oa(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame,
        target_day: datetime | None = None,
    ) -> list[str]:
        """Silver OA = raw concat + GFLocID added. Gold transforms happen in gold_munger."""
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("OA", run_dt, target_day)
        try:
            raw_df = await asyncio.to_thread(self._concat_point_csvs, raw_dir, "OA")
            if raw_df.empty:
                return []

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            silver_df = await asyncio.to_thread(
                self._build_oa_silver, pl.from_pandas(raw_df), enb_configs
            )
            await self._sync_point_locations(silver_df, RowType.OA)
            await asyncio.to_thread(silver_df.write_parquet, output_path)
        except Exception as e:
            logger.error(f"processOA failed: {e}")
        return []

    def _build_oa_silver(self, df: pl.DataFrame, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """Join PipeConfigs, add GFLocID. Raw columns preserved — no gold transforms."""
        return (
            df.join(pl.from_pandas(enb_configs[["PipeCode", "GFPipeID", "PipeName"]]), on="PipeCode", how="inner")
            .with_columns(
                pl.col("Loc").str.strip_chars(),
            )
            .with_columns(
                pl.concat_str([
                    pl.col("GFPipeID"),
                    pl.col("Loc").str.zfill(7),
                ]).alias("GFLocID")
            )
            .select(pl.exclude(["PipeCode", "GFPipeID"]))
            .unique()
        )

    # ------------------------------------------------------------------
    # SG silver — concat raw + GFPipeID joined, update new locs
    # ------------------------------------------------------------------

    async def _process_sg(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame,
        target_day: datetime | None = None,
    ) -> list[str]:
        """Silver SG = raw concat + GFPipeID joined + unique. Gold transforms in gold_munger."""
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("SG", run_dt, target_day)

        await asyncio.to_thread(dump_Loc_configs, self._paths.config_files)

        try:
            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            silver_df = await asyncio.to_thread(self._concat_sg_csvs, raw_dir, enb_configs)
            if silver_df.is_empty():
                return []

            loc_path = self._paths.config_files / "LocConfigs.parquet"
            locs_pl = await asyncio.to_thread(pl.read_parquet, loc_path)
            await self._updated_sg_locs(silver_df, locs_pl, enb_configs)

            await asyncio.to_thread(silver_df.write_parquet, output_path)
        except Exception as e:
            logger.error(f"processSG failed: {e}")
        return []

    def _concat_sg_csvs(self, raw_dir: Path, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """Read all SG raw CSVs — parse gas date (as date) and cycle from first line.

        Returns Polars DataFrame with GFPipeID joined and PipeCode dropped.
        """
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
                gas_date = datetime.strptime(gdate_part.split("Gas Date: ")[1].strip(), "%Y-%m-%d").date()
                cycle = cycle_part.strip()
                df = pd.read_csv(fp, header=1, usecols=cfg.SG_RAW_COLUMNS)
                if not df.empty:
                    frames.append(df.assign(EffGasDate=gas_date, CycleDesc=cycle, PipeCode=pipe_code))
            except Exception as e:
                logger.error(f"SG CSV read failed: {fp.name} — {e}")
        if not frames:
            return pl.DataFrame()
        return (
            pl.from_pandas(pd.concat(frames, ignore_index=True))
            .join(pl.from_pandas(enb_configs[["PipeCode", "GFPipeID", "PipeName"]]), on="PipeCode", how="inner")
            .select(pl.exclude(["PipeCode", "PipeName"]))
            .unique()
        )

    async def _updated_sg_locs(
        self,
        raw_pl: pl.DataFrame,
        locs_df: pl.DataFrame,
        enb_configs: pd.DataFrame,
    ) -> None:
        """Find new SG stations, assign GF-prefix LocIDs, upsert to Azure LocConfigs."""
        # Anti-join on (GFPipeID, Station Name) vs existing LocConfigs LocName
        new_df = (
            raw_pl.rename({"Station Name": "LocName"})
            .join(locs_df.select(["GFPipeID", "LocName"]), on=["GFPipeID", "LocName"], how="anti")
            .select(["GFPipeID", "LocName"])
            .unique()
        )
        if new_df.is_empty():
            return

        df_list: list[pl.DataFrame] = []
        for (pipe_id,), data in new_df.group_by("GFPipeID"):
            existing_len = (
                locs_df.filter(
                    pl.col("GFPipeID").eq(pipe_id)
                    & (pl.col("Loc").is_null() | (pl.col("Loc").cast(pl.String) == ""))
                ).height
                + 1
            )
            temp = data.sort("LocName").with_columns(
                pl.concat_str([
                    pl.lit("GF"),
                    pl.int_range(
                        start=existing_len,
                        end=pl.len() + existing_len,
                        step=1,
                    ).cast(pl.String).str.zfill(5),
                ]).alias("LocID"),
            ).with_columns(
                pl.concat_str([pl.col("GFPipeID"), pl.col("LocID")]).alias("GFLocID"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("UpdatedTime"),
            )
            df_list.append(temp)

        sg_locs = (
            pl.concat(df_list, how="vertical")
            .join(
                pl.from_pandas(enb_configs[["GFPipeID", "PipeName"]]),
                on="GFPipeID",
                how="left",
            )
        )
        schema_names = sg_locs.collect_schema().names()
        sg_locs = sg_locs.with_columns(
            *[pl.lit("").alias(c) for c in META_COLS if c not in schema_names],
        ).select(META_COLS)

        self.new_sg_locations = len(sg_locs)
        to_upsert = sg_locs.with_columns(
            pl.lit(cfg.PARENT_PIPE).alias("PartitionKey"),
            pl.col("GFLocID").alias("RowKey"),
        )
        await asyncio.to_thread(update_Loc_configs, to_upsert)
        await asyncio.to_thread(dump_Loc_configs, self._paths.config_files, True)
        logger.info(f"SG {self.new_sg_locations} new locations pushed to LocConfigs")

    # ------------------------------------------------------------------
    # NN silver — concat raw, add GFLocID, update new locs
    # ------------------------------------------------------------------

    async def _process_nn(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame,
        target_day: datetime | None = None,
    ) -> list[str]:
        """Silver NN = raw concat + GFLocID added. Gold transforms happen in gold_munger."""
        run_dt = datetime.now()
        output_path = silver_dir / _silver_filename("NN", run_dt, target_day)
        try:
            raw_df = await asyncio.to_thread(self._concat_point_csvs, raw_dir, "NN")
            if raw_df.empty:
                return []

            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]
            silver_df = await asyncio.to_thread(
                self._build_nn_silver, pl.from_pandas(raw_df), enb_configs
            )
            await self._sync_point_locations(silver_df, RowType.NN)
            await asyncio.to_thread(silver_df.write_parquet, output_path)
        except Exception as e:
            logger.error(f"processNN failed: {e}")
        return []

    def _build_nn_silver(self, df: pl.DataFrame, enb_configs: pd.DataFrame) -> pl.DataFrame:
        """Join PipeConfigs, add GFLocID. Raw columns preserved — no gold transforms."""
        return (
            df.join(pl.from_pandas(enb_configs[["PipeCode", "GFPipeID", "PipeName"]]), on="PipeCode", how="inner")
            .with_columns(
                pl.col("Loc_Prop").str.strip_chars(),
            )
            .with_columns(
                pl.concat_str([
                    pl.col("GFPipeID"),
                    pl.col("Loc_Prop").str.zfill(7),
                ]).alias("GFLocID")
            )
            .select(pl.exclude(["PipeCode", "GFPipeID"]))
            .unique()
        )

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _concat_point_csvs(self, raw_dir: Path, label: str) -> pd.DataFrame:
        """Read all CSVs in raw_dir as string dtype, tag with PipeCode, return combined DataFrame."""
        frames: list[pd.DataFrame] = []
        for fp in sorted(raw_dir.iterdir()):
            if fp.suffix != ".csv":
                continue
            pipe_code = fp.name.split("_", 1)[0]
            try:
                df = pd.read_csv(fp, dtype=str)
                if not df.empty:
                    frames.append(df.assign(PipeCode=pipe_code))
            except Exception as e:
                logger.error(f"{label} CSV read failed: {fp.name} — {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    async def _sync_point_locations(
        self, silver_df: pl.DataFrame, row_type: RowType
    ) -> None:
        """Find new GFLocIDs in OA/NN silver data and upsert to LocConfigs Azure Table.

        Silver schema has raw cols + GFLocID. Derive GFPipeID, LocID, Loc from GFLocID/raw cols.
        OA raw: Loc (raw loc number), Loc_Name (loc name)
        NN raw: Loc_Prop (raw loc number), Loc_Name (loc name)
        """
        try:
            locs_pl = await asyncio.to_thread(pl.read_parquet, self._paths.config_files / "LocConfigs.parquet")

            # Normalise to common fields: GFLocID, GFPipeID, LocID, Loc, LocName, PipeName
            loc_col = "Loc" if "Loc" in silver_df.columns else "Loc_Prop"
            name_col = "Loc_Name" if "Loc_Name" in silver_df.columns else "LocName"
            locs_df = (
                silver_df.select([loc_col, name_col, "GFLocID", "PipeName"])
                .unique(subset=["GFLocID"])
                .with_columns(
                    pl.col(loc_col).str.strip_chars().alias("Loc"),
                    pl.col(name_col).str.strip_chars().alias("LocName"),
                    pl.col(loc_col).str.strip_chars().str.zfill(7).alias("LocID"),
                    pl.col("GFLocID").str.slice(0, 3).alias("GFPipeID"),
                    pl.lit(datetime.now()).cast(pl.Datetime).alias("UpdatedTime"),
                )
            )

            # Anti-join to find new GFLocIDs
            new_locs = locs_df.join(
                locs_pl.select("GFLocID"), on="GFLocID", how="anti"
            )
            if new_locs.is_empty():
                return

            count = len(new_locs)
            if row_type == RowType.OA:
                self.new_oa_locations = count
            elif row_type == RowType.NN:
                self.new_nn_locations = count

            # Enrich from AllPoints meta where available
            meta_pl = await asyncio.to_thread(self._build_meta_df)
            if meta_pl is not None and not meta_pl.is_empty():
                new_locs = new_locs.join(meta_pl.select(
                    ["GFLocID", "LocZone", "State", "County", "ReportedFacilityType"]
                ), on="GFLocID", how="left")

            # Fill any missing META_COLS columns
            schema_names = new_locs.collect_schema().names()
            new_locs = new_locs.with_columns(
                *[pl.lit("").alias(c) for c in META_COLS if c not in schema_names],
            ).select(META_COLS)

            # Push to LocConfigs Azure Table
            to_upsert = new_locs.with_columns(
                pl.lit(cfg.PARENT_PIPE).alias("PartitionKey"),
                pl.col("GFLocID").alias("RowKey"),
            )
            await asyncio.to_thread(update_Loc_configs, to_upsert)
            await asyncio.to_thread(dump_Loc_configs, self._paths.config_files, True) # froceful update
            logger.info(f"{row_type} {count} new locations pushed to LocConfigs")

        except Exception as e:
            logger.error(f"syncPointLocations failed ({row_type}): {e}")

    def _build_meta_df(self) -> pl.DataFrame | None:
        """Build enrichment DataFrame from AllPoints meta CSVs.

        Returns Polars DataFrame with GFLocID built from GFPipeID + Loc.zfill(7),
        with raw column names renamed per _META_RAW_RENAME.
        """
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

        pipe_configs = dump_pipe_configs(self._paths.config_files)
        if pipe_configs is None:
            return None

        df = pl.from_pandas(pd.concat(frames, ignore_index=True))
        df = df.with_columns(*[pl.col(c).str.strip_chars() for c in df.columns]).fill_null("")

        avail = [c for c in _META_LOC_COLUMNS if c in df.collect_schema().names()]
        df = (
            df.select([*avail])
            .join(
                pl.from_pandas(pipe_configs[["PipeCode", "GFPipeID", "PipeName"]]),
                on="PipeCode",
                how="inner",
            )
            .unique()
            .with_columns(
                pl.concat_str([
                    pl.col("GFPipeID"),
                    pl.col("Loc").str.strip_chars().str.zfill(7),
                ]).alias("GFLocID")
            )
            .rename(_META_RAW_RENAME)
        )
        return df

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
