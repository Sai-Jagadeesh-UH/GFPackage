"""Enbridge silver munger — implements BaseSilverMunger.

Converts raw CSVs downloaded by EnbridgeScraper into normalized silver
parquets conforming to the gold schema. Uses shared batch transforms
from core.transforms and Enbridge-specific column mappings from config.

OA: CSV → parquet with PipeCode → cleanse (date parse, float parse, rename)
OC: CSV → parquet with PipeCode/EffGasDate/CycleDesc → cleanse (TD1/TD2 split)
NN: CSV → parquet with PipeCode → cleanse (date parse, flow map)
META: CSV processing and Azure Table upsert
"""

import asyncio
import re
from datetime import datetime
from functools import partial
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
    batch_float_parse,
    batch_absolute,
    batch_fi_mapper,
    add_modeling_columns,
    compose_gfloc,
    filter_all_null,
)
from app.core.azure_tables import dump_segment_configs, update_segment_configs
from app.core.settings import settings

from . import config as cfg


class EnbridgeSilverMunger(BaseSilverMunger):
    """Raw → silver transformation for Enbridge pipeline data."""

    def __init__(self, paths):
        """
        Args:
            paths: PipelinePaths instance.
        """
        self._paths = paths
        self.new_oc_locations: int = 0

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
        """Convert raw files → silver parquets.

        Dispatches to OA/OC/NN-specific processing logic.
        """
        if dataset_type == RowType.OA:
            return await self._process_oa(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type in (RowType.OC, RowType.SG):
            return await self._process_oc(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.NN:
            return await self._process_nn(raw_dir, silver_dir, pipe_configs_df)
        elif dataset_type == RowType.META:
            await self._process_meta(raw_dir, pipe_configs_df)
            return []
        else:
            logger.warning(f"Unknown dataset_type {dataset_type} — skipping")
            return []

    async def cleanse(self, file_path: Path, dataset_type: RowType, **kwargs) -> None:
        """Cleanse a single silver parquet in-place."""
        pipe_configs_df = kwargs.get("pipe_configs_df")
        segment_configs_df = kwargs.get("segment_configs_df")

        if dataset_type == RowType.OA:
            await self._cleanse_oa(file_path, pipe_configs_df)
        elif dataset_type in (RowType.OC, RowType.SG):
            await self._cleanse_oc(file_path, segment_configs_df, pipe_configs_df)
        elif dataset_type == RowType.NN:
            await self._cleanse_nn(file_path, pipe_configs_df)

    # ------------------------------------------------------------------
    # OA processing
    # ------------------------------------------------------------------

    async def _process_oa(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        empty_list: list[str] = []
        try:
            # Offload sync CSV reads + parquet writes to thread pool
            def _convert_csvs():
                empties = []
                for file_path in raw_dir.iterdir():
                    try:
                        pipe_code = file_path.name.split("_", 2)[0]
                        df = pd.read_csv(file_path, header=0)
                        if len(df) < 1:
                            empties.append(file_path.name.replace(".csv", ".parquet"))
                        df.assign(PipeCode=pipe_code).to_parquet(
                            silver_dir / file_path.name.replace(".csv", ".parquet"),
                            index=False,
                        )
                    except Exception as e:
                        logger.error(f"OA CSV read failed: {file_path.name} — {e}")
                return empties

            empty_list = await asyncio.to_thread(_convert_csvs)

            async with asyncio.TaskGroup() as group:
                for fp in silver_dir.iterdir():
                    if fp.name not in empty_list:
                        group.create_task(
                            self._cleanse_oa(fp, pipe_configs_df)
                        )
        except Exception as e:
            logger.error(f"processOA failed: {e}")

        return empty_list

    async def _cleanse_oa(self, file_path: Path, pipe_configs_df: pd.DataFrame) -> None:
        try:
            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]

            df = (
                pl.scan_parquet(file_path)
                .select([*cfg.OA_RAW_COLUMNS, "PipeCode"])
            )
            df = filter_all_null(df, cfg.OA_RAW_COLUMNS)

            df = df.with_columns(
                pl.col("Eff_Gas_Day")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.OA_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("EffGasDay"),
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
                .alias("FlowInd"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
                pl.col("Loc")
                .cast(pl.String)
                .map_batches(padded_string, return_dtype=pl.String),
                pl.lit(cfg.PARENT_PIPE).cast(pl.String).alias("ParentPipe"),
                pl.lit(None).cast(pl.String).alias("QtyReason"),
                pl.lit(None).cast(pl.String).alias("LocSegment"),
            ).rename(cfg.OA_RENAME_MAP)

            result = (
                df.join(
                    pl.LazyFrame(enb_configs),
                    on="PipeCode",
                    how="inner",
                )
            )
            result = add_modeling_columns(result, "OA", cfg.PARENT_PIPE)
            result = result.with_columns(
                pl.col("LocZn").cast(pl.String),
                pl.col("PipeName").cast(pl.String).alias("PipelineName"),
            )
            result = compose_gfloc(result)
            final = result.select(GOLD_SCHEMA)
            await asyncio.to_thread(lambda: final.collect().write_parquet(file_path))

        except Exception as e:
            logger.error(f"cleanseOA failed: {file_path.name} — {e}")

    # ------------------------------------------------------------------
    # OC processing
    # ------------------------------------------------------------------

    async def _process_oc(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        await asyncio.to_thread(dump_segment_configs, self._paths.config_files)

        try:
            empty_list = await asyncio.to_thread(self._oc_csv_to_parquet, raw_dir, silver_dir)

            configs_df_full = await asyncio.to_thread(
                pd.read_parquet, self._paths.config_files / "SegmentConfigs.parquet"
            )
            try:
                configs_df = configs_df_full[
                    configs_df_full["ParentPipe"] == cfg.PARENT_PIPE
                ]
            except Exception:
                configs_df = pd.DataFrame(
                    columns=["ParentPipe", "PipeCode", "GFPipeID", "StationName"]
                )

            oc_df = await asyncio.to_thread(
                lambda: pd.read_parquet(silver_dir, columns=["PipeCode", "Station Name"])
                .rename(columns={"Station Name": "StationName"})
                .drop_duplicates(keep="first")
            )

            records_list = self._discover_new_oc_segments(
                oc_df, configs_df, pipe_configs_df
            )
            self.new_oc_locations = len(records_list)

            if records_list:
                new_df = pd.DataFrame(records_list)
                new_df["PartitionKey"] = cfg.PARENT_PIPE
                new_df["RowKey"] = (
                    new_df["ParentPipe"] + new_df["Loc"] + new_df["PipeCode"]
                )

                await asyncio.to_thread(update_segment_configs, new_df)
                merged_configs = pd.concat([configs_df_full, new_df], ignore_index=True)
                await asyncio.to_thread(
                    merged_configs.to_parquet,
                    self._paths.config_files / "SegmentConfigs.parquet",
                    index=False,
                )

                enb_configs_df = (
                    pd.concat([configs_df, new_df], ignore_index=True)
                    .rename(columns={"StationName": "Station Name"})[
                        ["PipeCode", "Loc", "Station Name"]
                    ]
                )
            else:
                enb_configs_df = configs_df.rename(
                    columns={"StationName": "Station Name"}
                )[["PipeCode", "Loc", "Station Name"]]

            async with asyncio.TaskGroup() as group:
                for fp in silver_dir.iterdir():
                    if fp.name not in empty_list:
                        group.create_task(
                            self._cleanse_oc(fp, enb_configs_df, pipe_configs_df)
                        )
        except Exception as e:
            logger.error(f"processOC failed: {e}")

        return empty_list if "empty_list" in dir() else []

    def _oc_csv_to_parquet(self, raw_dir: Path, silver_dir: Path) -> list[str]:
        """Read raw OC CSVs, convert to parquet with metadata columns."""
        empty_list: list[str] = []

        for file_path in raw_dir.iterdir():
            try:
                pipe_code, _, eff_gas_day_time, cycle_desc = file_path.name.split(
                    "_", 3
                )
                cycle_desc = cycle_desc[:-4]

                try:
                    df = pd.read_csv(
                        file_path,
                        skiprows=1,
                        usecols=cfg.OC_RAW_COLUMNS,
                    )
                    if len(df) < 1:
                        empty_list.append(file_path.name.replace(".csv", ".parquet"))
                except Exception as e:
                    logger.error(f"OC CSV read failed: {file_path.name} — {e}")
                    continue

                (
                    df.assign(
                        PipeCode=pipe_code,
                        EffGasDate=eff_gas_day_time,
                        CycleDesc=cycle_desc,
                    )[
                        [
                            "PipeCode", "EffGasDate", "CycleDesc",
                            "Station Name", "Cap", "Nom", "Cap2",
                        ]
                    ].to_parquet(
                        silver_dir / file_path.name.replace(".csv", ".parquet"),
                        index=False,
                    )
                )
            except Exception as e:
                logger.error(f"OC file processing failed: {file_path.name} — {e}")

        return empty_list

    def _discover_new_oc_segments(
        self,
        oc_df: pd.DataFrame,
        configs_df: pd.DataFrame,
        pipe_configs_df: pd.DataFrame,
    ) -> list[dict]:
        """Find new OC station names not yet in SegmentConfigs."""
        records_list: list[dict] = []

        for pipe_code in oc_df["PipeCode"].unique():
            gf_pipe_id = pipe_configs_df.loc[
                (pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE)
                & (pipe_configs_df["PipeCode"] == pipe_code),
                "GFPipeID",
            ].item()

            config_set = set(
                configs_df[configs_df["PipeCode"] == pipe_code]["StationName"]
            )
            oc_set = set(oc_df[oc_df["PipeCode"] == pipe_code]["StationName"])

            records_list.extend(
                [
                    {
                        "Loc": f"{key:0>6}",
                        "ParentPipe": cfg.PARENT_PIPE,
                        "GFPipeID": gf_pipe_id,
                        "PipeCode": pipe_code,
                        "StationName": station_name,
                    }
                    for key, station_name in enumerate(
                        oc_set - config_set, start=len(config_set) + 1
                    )
                ]
            )

        return records_list

    async def _cleanse_oc(
        self,
        file_path: Path,
        segment_configs: pd.DataFrame,
        pipe_configs_df: pd.DataFrame,
    ) -> None:
        try:
            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]

            lz = (
                pl.scan_parquet(file_path)
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

            # Records with no Cap2 — single direction
            df_td1 = (
                lz.filter(pl.col("Cap2").is_null())
                .with_columns(
                    pl.col("Cap").alias("OpCap"),
                    pl.col("Nom")
                    .map_batches(
                        lambda s: pl.Series(
                            map(lambda v: "TD2" if v < 0 else "TD1", s)
                        ),
                        return_dtype=pl.String,
                    )
                    .alias("FlowInd"),
                )
                .select(
                    ["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"]
                )
            )
            lazy_list.append(df_td1)

            # Rows with both Cap & Cap2
            df_td2 = lz.filter(~pl.col("Cap2").is_null())

            # Cap1 + Nom<0 → Nom=0, TD1
            lazy_list.append(
                df_td2.filter(pl.col("Nom") < 0)
                .with_columns(
                    pl.col("Cap").alias("OpCap"),
                    pl.lit(float(0)).alias("Nom"),
                    pl.lit("TD1").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )
            # Cap2 + Nom<0 → Nom unchanged, TD2
            lazy_list.append(
                df_td2.filter(pl.col("Nom") < 0)
                .with_columns(
                    pl.col("Cap2").alias("OpCap"),
                    pl.lit("TD2").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )
            # Cap1 + Nom>0 → Nom unchanged, TD1
            lazy_list.append(
                df_td2.filter(pl.col("Nom") > 0)
                .with_columns(
                    pl.col("Cap").alias("OpCap"),
                    pl.lit("TD1").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )
            # Cap2 + Nom>0 → Nom=0, TD2
            lazy_list.append(
                df_td2.filter(pl.col("Nom") > 0)
                .with_columns(
                    pl.col("Cap2").alias("OpCap"),
                    pl.lit(float(0)).alias("Nom"),
                    pl.lit("TD2").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )
            # Cap1 + Nom==0 → TD1
            lazy_list.append(
                df_td2.filter(pl.col("Nom") == 0)
                .with_columns(
                    pl.col("Cap").alias("OpCap"),
                    pl.lit("TD1").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )
            # Cap2 + Nom==0 → TD2
            lazy_list.append(
                df_td2.filter(pl.col("Nom") == 0)
                .with_columns(
                    pl.col("Cap2").alias("OpCap"),
                    pl.lit("TD2").alias("FlowInd"),
                )
                .select(["PipeCode", "Station Name", "OpCap", "FlowInd", "Nom", "EffGasDate", "CycleDesc"])
            )

            df = pl.concat(lazy_list, how="vertical").with_columns(
                pl.col("OpCap").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("Nom").map_batches(batch_absolute, return_dtype=pl.Float64),
                pl.col("EffGasDate")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.OC_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("EffGasDay"),
                pl.col("FlowInd")
                .map_batches(
                    lambda s: batch_fi_mapper(s, cfg.OC_FLOW_MAP),
                    return_dtype=pl.String,
                ),
                pl.lit(None).cast(pl.String).alias("LocZn"),
            )

            result = (
                df.join(pl.LazyFrame(enb_configs), on="PipeCode", how="inner")
                .join(
                    pl.LazyFrame(segment_configs),
                    on=["PipeCode", "Station Name"],
                    how="inner",
                )
            )
            result = add_modeling_columns(result, "STA", cfg.PARENT_PIPE)
            result = result.with_columns(
                (pl.col("OpCap") - pl.col("Nom")).alias(
                    "OperationallyAvailableCapacity"
                ),
            )
            result = result.with_columns(
                pl.concat_str(
                    [
                        pl.col("GFPipeID"),
                        pl.col("RowType"),
                        pl.col("Loc"),
                        pl.col("FlowInd"),
                    ],
                    separator="",
                ).alias("GFLOC"),
                pl.lit(cfg.PARENT_PIPE).cast(pl.String).alias("ParentPipe"),
                pl.col("PipeName").alias("PipelineName"),
                pl.lit(None).cast(pl.String).alias("LocPurpDesc"),
                pl.col("Station Name").alias("LocName"),
                pl.lit(None).cast(pl.String).alias("LocZn"),
                pl.lit(None).cast(pl.String).alias("LocSegment"),
                pl.lit(None).alias("DesignCapacity"),
                pl.col("OpCap").alias("OperatingCapacity"),
                pl.col("Nom").alias("TotalScheduledQuantity"),
                pl.lit(None).cast(pl.String).alias("IT"),
                pl.lit(None).cast(pl.String).alias("AllQtyAvail"),
                pl.lit(None).cast(pl.String).alias("QtyReason"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
            )

            final = result.select(GOLD_SCHEMA)
            await asyncio.to_thread(lambda: final.collect().write_parquet(file_path))

        except Exception as e:
            logger.error(f"cleanseOC failed: {file_path.name} — {e}")

    # ------------------------------------------------------------------
    # NN processing
    # ------------------------------------------------------------------

    async def _process_nn(
        self, raw_dir: Path, silver_dir: Path, pipe_configs_df: pd.DataFrame
    ) -> list[str]:
        empty_list: list[str] = []
        try:
            def _convert_csvs():
                empties = []
                for file_path in raw_dir.iterdir():
                    try:
                        pipe_code, _, _ = file_path.name.split("_")
                        df = pd.read_csv(file_path, header=0)
                        if len(df) < 1:
                            empties.append(file_path.name.replace(".csv", ".parquet"))
                        df.assign(PipeCode=pipe_code).to_parquet(
                            silver_dir / file_path.name.replace(".csv", ".parquet"),
                            index=False,
                        )
                    except Exception as e:
                        logger.error(f"NN CSV read failed: {file_path.name} — {e}")
                return empties

            empty_list = await asyncio.to_thread(_convert_csvs)

            async with asyncio.TaskGroup() as group:
                for fp in silver_dir.iterdir():
                    if fp.name not in empty_list:
                        group.create_task(self._cleanse_nn(fp, pipe_configs_df))

        except Exception as e:
            logger.error(f"processNN failed: {e}")

        return empty_list

    async def _cleanse_nn(self, file_path: Path, pipe_configs_df: pd.DataFrame) -> None:
        try:
            enb_configs = pipe_configs_df.loc[
                pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE,
                ["GFPipeID", "PipeCode", "PipeName"],
            ]

            df = (
                pl.scan_parquet(file_path)
                .select([*cfg.NN_RAW_COLUMNS, "PipeCode"])
            )
            df = filter_all_null(df, cfg.NN_RAW_COLUMNS)

            df = df.with_columns(
                pl.col("Effective_From_DateTime")
                .map_batches(
                    lambda s: batch_date_parse(s, cfg.NN_DATE_FORMAT),
                    return_dtype=pl.Date,
                )
                .alias("EffGasDay"),
                pl.col("Loc_Prop")
                .map_batches(padded_string, return_dtype=pl.String)
                .alias("Loc"),
                pl.col("Loc_Name").cast(pl.String).alias("LocName"),
                pl.lit(cfg.PARENT_PIPE).cast(pl.String).alias("ParentPipe"),
                pl.col("Allocated_Qty")
                .map_batches(batch_float_parse, return_dtype=pl.Float64)
                .alias("TotalScheduledQuantity"),
                pl.col("Direction_Of_Flow")
                .map_batches(
                    lambda s: batch_fi_mapper(s, cfg.NN_FLOW_MAP),
                    return_dtype=pl.String,
                )
                .alias("FlowInd"),
                pl.lit(None).cast(pl.String).alias("CycleDesc"),
                pl.lit(None).cast(pl.String).alias("LocPurpDesc"),
                pl.lit(None).cast(pl.String).alias("LocZn"),
                pl.lit(None).cast(pl.String).alias("LocSegment"),
                pl.lit(None).alias("DesignCapacity"),
                pl.lit(None).alias("OperatingCapacity"),
                pl.lit(None).alias("OperationallyAvailableCapacity"),
                pl.lit(None).cast(pl.String).alias("IT"),
                pl.lit(None).cast(pl.String).alias("AllQtyAvail"),
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),
                pl.col("Accounting_Physincal_Indicator")
                .cast(pl.String)
                .alias("QtyReason"),
            )

            result = df.join(
                pl.LazyFrame(enb_configs), on="PipeCode", how="inner"
            )
            result = add_modeling_columns(result, "NN", cfg.PARENT_PIPE)
            result = result.with_columns(
                pl.col("Loc").map_batches(padded_string, return_dtype=pl.String),
                pl.col("PipeName").alias("PipelineName"),
            )
            result = compose_gfloc(result)
            final = result.select(GOLD_SCHEMA)
            await asyncio.to_thread(lambda: final.collect().write_parquet(file_path))

        except Exception as e:
            logger.error(f"cleanseNN failed: {file_path.name} — {e}")

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
