"""Enbridge pusher — implements BasePusher.

Handles bronze, silver, and gold push logic for Enbridge data.
Uses shared upload helpers from core.cloud and Delta Lake push
from core.delta.
"""

import asyncio
from functools import partial
from pathlib import Path

import polars as pl

from app.base.pusher import BasePusher
from app.base.types import DatasetDetail, RowType, RunStats
from app.core.cloud import push_directory
from app.core.delta import LakeMerge
from app.core.logging import logger
from app.core.settings import settings

from . import config as cfg


class EnbridgePusher(BasePusher):
    """Medallion layer push for Enbridge pipeline data."""

    def __init__(self, paths):
        """
        Args:
            paths: PipelinePaths instance.
        """
        self._paths = paths

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    # ------------------------------------------------------------------
    # Blob path construction
    # ------------------------------------------------------------------

    def bronze_blob_path(self, dataset_type: RowType, file_path: Path) -> str:
        """Construct bronze blob path based on dataset type and filename."""
        name = file_path.name

        if dataset_type == RowType.OA:
            pipe_code, _, eff_date, _ = name.split("_", 3)
            return cfg.oa_bronze_blob_path(pipe_code, eff_date, name)

        elif dataset_type in (RowType.OC, RowType.SG):
            pipe_code, _, eff_date, _ = name.split("_", 3)
            return cfg.oc_bronze_blob_path(pipe_code, eff_date, name)

        elif dataset_type == RowType.NN:
            pipe_code, _, eff_date = name.split("_")
            return cfg.nn_bronze_blob_path(pipe_code, eff_date, name)

        elif dataset_type == RowType.META:
            return cfg.meta_bronze_blob_path(name)

        else:
            return f"Enbridge/other/{name}"

    def silver_blob_path(self, dataset_type: RowType, file_path: Path) -> str:
        """Construct silver blob path based on dataset type and filename."""
        name = file_path.name

        if dataset_type == RowType.OA:
            pipe_code, _, eff_date, _ = name.split("_", 3)
            return cfg.oa_silver_blob_path(pipe_code, eff_date, name)

        elif dataset_type in (RowType.OC, RowType.SG):
            pipe_code, _, eff_date, _ = name.split("_", 3)
            return cfg.oc_silver_blob_path(pipe_code, eff_date, name)

        elif dataset_type == RowType.NN:
            pipe_code, _, eff_date = name.split("_")
            return cfg.nn_silver_blob_path(pipe_code, eff_date, name)

        else:
            return f"Enbridge/other/{name}"

    # ------------------------------------------------------------------
    # Push operations
    # ------------------------------------------------------------------

    async def push_bronze(self, raw_dir: Path, dataset_type: RowType) -> None:
        """Upload raw files to the bronze blob container."""
        await push_directory(
            directory=raw_dir,
            container=settings.bronze_container,
            path_resolver=lambda fp: self.bronze_blob_path(dataset_type, fp),
        )

    async def push_silver(self, silver_dir: Path, dataset_type: RowType) -> None:
        """Upload normalized parquets to the silver blob container."""
        await push_directory(
            directory=silver_dir,
            container=settings.silver_container,
            path_resolver=lambda fp: self.silver_blob_path(dataset_type, fp),
        )

    async def push_gold(self, gold_df: pl.DataFrame) -> None: # type: ignore
        """Push merged gold DataFrame to Delta Lake (offloaded to thread)."""
        try:
            await asyncio.to_thread(
                LakeMerge, df=gold_df, parent_pipe=cfg.PARENT_PIPE
            )
        except Exception as e:
            logger.error(f"Gold Delta Lake push failed: {e}")

    # ------------------------------------------------------------------
    # Orchestrated push (bronze → silver → gold)
    # ------------------------------------------------------------------

    async def push_all(
        self, silver_munger, pipe_configs_df, stats: RunStats | None = None
    ) -> None:
        """Full push pipeline: raw meta → (OA + OC + NN in parallel) → gold → cleanup.

        This replicates the original pushEnbridge() orchestration:
        1. Push raw metadata to bronze
        2. In parallel: for each of OA, OC, NN:
           a. Push raw to bronze
           b. Process silver (cleanse)
           c. Push silver
        3. Merge gold and push to Delta Lake
        """
        from .gold_munger import EnbridgeGoldMunger

        # Push raw metadata
        await self.push_bronze(self._paths.meta_raw, RowType.META)

        # Process and push OA, OC, NN in parallel
        async with asyncio.TaskGroup() as group:
            oa_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.OA)
            )
            oc_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.OC)
            )
            nn_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.NN)
            )

        # Collect dataset details into stats
        if stats is not None:
            for task in (oa_task, oc_task, nn_task):
                for detail in task.result():
                    stats.add_dataset_detail(detail)

        # Gold merge and push
        gold_munger = EnbridgeGoldMunger(self._paths)
        gold_df = await gold_munger.merge({
            RowType.OA: self._paths.silver_dir(RowType.OA),
            RowType.OC: self._paths.silver_dir(RowType.OC),
            RowType.NN: self._paths.silver_dir(RowType.NN),
        })
        await self.push_gold(gold_df)

        # Cleanup
        await gold_munger.clean_directories()

    async def _push_dataset(
        self, silver_munger, pipe_configs_df, dataset_type: RowType
    ) -> list[DatasetDetail]:
        """Push bronze → process silver → push silver for one dataset type.

        Returns per-pipe DatasetDetail list with record counts and actual ADLS file paths.
        """
        raw_dir = self._paths.raw_dir(dataset_type)
        silver_dir = self._paths.silver_dir(dataset_type)

        blob_base = f"https://{settings.blob_account_name}.blob.core.windows.net"

        # Count raw rows per pipe and collect actual file blob paths
        raw_counts: dict[str, int] = {}
        raw_blob_files: dict[str, list[str]] = {}
        for f in raw_dir.iterdir():
            if f.is_file() and f.suffix == ".csv":
                try:
                    pipe_code = f.name.split("_", 1)[0]
                    nrows = sum(1 for _ in open(f)) - 1
                    raw_counts[pipe_code] = raw_counts.get(pipe_code, 0) + max(nrows, 0)
                    blob_path = self.bronze_blob_path(dataset_type, f)
                    raw_blob_files.setdefault(pipe_code, []).append(
                        f"{blob_base}/{settings.bronze_container}/{blob_path}"
                    )
                except Exception:
                    pass

        # Push raw to bronze
        await self.push_bronze(raw_dir, dataset_type)

        # Process silver
        await silver_munger.process(
            raw_dir, silver_dir, dataset_type, pipe_configs_df
        )

        # Count silver records per pipe and collect actual file blob paths
        silver_counts: dict[str, int] = {}
        silver_blob_files: dict[str, list[str]] = {}
        for f in silver_dir.iterdir():
            if f.is_file() and f.suffix == ".parquet":
                try:
                    pipe_code = f.name.split("_", 1)[0]
                    nrows = pl.scan_parquet(f).select(pl.len()).collect().item()
                    silver_counts[pipe_code] = (
                        silver_counts.get(pipe_code, 0) + nrows
                    )
                    blob_path = self.silver_blob_path(dataset_type, f)
                    silver_blob_files.setdefault(pipe_code, []).append(
                        f"{blob_base}/{settings.silver_container}/{blob_path}"
                    )
                except Exception:
                    pass

        # Push silver
        await self.push_silver(silver_dir, dataset_type)

        # Determine expected pipes from config
        enb_df = pipe_configs_df[pipe_configs_df["ParentPipe"] == cfg.PARENT_PIPE]
        code_col = {
            RowType.OA: "PointCapCode",
            RowType.OC: "SegmentCapCode",
            RowType.NN: "NoNoticeCode",
        }.get(dataset_type)
        expected_pipes: set[str] = set()
        if code_col and code_col in enb_df.columns:
            expected_pipes = set(
                enb_df.loc[enb_df[code_col].notna(), "PipeCode"].str.upper()
            )

        # Build details with actual ADLS file paths
        all_pipes = expected_pipes | set(raw_counts) | set(silver_counts)
        details: list[DatasetDetail] = []
        new_locs = silver_munger.new_oc_locations if dataset_type == RowType.OC else 0
        for pipe_code in sorted(all_pipes):
            is_missing = pipe_code in expected_pipes and pipe_code not in raw_counts
            details.append(DatasetDetail(
                dataset_type=dataset_type,
                pipe_code=pipe_code,
                raw_records=raw_counts.get(pipe_code, 0),
                silver_records=silver_counts.get(pipe_code, 0),
                new_locations=new_locs if dataset_type == RowType.OC else 0,
                raw_paths=raw_blob_files.get(pipe_code, []),
                silver_paths=silver_blob_files.get(pipe_code, []),
                missing=is_missing,
            ))

        return details
