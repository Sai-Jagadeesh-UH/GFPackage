"""Enbridge pusher — implements BasePusher.

Handles bronze, silver, and gold push logic for Enbridge data.
Uses shared upload helpers from core.cloud and Delta Lake push
from core.delta.
"""

import asyncio
from datetime import datetime
from functools import partial
from pathlib import Path

import re

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
        
        pattern = r'_(\d{8})_'
        eff_date = re.search(pattern, name).group(1) # type: ignore


        if dataset_type == RowType.OA:
            # {pipe_code}_{orig_code}_{type}_{eff_date}_{rest}
            
            return cfg.oa_bronze_blob_path(eff_date, name)

        elif dataset_type == RowType.SG:
            # {pipe_code}_{seg_id}_{eff_date}_{rest}
            return cfg.sg_bronze_blob_path(eff_date, name)


        elif dataset_type == RowType.NN:
            # {nn_code}_{type}_{eff_date}_{rest}
            return cfg.nn_bronze_blob_path(eff_date, name)

        elif dataset_type == RowType.META:
            return cfg.meta_bronze_blob_path(name)

        else:
            return f"Enbridge/other/{name}"

    def silver_blob_path(self, dataset_type: RowType, file_path: Path) -> str:
        """Construct silver blob path for concatenated silver parquets.

        Silver files are named: {DS}_{DS}_Enbridge_{mmddyyyy}_{hhmmss}_tgt_{mmddyyyy}.parquet
        Blob path: Enbridge/{DatasetFolder}/{YYYYMM}/{filename}
        """
        from datetime import datetime as _dt

        name = file_path.name[3:]
        folder_map = {
            RowType.OA: "OA",
            RowType.SG: "SG",
            RowType.NN: "NN",
        }
        folder = folder_map.get(dataset_type)
        if not folder:
            return f"Enbridge/other/{name}"


        return f"Enbridge/{folder}/{name}"

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
        self, silver_munger, pipe_configs_df, stats: RunStats | None = None,
        target_day: datetime | None = None,
    ) -> None:
        """Full push pipeline: raw meta → (OA + SG + ST + NN in parallel) → gold → cleanup.

        1. Push raw metadata to bronze
        2. In parallel: for each of OA, SG, ST, NN:
           a. Push raw to bronze
           b. Process silver (cleanse)
           c. Push silver
        3. Merge gold and push to Delta Lake
        """
        from .gold_munger import EnbridgeGoldMunger

        # Push raw metadata
        await self.push_bronze(self._paths.meta_raw, RowType.META)

        # Process and push OA, SG, NN in parallel
        async with asyncio.TaskGroup() as group:
            oa_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.OA, target_day)
            )
            sg_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.SG, target_day)
            )
            nn_task = group.create_task(
                self._push_dataset(silver_munger, pipe_configs_df, RowType.NN, target_day)
            )

        # Collect dataset details into stats
        if stats is not None:
            for task in (oa_task, sg_task, nn_task):
                for detail in task.result():
                    stats.add_dataset_detail(detail)

        # Gold merge and push
        gold_munger = EnbridgeGoldMunger(self._paths)
        gold_df = await gold_munger.merge({
            RowType.OA: self._paths.silver_dir(RowType.OA),
            RowType.SG: self._paths.silver_dir(RowType.SG),
            RowType.NN: self._paths.silver_dir(RowType.NN),
        }, target_day=target_day)
        await self.push_gold(gold_df)

        # Cleanup
        await gold_munger.clean_directories()

    async def _push_dataset(
        self, silver_munger, pipe_configs_df, dataset_type: RowType,
        target_day: datetime | None = None,
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
            raw_dir, silver_dir, dataset_type, pipe_configs_df, target_day=target_day
        )

        # Count silver records per pipe from the concatenated parquet(s)
        # Silver files now contain all pipes; group by PipeCode to get per-pipe counts
        silver_counts: dict[str, int] = {}
        silver_blob_files: dict[str, list[str]] = {}
        for f in silver_dir.iterdir():
            if not (f.is_file() and f.suffix == ".parquet"):
                continue
            try:
                blob_path = self.silver_blob_path(dataset_type, f)
                blob_url = f"{blob_base}/{settings.silver_container}/{blob_path}"
                # Silver parquets use GOLD_SCHEMA (no PipeCode column).
                # Attribute total row count to a synthetic "all" key per file.
                nrows = pl.scan_parquet(f).select(pl.len()).collect().item()
                silver_counts["_all"] = silver_counts.get("_all", 0) + nrows
                silver_blob_files.setdefault("_all", [])
                if blob_url not in silver_blob_files["_all"]:
                    silver_blob_files["_all"].append(blob_url)
            except Exception:
                pass

        # Push silver
        await self.push_silver(silver_dir, dataset_type)

        # Build details — silver is one combined file so silver_records are aggregate.
        # Per-pipe entries get raw_records; silver_records summed on first pipe entry.
        # expected_pipes comes from what was actually scraped (availability cache drives scraping).
        all_pipes = set(raw_counts)
        details: list[DatasetDetail] = []
        new_locs_map = {
            RowType.OA: getattr(silver_munger, "new_oa_locations", 0),
            RowType.SG: getattr(silver_munger, "new_sg_locations", 0),
            RowType.NN: getattr(silver_munger, "new_nn_locations", 0),
        }
        new_locs = new_locs_map.get(dataset_type, 0)
        total_silver = silver_counts.get("_all", 0)
        silver_file_urls = silver_blob_files.get("_all", [])
        for i, pipe_code in enumerate(sorted(all_pipes)):
            is_missing = pipe_code not in raw_counts
            details.append(DatasetDetail(
                dataset_type=dataset_type,
                pipe_code=pipe_code,
                raw_records=raw_counts.get(pipe_code, 0),
                # Assign aggregate silver count to the first pipe to avoid duplication
                silver_records=total_silver if i == 0 else 0,
                new_locations=new_locs if i == 0 else 0,
                raw_paths=raw_blob_files.get(pipe_code, []),
                silver_paths=silver_file_urls if i == 0 else [],
                missing=is_missing,
            ))

        return details
