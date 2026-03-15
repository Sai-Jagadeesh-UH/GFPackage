from datetime import datetime
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel

from app.base.types import RowType


class PipelinePaths(BaseModel, frozen=True):
    """Directory layout for a pipeline's working files.

    Created via PipelinePaths.create() which ensures directories exist.
    """
    root: Path
    logs: Path
    fail_file: Path
    downloads: Path
    config_files: Path
    reports: Path

    # Raw download subdirectories per dataset type
    oa_raw: Path
    sg_raw: Path
    st_raw: Path
    nn_raw: Path
    meta_raw: Path

    # Silver (normalized) subdirectories
    oa_silver: Path
    sg_silver: Path
    st_silver: Path
    nn_silver: Path

    @classmethod
    @lru_cache(maxsize=8)
    def create(cls, root: Path, pipeline_name: str) -> "PipelinePaths":
        """Build paths for a pipeline and create directories.

        Cached: same (root, pipeline_name) returns the same instance.
        """
        root = root.resolve()
        downloads = root / "downloads" / pipeline_name.lower()
        config_files = root / "src" / "artifacts" / "configFiles"
        logs = root / "logs"
        today = datetime.now().strftime("%Y-%m-%d")
        reports = root / "reports" / today

        paths = cls(
            root=root,
            logs=logs,
            fail_file=logs / f"{pipeline_name}_fails.csv",
            downloads=downloads,
            config_files=config_files,
            reports=reports,
            oa_raw=downloads / "OA_raw",
            sg_raw=downloads / "SG_raw",
            st_raw=downloads / "ST_raw",
            nn_raw=downloads / "NN_raw",
            meta_raw=downloads / "MetaData",
            oa_silver=downloads / "OA",
            sg_silver=downloads / "SG",
            st_silver=downloads / "ST",
            nn_silver=downloads / "NN",
        )

        # Ensure all directories exist
        for d in [
            paths.logs, paths.downloads, paths.config_files,
            paths.reports,
            paths.oa_raw, paths.sg_raw, paths.st_raw,
            paths.nn_raw, paths.meta_raw,
            paths.oa_silver, paths.sg_silver,
            paths.st_silver, paths.nn_silver,
        ]:
            d.mkdir(parents=True, exist_ok=True)

        return paths

    def raw_dir(self, dataset_type: RowType) -> Path:
        """Get raw download directory for a dataset type."""
        mapping: dict[RowType, Path] = {
            RowType.OA: self.oa_raw,
            RowType.SG: self.sg_raw,
            RowType.ST: self.st_raw,
            RowType.NN: self.nn_raw,
            RowType.META: self.meta_raw,
        }
        return mapping[dataset_type]

    def silver_dir(self, dataset_type: RowType) -> Path:
        """Get silver directory for a dataset type."""
        mapping: dict[RowType, Path] = {
            RowType.OA: self.oa_silver,
            RowType.SG: self.sg_silver,
            RowType.ST: self.st_silver,
            RowType.NN: self.nn_silver,
        }
        if dataset_type not in mapping:
            raise ValueError(f"No silver directory for dataset type {dataset_type}")
        return mapping[dataset_type]
