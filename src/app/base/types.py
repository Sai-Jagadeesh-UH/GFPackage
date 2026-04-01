from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator, computed_field


class RowType(StrEnum):
    """Data types available for pipeline scraping."""
    OA = "OA"     # Operational Capacity
    SG = "SG"     # Segment Capacity
    ST = "ST"     # Storage Capacity
    NN = "NN"     # No Notice Activity
    META = "META" # Metadata (e.g. pipeline name, locs, etc.)


# The gold schema that all pipelines must produce.
# Fields a pipe doesn't have get filled with None in silver munging.
GOLD_SCHEMA: list[str] = [
    "GasMonth",    # YYYYMM — partition key
    "GFLocID",     # 10-char surrogate key (3-digit PipeID + 7-digit LocID)
    "Dataset",     # OA / SG / ST / NN
    "GasDay",      # effective gas date
    "LocName",
    "DesignCapacity",
    "OperatingCapacity",
    "TotalScheduledQuantity",
    "OperationallyAvailableCapacity",
    "IT",
    "FlowDirection",
    "Timestamp",
]


class PipeConfig(BaseModel, frozen=True):
    """Identity configuration for a pipeline — sourced from Azure Table Storage."""
    pipe_code: str = Field(min_length=2, max_length=10)
    parent_pipe: str = Field(min_length=2, max_length=50)
    pipe_name: str = Field(min_length=2)
    gf_pipe_id: str = Field(pattern=r"^\d{3}$")

    @field_validator("pipe_code")
    @classmethod
    def pipe_code_uppercase(cls, v: str) -> str:
        return v.upper()


class PipeChange(BaseModel):
    """A detected change in pipe availability vs stored PipeConfigs."""
    change_type: str   # "new" | "removed" | "renamed"
    pipe_code: str
    live_name: str | None = None    # name from UI (None if removed)
    config_name: str | None = None  # name from PipeConfigs (None if new)


class ScrapeResult(BaseModel):
    """Outcome of a single scrape attempt."""
    pipe_code: str = Field(min_length=2, max_length=10)
    dataset_type: RowType
    date: datetime
    success: bool
    duration_s: float = Field(ge=0)
    error: str | None = None

    @field_validator("error")
    @classmethod
    def error_only_on_failure(cls, v: str | None, info) -> str | None:
        if v and info.data.get("success"):
            raise ValueError("error must be None when success is True")
        return v


class DatasetDetail(BaseModel):
    """Per-dataset statistics collected during munge/push."""
    dataset_type: RowType
    pipe_code: str
    raw_records: int = 0
    silver_records: int = 0
    new_locations: int = 0
    raw_paths: list[str] = Field(default_factory=list)
    silver_paths: list[str] = Field(default_factory=list)
    missing: bool = False


class RunStats(BaseModel):
    """Aggregated stats for a full pipeline run."""
    pipeline: str = Field(min_length=1)
    start_time: datetime
    end_time: datetime | None = None
    results: list[ScrapeResult] = Field(default_factory=list)
    dataset_details: list[DatasetDetail] = Field(default_factory=list)
    pipe_changes: list[PipeChange] = Field(default_factory=list)

    def add(self, result: ScrapeResult) -> None:
        self.results.append(result)

    def add_dataset_detail(self, detail: DatasetDetail) -> None:
        self.dataset_details.append(detail)

    def add_pipe_changes(self, changes: list[PipeChange]) -> None:
        self.pipe_changes.extend(changes)

    @computed_field
    @property
    def total(self) -> int:
        return len(self.results)

    @computed_field
    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @computed_field
    @property
    def failed(self) -> int:
        return self.total - self.succeeded

    @property
    def failures(self) -> list[ScrapeResult]:
        return [r for r in self.results if not r.success]

    @computed_field
    @property
    def duration_s(self) -> float:
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()
