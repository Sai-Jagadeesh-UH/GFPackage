"""Top-level runner — dispatches to registered pipeline runners.

Provides the main orchestration layer that:
1. Sets up logging
2. Resolves the target pipeline(s)
3. Runs scrape operations
4. Generates HTML audit reports

This module is the single entry point for all scraping operations.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence

from app.base.types import RunStats
from app.core.logging import setup_logger, logger
from app.core.settings import settings
from app.registry import registry
from app.reporter import generate_report


def _get_root() -> Path:
    return settings.root_dir.resolve()


def _setup() -> Path:
    """Initialize logging and return root dir."""
    root = _get_root()
    setup_logger(root / "logs")
    return root


async def scrape_today(
    pipeline: str = "enbridge",
    headless: bool = True,
    report: bool = True,
) -> RunStats:
    """Scrape yesterday + today's data for a pipeline, then push.

    Args:
        pipeline: Pipeline name (e.g. "enbridge").
        headless: Run browser in headless mode.
        report: Generate HTML audit report after run.

    Returns:
        RunStats with all scrape outcomes.
    """
    root = _setup()
    runner_cls = registry.get_or_raise(pipeline)
    runner = runner_cls(root_dir=root, headless=headless)

    logger.info(f"scrapeToday starting for '{pipeline}'")
    stats = await runner.scrape_today()

    if report:
        await _write_report([stats], root)

    return stats


async def scrape_someday(
    scrape_day: datetime,
    pipeline: str = "enbridge",
    headless: bool = True,
    report: bool = True,
) -> RunStats:
    """Scrape a specific date for a pipeline, then push.

    Args:
        scrape_day: The date to scrape.
        pipeline: Pipeline name.
        headless: Run browser in headless mode.
        report: Generate HTML audit report after run.
    """
    root = _setup()
    runner_cls = registry.get_or_raise(pipeline)
    runner = runner_cls(root_dir=root, headless=headless)

    logger.info(f"scrapeSomeday starting for '{pipeline}' date={scrape_day}")
    stats = await runner.scrape_someday(scrape_day)

    if report:
        await _write_report([stats], root)

    return stats


async def scrape_range(
    start_date: datetime,
    end_date: datetime | None = None,
    pipeline: str = "enbridge",
    headless: bool = True,
    report: bool = True,
) -> RunStats:
    """Scrape a date range for a pipeline using historic backfill.

    Args:
        start_date: Start of date range.
        end_date: End of date range (defaults to today).
        pipeline: Pipeline name.
        headless: Run browser in headless mode.
        report: Generate HTML audit report after run.
    """
    root = _setup()
    runner_cls = registry.get_or_raise(pipeline)
    runner = runner_cls(root_dir=root, headless=headless)

    logger.info(
        f"scrapeRange starting for '{pipeline}' "
        f"from {start_date:%Y-%m-%d} to {(end_date or datetime.today()):%Y-%m-%d}"
    )
    end = end_date or datetime.today()
    stats = await runner.scrape_range(start_date=start_date, end_date=end)

    if report:
        await _write_report([stats], root)

    return stats


async def scrape_failed(
    pipeline: str = "enbridge",
    headless: bool = True,
    report: bool = True,
) -> RunStats:
    """Re-scrape failed dates from the fail log.

    Args:
        pipeline: Pipeline name.
        headless: Run browser in headless mode.
        report: Generate HTML audit report after run.
    """
    root = _setup()
    runner_cls = registry.get_or_raise(pipeline)
    runner = runner_cls(root_dir=root, headless=headless)

    logger.info(f"scrapeFailedDates starting for '{pipeline}'")
    stats = await runner.scrape_failed_dates()

    if report:
        await _write_report([stats], root)

    return stats


async def scrape_all_pipelines(
    headless: bool = True,
    report: bool = True,
) -> list[RunStats]:
    """Run scrapeToday for all registered pipelines.

    Returns:
        List of RunStats, one per pipeline.
    """
    root = _setup()
    all_stats: list[RunStats] = []

    for name in registry.available:
        logger.info(f"Running pipeline: {name}")
        runner_cls = registry.get_or_raise(name)
        runner = runner_cls(root_dir=root, headless=headless)
        stats = await runner.scrape_today()
        all_stats.append(stats)

    if report:
        await _write_report(all_stats, root)

    return all_stats


async def _write_report(stats_list: list[RunStats], root: Path) -> None:
    """Write HTML audit report to the date-organized reports directory."""
    now = datetime.now()
    reports_dir = root / "reports" / f"{now:%Y-%m-%d}"
    output_path = reports_dir / f"audit_{now:%Y%m%d_%H%M%S}.html"
    await asyncio.to_thread(generate_report, stats_list, output_path)
