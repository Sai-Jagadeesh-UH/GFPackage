"""GFScrapePackage — modular gas pipeline data scraper.

Public API:
    scrapeToday()    — Scrape yesterday + today, push, and generate audit report
    scrapeSomeday()  — Scrape a specific date
    scrapeRange()    — Backfill a date range
    scrapeFailed()   — Re-scrape failed dates from fail log

All functions are async. Use `asyncio.run()` or the CLI entrypoint.

Example:
    import asyncio
    from app import scrapeToday
    asyncio.run(scrapeToday())
"""

import asyncio

from .runner import (
    scrape_today as scrapeToday,
    scrape_someday as scrapeSomeday,
    scrape_range as scrapeRange,
    scrape_failed as scrapeFailed,
    scrape_all_pipelines as scrapeAllPipelines,
)


def main() -> None:
    """CLI entrypoint — runs scrapeToday for Enbridge by default."""
    asyncio.run(scrapeToday(headless=True))


__all__ = [
    "main",
    "scrapeToday",
    "scrapeSomeday",
    "scrapeRange",
    "scrapeFailed",
    "scrapeAllPipelines",
]
