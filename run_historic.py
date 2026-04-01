"""Scrape last 45 days — push gold each day, archive to FileBackUp before cleaning."""
import asyncio
from datetime import datetime, timedelta
from app.pipelines.enbridge.runner import EnbridgeRunner


async def main():
    runner = EnbridgeRunner(headless=True)
    end = datetime.today()- timedelta(days=45)
    start = end - timedelta(days=365)
    print(f"Scraping {start:%Y-%m-%d} → {end:%Y-%m-%d}")
    stats = await runner.scrape_range(start_date=start, end_date=end)
    print(f"\n--- DONE ---")
    print(f"Total: {stats.total}  OK: {stats.succeeded}  Failed: {stats.failed}  Duration: {stats.duration_s:.1f}s")


asyncio.run(main())
