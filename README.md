# GFScrapePackage

Async web scraper for Enbridge gas pipeline data across four dataset types: **OA** (Operational Capacity), **SG** (Segment Capacity), **ST** (Storage Capacity), and **NN** (No Notice Activity). Processes data through a medallion architecture (bronze → silver → gold) and pushes to Azure Blob Storage and Delta Lake.

## Quick Start

```bash
# Install dependencies
uv sync

# Install Playwright browsers
uv run playwright install chromium

# Configure environment
cp .env.example .env   # Fill in Azure credentials

# Run daily scrape
uv run app
```

## Architecture

```
Scrape (Playwright)  →  Bronze (Azure Blob)  →  Silver (Polars cleanse)  →  Gold (Delta Lake)
   ↓                        ↓                       ↓                          ↓
rtba.enbridge.com      raw CSVs uploaded      normalized parquets       merged & upserted
infopost.enbridge.com  to bronze container    to silver container       partitioned by EffGasMonth
```

### Pipeline Flow

1. **Config** — Load `PipeConfigs` from Azure Table Storage (cached as parquet)
2. **Scrape** — Playwright browser automation against Enbridge websites
   - Short-way (rtba) → long-way (infopost) fallback
   - 3 retries per attempt (tenacity, exponential backoff)
   - 20s browser timeout (fail-fast)
   - All pipes scraped concurrently via `asyncio.TaskGroup`
3. **Push** — Medallion architecture, OA/OC/NN processed in parallel
   - **Bronze**: Raw CSVs → Azure Blob Storage
   - **Silver**: Polars cleansing → parquet → Azure Blob Storage
   - **Gold**: Merge all datasets → Delta Lake upsert
4. **Report** — HTML audit report with ADLS file links, missing download detection

### Entry Points

| Function | Description |
|----------|-------------|
| `scrapeToday()` | Yesterday + today + NN → push → report |
| `scrapeSomeday(date)` | Single date + NN → push → report |
| `scrapeRange(start, end)` | Historic backfill with ProcessPool |
| `scrapeFailed()` | Re-scrape from `Enbridge_fails.csv` |

## Project Structure

```
src/app/
├── __init__.py              # CLI entrypoint & public API
├── runner.py                # Top-level orchestrator
├── reporter.py              # HTML audit report generator
├── registry.py              # Pipeline registry
├── base/                    # Abstract base classes & types
├── core/                    # Shared infrastructure
│   ├── browser.py           #   Playwright (20s timeout, retry-ready)
│   ├── cloud.py             #   Azure Blob async uploads
│   ├── delta.py             #   Delta Lake upserts
│   ├── transforms.py        #   Polars batch transforms
│   └── ...                  #   settings, logging, paths, errors
└── pipelines/enbridge/      # Enbridge implementation
    ├── config.py            #   URLs, selectors, column maps
    ├── scraper.py           #   Browser scraping with tenacity retry
    ├── silver_munger.py     #   Raw → cleansed parquet
    ├── gold_munger.py       #   Silver merge → gold DataFrame
    ├── pusher.py            #   Medallion push orchestration
    └── runner.py            #   Pipeline orchestrator
```

## Documentation

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — Detailed flow diagrams, module reference, function signatures
- **[docs/architecture.excalidraw](docs/architecture.excalidraw)** — Visual system diagram (open in Excalidraw)

## Key Technologies

- **Playwright** — async browser automation
- **tenacity** — retry with exponential backoff
- **Polars** — high-performance data transformation
- **deltalake** — Delta Lake reads/writes
- **Azure Blob Storage** — bronze/silver data lake
- **pydantic** — config models & validation
- **loguru** — structured logging
- **uv** — package management

## Environment Variables

See `.env` for required configuration. Key variables:

- `PROD_STORAGE_CONSTR` — Azure Blob Storage connection string
- `ACCESS_KEY` — Azure Storage key for Delta Lake
- `BLOB_ACCOUNT_NAME` — Storage account for ADLS URLs in reports

## License

Private — GasFundies internal use.
