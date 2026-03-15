# GFScrapePackage — Architecture & Flow Reference

> Developer reference document — system flow diagrams, module responsibilities, and function-level documentation.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Module Dependency Graph](#2-module-dependency-graph)
3. [Daily Scrape Flow (scrapeToday)](#3-daily-scrape-flow-scrapetoday)
4. [Scraper Flow — Per-Pipe Decision Tree](#4-scraper-flow--per-pipe-decision-tree)
5. [Medallion Push Pipeline](#5-medallion-push-pipeline)
6. [Silver Munger — Data Transformation](#6-silver-munger--data-transformation)
7. [Delta Lake Gold Push](#7-delta-lake-gold-push)
8. [Retry & Fault Tolerance Strategy](#8-retry--fault-tolerance-strategy)
9. [Module Reference](#9-module-reference)

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        GFScrapePackage                              │
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌───────────┐    ┌──────────┐ │
│  │  Scrape   │───▶│  Bronze Push  │───▶│  Silver   │───▶│  Gold    │ │
│  │ (Playwright)│  │ (Azure Blob) │    │ (Cleanse) │    │ (Delta)  │ │
│  └──────────┘    └──────────────┘    └───────────┘    └──────────┘ │
│       │                                     │               │       │
│       ▼                                     ▼               ▼       │
│  downloads/          bronze container   silver container  Delta Lake │
│  enbridge/           (Azure Blob)       (Azure Blob)     (ADLS)    │
│  ├─ OA_raw/                                                        │
│  ├─ SG_raw/          Enbridge/          Enbridge/        goldlayer/ │
│  ├─ ST_raw/          ├─ PointCapacity/  ├─ PointCapacity/ GFundies │
│  ├─ NN_raw/          ├─ SegmentCapacity/├─ SegmentCapacity/        │
│  ├─ MetaData/        ├─ StorageCapacity/├─ StorageCapacity/        │
│  ├─ OA/ (silver)     ├─ NoNotice/       └─ NoNotice/              │
│  ├─ SG/ (silver)     └─ Metadata/                                  │
│  ├─ ST/ (silver)                                                   │
│  └─ NN/ (silver)                                                   │
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ Reporter │    │ Azure Tables │    │ Config (PipeConfigs,     │  │
│  │ (HTML)   │    │ (Metadata,   │    │  SegmentConfigs, .env)   │  │
│  └──────────┘    │  Segments)   │    └──────────────────────────┘  │
│                  └──────────────┘                                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Module Dependency Graph

```
src/app/
├── __init__.py              ─── CLI entrypoint (main, scrapeToday, etc.)
├── runner.py                ─── Top-level orchestrator (setup, report)
├── reporter.py              ─── HTML audit report generator
├── registry.py              ─── Pipeline registry (name → RunnerClass)
│
├── base/                    ─── Abstract base classes
│   ├── types.py             ─── RowType, PipeConfig, ScrapeResult, DatasetDetail, RunStats
│   ├── scraper.py           ─── BasePipeScraper (ABC)
│   ├── pusher.py            ─── BasePusher (ABC)
│   └── munger.py            ─── BaseSilverMunger, BaseGoldMunger (ABC)
│
├── core/                    ─── Shared infrastructure
│   ├── settings.py          ─── AppSettings (pydantic-settings, .env)
│   ├── paths.py             ─── PipelinePaths (directory layout)
│   ├── browser.py           ─── open_page(), fill_date_box() (Playwright)
│   ├── cloud.py             ─── push_directory(), upload_blob() (Azure Blob)
│   ├── delta.py             ─── LakeMerge (Delta Lake upserts)
│   ├── azure_tables.py      ─── dump_pipe_configs(), dump_segment_configs()
│   ├── transforms.py        ─── Polars batch transforms (dates, floats, GFLOC)
│   ├── logging.py           ─── Loguru setup (app.log, app_error.log)
│   └── errors.py            ─── track_fail() (fail CSV writer)
│
└── pipelines/
    └── enbridge/            ─── Enbridge pipeline implementation
        ├── __init__.py      ─── Auto-registers with PipelineRegistry
        ├── config.py        ─── URLs, selectors, column maps, blob paths
        ├── runner.py        ─── EnbridgeRunner (orchestrator)
        ├── scraper.py       ─── EnbridgeScraper (Playwright browser automation)
        ├── silver_munger.py ─── EnbridgeSilverMunger (raw → silver transforms)
        ├── gold_munger.py   ─── EnbridgeGoldMunger (silver merge → gold)
        └── pusher.py        ─── EnbridgePusher (blob + delta push)
```

### Import / Call Graph

```
__init__.py ──▶ runner.py ──▶ registry.py ──▶ EnbridgeRunner
                   │                              │
                   ▼                              ├──▶ EnbridgeScraper
              reporter.py                         ├──▶ EnbridgeSilverMunger
                                                  ├──▶ EnbridgePusher
                                                  └──▶ PipelinePaths

EnbridgeScraper ──▶ browser.py (open_page, fill_date_box)
                ──▶ errors.py  (track_fail)
                ──▶ config.py  (URLs, selectors)

EnbridgeSilverMunger ──▶ transforms.py (batch_date_parse, compose_gfloc, ...)
                     ──▶ azure_tables.py (dump_segment_configs, update_segment_configs)

EnbridgePusher ──▶ cloud.py (push_directory)
               ──▶ delta.py (LakeMerge)

EnbridgeGoldMunger ──▶ (Polars merge only, no external deps)
```

---

## 3. Daily Scrape Flow (scrapeToday)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          main() → scrapeToday()                             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  runner.py :: scrape_today(pipeline="enbridge")                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ 1. setup_logger(root/logs)                                             ││
│  │ 2. registry.get_or_raise("enbridge") → EnbridgeRunner                  ││
│  │ 3. runner = EnbridgeRunner(root_dir, headless)                         ││
│  │ 4. stats = await runner.scrape_today()   ◀─── SEE BELOW               ││
│  │ 5. _write_report([stats], root)          ◀─── reporter.generate_report ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  EnbridgeRunner :: scrape_today()                                           │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  PHASE 1: LOAD CONFIG                                                │    │
│  │  _load_pipe_configs()                                                │    │
│  │    └─▶ dump_pipe_configs()     ──▶ Azure Table "PipeConfigs"         │    │
│  │         └─▶ PipeConfigs.parquet (cached with mtime)                  │    │
│  │    └─▶ Build [PipeConfig] models from DataFrame rows                 │    │
│  │         Fields loaded: oa_code, sg_code, st_code, nn_code, meta_code │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                        │                                                     │
│                        ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  PHASE 2: SCRAPE                                                     │    │
│  │                                                                      │    │
│  │  2a. scraper.scrape_metadata(configs)                                │    │
│  │      └─▶ TaskGroup: HTTP GET metadata CSVs → downloads/MetaData/     │    │
│  │                                                                      │    │
│  │  2b. scraper.scrape_all(configs, yesterday)                          │    │
│  │      └─▶ TaskGroup: scrape(pc, yesterday) for each pipe              │    │
│  │          └─▶ Playwright: OA + SG + ST per pipe                      │    │
│  │          └─▶ short-way → long-way fallback                          │    │
│  │          └─▶ Returns [ScrapeResult]                                  │    │
│  │                                                                      │    │
│  │  2c. scraper.scrape_all(configs, today)                              │    │
│  │      └─▶ Same as 2b with today's date                               │    │
│  │                                                                      │    │
│  │  2d. scraper.scrape_nn(configs, today)                               │    │
│  │      └─▶ TaskGroup: scrape_nn(pc, today - 4 days) for each pipe     │    │
│  │          └─▶ Playwright: rtba NN pages                               │    │
│  │          └─▶ Returns [ScrapeResult]                                  │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                        │                                                     │
│                        ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  PHASE 3: PUSH (pusher.push_all)                                     │    │
│  │                                                                      │    │
│  │  3a. push_bronze(meta_raw, META) ──▶ Azure Blob bronze/              │    │
│  │                                                                      │    │
│  │  3b. TaskGroup (PARALLEL):                                           │    │
│  │      ├─ _push_dataset(OA)  ──▶ bronze push → silver munge → silver  │    │
│  │      ├─ _push_dataset(SG)      push → collect DatasetDetail          │    │
│  │      ├─ _push_dataset(ST)                                           │    │
│  │      └─ _push_dataset(NN)                                           │    │
│  │                                                                      │    │
│  │  3c. gold_munger.merge(OA + SG + ST + NN silver dirs)               │    │
│  │      └─▶ Polars concat + dedup → gold DataFrame                     │    │
│  │                                                                      │    │
│  │  3d. push_gold(gold_df)                                              │    │
│  │      └─▶ LakeMerge → Delta Lake upsert (partitioned by EffGasMonth) │    │
│  │                                                                      │    │
│  │  3e. gold_munger.clean_directories() [disabled for testing]          │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                        │                                                     │
│                        ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  PHASE 4: REPORT                                                     │    │
│  │  reporter.generate_report([stats])                                   │    │
│  │    └─▶ reports/{YYYY-MM-DD}/audit_{YYYYMMDD}_{HHMMSS}.html          │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Scraper Flow — Per-Pipe Decision Tree

```
                    scrape(pipe_config, date)
                            │
                            ▼
                ┌───────────────────────┐
                │ pipe_code in           │
                │ LONG_WAY_ONLY_CODES?   │
                │ (e.g., MNUS)           │
                └───────┬───────┬───────┘
                   YES  │       │  NO
                        │       │
          ┌─────────────┘       └─────────────┐
          ▼                                   ▼
┌──────────────────┐               ┌──────────────────┐
│ _scrape_long_way │               │ _scrape_short_way │
│ _with_retry()    │               │ _with_retry()     │
│                  │               │                   │
│ infopost.        │               │ rtba.enbridge.com │
│ enbridge.com     │               │                   │
│ ├─ Navigate home │               │ ├─ goto OA URL    │
│ ├─ Click Capacity│               │ ├─ fill_date_box  │
│ ├─ Click OA link │               │ ├─ _download_oa() │
│ ├─ Access iframe │               │ ├─ goto ST URL    │
│ ├─ fill_date_box │               │ ├─ fill_date_box  │
│ ├─ _download_oa()│               │ ├─ _download_st() │
│ ├─ _download_st()│               │ └─ _scrape_sg()   │
│ └─ _scrape_sg()  │               └────────┬──────────┘
└────────┬─────────┘                        │
         │                                  │ FAILS (timeout, etc.)
         │                                  ▼
         │                       ┌──────────────────┐
         │                       │ _scrape_long_way  │
         │                       │ _with_retry()     │
         │                       │ (fallback)        │
         │                       └────────┬──────────┘
         │                                │
         ▼                                ▼
    ┌──────────┐                   ┌──────────────┐
    │ SUCCESS  │                   │ STILL FAILS? │
    │          │                   └──┬───────┬───┘
    │ Return   │                  YES │       │ NO
    │ ScrapeResult│                   │       │
    │ success=True│                   ▼       ▼
    └──────────┘             ┌──────────┐ ┌──────────┐
                             │track_fail│ │ SUCCESS  │
                             │log error │ │ Return   │
                             │Return    │ │ success  │
                             │fail=True │ │ =True    │
                             └──────────┘ └──────────┘


    ┌─────────────────────────────────────────────────┐
    │           RETRY STRATEGY (each attempt)         │
    │                                                 │
    │  Attempt 1 ──▶ fail ──▶ wait 2s                 │
    │  Attempt 2 ──▶ fail ──▶ wait 4s                 │
    │  Attempt 3 ──▶ fail ──▶ GIVE UP (reraise)       │
    │                                                 │
    │  Retries on: PlaywrightTimeout, TimeoutError,   │
    │              ConnectionError                    │
    │  Browser timeout: 20s (fail-fast)               │
    └─────────────────────────────────────────────────┘
```

### SG Segment Scraping Detail

```
_scrape_sg(mainpage, pipecode, date, iframe?)
                    │
                    ▼
         ┌────────────────────┐
         │ Get SG segment     │
         │ links from page    │
         │ (text_list)        │
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────────┐
         │ For each segment_text: │◀────────────────┐
         └─────────┬──────────────┘                 │
                   │                                │
         ┌─────────▼──────────┐                     │
         │ iframe present?    │                     │
         └──┬──────────┬──────┘                     │
        YES │          │ NO                         │
            │          │                            │
            ▼          ▼                            │
  ┌──────────────┐ ┌──────────────┐                 │
  │ Raise to     │ │ Click segment│                 │
  │ trigger      │ │ navigate     │                 │
  │ long-way     │ │ download CSV │                 │
  │ fallback     │ │ go_back()    │                 │
  └──────┬───────┘ └──────┬───────┘                 │
         │                │ success                  │
         │                └─────────────────────────┘
         │
         │ (or any failure)
         ▼
  ┌────────────────────────┐
  │ Skip if in             │
  │ SG_SKIP_SEGMENTS?      │
  └──┬──────────┬──────────┘
  YES│          │ NO
     │          │
     ▼          ▼
  (skip)  ┌─────────────────────┐
          │ _sg_refresh_dump()  │
          │ for remaining segs  │
          │                     │
          │ ├─ Re-navigate home │
          │ ├─ Access iframe    │
          │ ├─ Click segment    │
          │ └─ Download CSV     │
          └─────────────────────┘
```

---

## 5. Medallion Push Pipeline

```
pusher.push_all(silver_munger, pipe_configs_df, stats)
                    │
    ┌───────────────┼──────────────────────────────────────────────┐
    │               │                                              │
    ▼               ▼                                              ▼
 ┌────────┐   ┌──────────────────────────────────────────┐   ┌──────────┐
 │ BRONZE │   │          TaskGroup (PARALLEL)             │   │   GOLD   │
 │ META   │   │                                          │   │          │
 │        │   │  ┌──────┐ ┌──────┐ ┌──────┐ ┌────────┐  │   │          │
 │push_   │   │  │_push_│ │_push_│ │_push_│ │_push_  │  │   │          │
 │bronze( │   │  │data( │ │data( │ │data( │ │data(   │  │   │          │
 │meta_raw│   │  │ OA)  │ │ SG)  │ │ ST)  │ │ NN)    │  │   │          │
 │ META)  │   │  └──┬───┘ └──┬───┘ └──┬───┘ └───┬────┘  │   │          │
 └────────┘   │     │        │        │          │       │   │          │
              └─────┼────────┼────────┼──────────┼───────┘   │          │
                    └────────┴────────┴──────────┘           │          │
                                      │                      │          │
                           ┌──────────▼──────────┐           │          │
                           │  Collect            │           │          │
                           │  DatasetDetail      │           │          │
                           │  into stats         │           │          │
                           └──────────┬──────────┘           │          │
                                      │                      │          │
                                      ▼                      │          │
                           ┌──────────────────────┐          │          │
                           │ gold_munger.merge()  │──────────┘          │
                           │ OA + SG + ST + NN    │                     │
                           │ silver → gold df     │                     │
                           └──────────┬───────────┘                     │
                                      │                                 │
                                      ▼                                 │
                           ┌──────────────────────┐                     │
                           │ push_gold(gold_df)   │◀────────────────────┘
                           │ └─ LakeMerge()       │
                           │    └─ Delta Lake     │
                           │       upsert         │
                           └──────────────────────┘


_push_dataset(dataset_type) — Internal Detail:
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  1. Count raw rows per pipe                                  │
│     └─ Iterate raw_dir/*.csv, count lines - 1 (header)      │
│     └─ Collect ADLS file paths per pipe                      │
│                                                              │
│  2. push_bronze(raw_dir, type)                               │
│     └─ push_directory() → upload each file to Azure Blob     │
│        └─ Blob path: bronze_blob_path(type, file)            │
│                                                              │
│  3. silver_munger.process(raw_dir, silver_dir, type, df)     │
│     └─ CSV → Parquet → Cleanse (see §6)                      │
│                                                              │
│  4. Count silver records per pipe                            │
│     └─ pl.scan_parquet(f).select(pl.len())                   │
│     └─ Collect ADLS file paths per pipe                      │
│                                                              │
│  5. push_silver(silver_dir, type)                            │
│     └─ push_directory() → upload to Azure Blob               │
│        └─ Blob path: silver_blob_path(type, file)            │
│                                                              │
│  6. Build [DatasetDetail] per pipe                           │
│     ├─ raw_records, silver_records, new_locations            │
│     ├─ raw_paths (ADLS URLs), silver_paths (ADLS URLs)       │
│     └─ missing = expected but not downloaded                 │
│                                                              │
│  Return [DatasetDetail]                                      │
└──────────────────────────────────────────────────────────────┘
```

### Blob Path Patterns

> Note: Paths do **not** include `PipeCode` — files are stored flat within the month folder.

| Type | Bronze Path | Silver Path |
|------|-------------|-------------|
| OA | `Enbridge/PointCapacity/{YYYYMM}/{filename}.csv` | `Enbridge/PointCapacity/{YYYYMM}/{filename}.parquet` |
| SG | `Enbridge/SegmentCapacity/{YYYYMM}/{filename}.csv` | `Enbridge/SegmentCapacity/{YYYYMM}/{filename}.parquet` |
| ST | `Enbridge/StorageCapacity/{YYYYMM}/{filename}.csv` | `Enbridge/StorageCapacity/{YYYYMM}/{filename}.parquet` |
| NN | `Enbridge/NoNotice/{YYYY}/{filename}.csv` | `Enbridge/NoNotice/{YYYY}/{filename}.parquet` |
| META | `Enbridge/Metadata/{filename}.csv` | *(not pushed to silver)* |

---

## 6. Silver Munger — Data Transformation

```
silver_munger.process(raw_dir, silver_dir, dataset_type, pipe_configs_df)
                              │
            ┌─────────────────┼──────────────────┬───────────────┐
            │                 │                  │               │
            ▼                 ▼                  ▼               ▼
       ┌────────┐       ┌──────────┐       ┌────────┐     ┌──────────┐
       │   OA   │       │  SG / ST │       │   NN   │     │   META   │
       └────┬───┘       └────┬─────┘       └────┬───┘     └────┬─────┘
            │                │                  │               │
            ▼                ▼                  ▼               ▼
    ┌──────────┐     ┌──────────────┐    ┌──────────┐  ┌───────────┐
    │CSV → PQ  │     │ SG: CSV → PQ │    │CSV → PQ  │  │CSV → PQ   │
    │(add      │     │ (extract     │    │(add      │  │(combine   │
    │PipeCode) │     │ date from    │    │PipeCode) │  │all pipes) │
    └────┬─────┘     │ filename)    │    └────┬─────┘  └────┬──────┘
         │           │              │         │              │
         ▼           │ ST: CSV → PQ │         ▼              ▼
    ┌──────────┐     │ (add PipeCode│    ┌──────────┐  ┌───────────┐
    │TaskGroup │     │ like OA)     │    │TaskGroup │  │Query Azure│
    │_cleanse_ │     └──────┬───────┘    │_cleanse_ │  │Metadata   │
    │oa()      │            │            │nn()      │  │Table      │
    │          │            ▼            │          │  │Upsert new │
    │          │    ┌──────────────┐     │          │  │rows       │
    │          │    │dump_segment_ │     │          │  └───────────┘
    │          │    │configs()     │     │          │
    │          │    └──────┬───────┘     │          │
    └────┬─────┘           │             └────┬─────┘
         │                 ▼                  │
         │        ┌────────────────┐          │
         │        │_discover_new_  │          │
         │        │sg_segments()   │          │
         │        │→ upsert Azure  │          │
         │        │  Table         │          │
         │        └──────┬─────────┘          │
         │               │                    │
         │               ▼                    │
         │        ┌──────────┐                │
         │        │TaskGroup │                │
         │        │_cleanse_ │                │
         │        │sg()      │                │
         │        └────┬─────┘                │
         │             │                      │
         ▼             ▼                      ▼
    ┌───────────────────────────────────────────┐
    │           CLEANSE PIPELINE                │
    │                                           │
    │  1. pl.scan_parquet(file)                 │
    │  2. Select raw columns                    │
    │  3. Filter null rows                      │
    │  4. Parse dates, floats                   │
    │  5. Map flow indicators                   │
    │  6. Pad location codes (6 chars)          │
    │  7. Join pipe_configs (GFPipeID)          │
    │  8. add_modeling_columns(row_type)        │
    │     ├─ EffGasMonth (YYYYMM int)           │
    │     ├─ GFLocID                            │
    │     └─ RowType → "OA" | "SG" | "ST" | "NN"│
    │  9. compose_gfloc()                       │
    │     └─ GFLOC = GFPipeID|RowType|          │
    │               GFLocID|FlowInd             │
    │ 10. Select GOLD_SCHEMA columns            │
    │ 11. Write parquet (overwrite)             │
    └───────────────────────────────────────────┘
```

### Gold Schema

```
GOLD_SCHEMA = [
    "EffGasMonth",                    # int: YYYYMM (partition key)
    "GFPipeID",                       # int: 100–999
    "GFLocID",                        # str: padded location ID
    "RowType",                        # str: OA | SG | ST | NN
    "ParentPipe",                     # str: "Enbridge"
    "PipelineName",                   # str: e.g., "Alliance Pipeline"
    "GFLOC",                          # str: composite key (GFPipeID|RowType|GFLocID|FlowInd)
    "EffGasDay",                      # date: effective gas day
    "CycleDesc",                      # str: cycle description (OA/ST)
    "LocPurpDesc",                    # str: location purpose (OA/ST)
    "Loc",                            # str: location code (padded 6 chars)
    "LocName",                        # str: location name
    "LocZn",                          # str: location zone (OA/ST)
    "LocSegment",                     # str: segment name (SG)
    "DesignCapacity",                 # float: design capacity (OA/ST)
    "OperatingCapacity",              # float: operating capacity
    "TotalScheduledQuantity",         # float: scheduled quantity
    "OperationallyAvailableCapacity", # float: available capacity
    "IT",                             # str: interruptible transport (OA/ST)
    "FlowInd",                        # str: D|R|F|B (flow direction)
    "AllQtyAvail",                    # str: all quantity available (OA/ST)
    "QtyReason",                      # str: accounting indicator (NN)
    "Timestamp",                      # datetime: processing timestamp
]
```

---

## 7. Delta Lake Gold Push

```
LakeMerge(df=gold_df, parent_pipe="Enbridge")
                    │
                    ▼
         ┌──────────────────────┐
         │ Load DeltaTable      │
         │ table_uri = abfss:// │
         │ {container}@{account}│
         │ .dfs.core.windows.net│
         │ /{table_name}        │
         └──────────┬───────────┘
                    │
           ┌────────┴─────────┐
           │                  │
           ▼                  ▼
    ┌────────────┐    ┌──────────────┐
    │Table exists│    │Table missing │
    │ Load ref   │    │ self.dt=None │
    │ Set log    │    │ (first run)  │
    │ retention  │    └──────┬───────┘
    └─────┬──────┘           │
          │                  │
          └────────┬─────────┘
                   │
                   ▼
         ┌──────────────────────┐
         │ _push_by_row_type()  │
         │                      │
         │ For each unique      │
         │ RowType in df:       │
         │                      │
         │ ┌──────────────────┐ │
         │ │ Filter by RowType│ │
         │ │                  │ │
         │ │ Build predicate: │ │
         │ │ ParentPipe AND   │ │
         │ │ EffGasDay IN (...│ │
         │ │ ) AND RowType    │ │
         │ │                  │ │
         │ │ write_deltalake( │ │
         │ │   mode=overwrite │ │
         │ │   partition_by=  │ │
         │ │   "EffGasMonth"  │ │
         │ │   predicate=...  │ │
         │ │ )                │ │
         │ └──────────────────┘ │
         │                      │
         │ If self.dt was None: │
         │   First write creates│
         │   table, then load   │
         │   DeltaTable ref     │
         └──────────────────────┘
```

---

## 8. Retry & Fault Tolerance Strategy

```
┌──────────────────────────────────────────────────────────────────────┐
│                    FAULT TOLERANCE LAYERS                            │
│                                                                      │
│  LAYER 1: Browser Timeout (Fail-Fast)                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • Default timeout: 20 seconds (navigation + actions)          │  │
│  │ • Set via page.set_default_timeout(20_000)                    │  │
│  │ • Prevents hanging on unresponsive pages                      │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  LAYER 2: Tenacity Retry (Per-Attempt)                              │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • Max 3 attempts per scrape operation                         │  │
│  │ • Exponential backoff: 2s → 4s → give up                     │  │
│  │ • Retries on: PlaywrightTimeout, TimeoutError, ConnectionError│  │
│  │ • Each retry gets a fresh browser instance (open_page)        │  │
│  │ • Logged: "{pipe_code} short-way retry 2/3"                   │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  LAYER 3: Short-Way → Long-Way Fallback                            │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • If short-way (rtba) fails all 3 retries:                    │  │
│  │   → Try long-way (infopost) with its own 3 retries            │  │
│  │ • Total max attempts: 6 (3 short + 3 long)                   │  │
│  │ • LONG_WAY_ONLY_CODES (MNUS) skip short-way entirely         │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  LAYER 4: Failure Tracking                                          │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • track_fail() writes to logs/Enbridge_fails.csv              │  │
│  │ • Format: "{pipe_code}|{type}|{date}"                         │  │
│  │ • ScrapeResult(success=False, error=str(e)) collected         │  │
│  │ • scrape_failed_dates() can re-process failed entries         │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  LAYER 5: Async Concurrency                                         │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • All OA/SG/ST pipes scraped concurrently (asyncio.TaskGroup) │  │
│  │ • All NN pipes scraped concurrently (asyncio.TaskGroup)       │  │
│  │ • Metadata downloads concurrent (asyncio.TaskGroup)           │  │
│  │ • Push OA/SG/ST/NN in parallel (asyncio.TaskGroup)           │  │
│  │ • Individual pipe failure doesn't block other pipes           │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  LAYER 6: Delta Lake First-Run Handling                             │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ • If DeltaTable doesn't exist → self.dt = None                │  │
│  │ • First write_deltalake creates the table (no predicate)      │  │
│  │ • Subsequent writes use overwrite with predicate              │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 9. Module Reference

### 9.1 Entrypoint — `src/app/__init__.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `main` | `() → None` | CLI entrypoint, calls `scrapeToday(headless=False)` |
| `scrapeToday` | `(headless=False) → RunStats` | Scrape yesterday + today, push, report |
| `scrapeSomeday` | `(scrape_day, headless=False) → RunStats` | Scrape a specific date |
| `scrapeRange` | `(start_date, end_date, headless=False) → RunStats` | Historic backfill |
| `scrapeFailed` | `(headless=False) → RunStats` | Re-scrape failed dates |
| `scrapeAllPipelines` | `(headless=False) → list[RunStats]` | Run all registered pipelines |

### 9.2 Top-Level Runner — `src/app/runner.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `scrape_today` | `(pipeline, headless, report) → RunStats` | Setup → run → report |
| `scrape_someday` | `(scrape_day, pipeline, headless, report) → RunStats` | Single date scrape |
| `scrape_range` | `(start, end, pipeline, headless, report) → RunStats` | Date range backfill |
| `scrape_failed` | `(pipeline, headless, report) → RunStats` | Re-scrape failures |
| `_write_report` | `(stats_list, root) → None` | Generate HTML audit report |

### 9.3 EnbridgeRunner — `src/app/pipelines/enbridge/runner.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `__init__` | `(root_dir?, headless=True)` | Creates paths, scraper, munger, pusher |
| `scrape_today` | `() → RunStats` | Yesterday + today + NN + push |
| `scrape_someday` | `(scrape_day) → RunStats` | Single date + push |
| `scrape_historic` | `(start?, end?) → RunStats` | Parallel backfill + push |
| `scrape_failed_dates` | `() → RunStats` | Re-scrape from fails CSV |
| `_load_pipe_configs` | `() → (DataFrame, [PipeConfig])` | Load from Azure Table; maps PointCapCode→oa_code, SegmentCapCode→sg_code, StorageCapCode→st_code |

### 9.4 EnbridgeScraper — `src/app/pipelines/enbridge/scraper.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `scrape` | `(pipe_config, date, headless) → [ScrapeResult]` | Single pipe OA+SG+ST with retry |
| `scrape_all` | `(configs, date, headless) → [ScrapeResult]` | All pipes concurrent (TaskGroup) |
| `scrape_metadata` | `(configs) → None` | HTTP download metadata CSVs |
| `scrape_nn` | `(configs, date, headless) → [ScrapeResult]` | All NN pipes concurrent (TaskGroup) |
| `_scrape_short_way_with_retry` | `(pc, date, headless) → None` | Retry wrapper (3x, exponential) |
| `_scrape_long_way_with_retry` | `(pc, date, headless) → None` | Retry wrapper (3x, exponential) |
| `_scrape_nn_with_retry` | `(nn_code, date, headless) → None` | Retry wrapper (3x, exponential) |
| `_scrape_short_way` | `(pc, date, headless) → None` | rtba.enbridge.com: OA + ST + SG |
| `_scrape_long_way` | `(pc, date, headless) → None` | infopost.enbridge.com: OA + ST + SG |
| `_download_oa` | `(page, iframe?) → None` | Click download link, save to OA_raw/ |
| `_download_st` | `(page, iframe?) → None` | Click download link, save to ST_raw/ |
| `_scrape_sg` | `(page, pipecode, date, iframe?) → None` | Iterate SG segments, save to SG_raw/ |
| `_sg_refresh_dump` | `(pipecode, page, text, date) → None` | Long-way SG segment fallback |
| `_scrape_nn_single` | `(nn_code, date, headless) → None` | Single NN pipe scrape |
| `_download_file_with_retry` | `(url, path) → None` | HTTP download with retry (3x) |
| `_download_file` | `(url, path) → None` | Raw HTTP GET to file (aiohttp) |

### 9.5 EnbridgeSilverMunger — `src/app/pipelines/enbridge/silver_munger.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `process` | `(raw_dir, silver_dir, type, df) → [str]` | Route to type-specific processor |
| `cleanse` | `(file_path, type, **kwargs) → None` | Route to type-specific cleanser |
| `_process_oa` | `(raw_dir, silver_dir, df) → [str]` | CSV→PQ (add PipeCode), then TaskGroup _cleanse_oa |
| `_cleanse_oa` | `(file, df) → None` | Parse dates/floats, map flows, RowType="OA", GFLOC |
| `_process_sg` | `(raw_dir, silver_dir, df) → [str]` | CSV→PQ (extract date from filename), discover segments, cleanse |
| `_cleanse_sg` | `(file, seg_df, pipe_df) → None` | TD1/TD2 split, capacity calc, RowType="SG" |
| `_process_st` | `(raw_dir, silver_dir, df) → [str]` | CSV→PQ (add PipeCode), then TaskGroup _cleanse_st |
| `_cleanse_st` | `(file, df) → None` | Same pipeline as OA, RowType="ST" |
| `_process_nn` | `(raw_dir, silver_dir, df) → [str]` | CSV→PQ (add PipeCode), then TaskGroup _cleanse_nn |
| `_cleanse_nn` | `(file, df) → None` | Parse dates, map flows, RowType="NN", GFLOC |
| `_process_meta` | `(raw_dir, df) → None` | Combine, dedupe, upsert Azure Table |
| `_sg_csv_to_parquet` | `(raw_dir, silver_dir) → [str]` | Extract pipe_code/eff_date/cycle from filename |
| `_discover_new_sg_segments` | `(sg_df, cfg_df, pipe_df) → [dict]` | Find station names not in SegmentConfigs |

### 9.6 EnbridgeGoldMunger — `src/app/pipelines/enbridge/gold_munger.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `merge` | `(silver_dirs: dict[RowType, Path]) → DataFrame` | Concat OA+SG+ST+NN, dedupe, fresh Timestamp |
| `discover_locations` | `(gold_df, seg_df, pipe_df) → None` | No-op — SG location discovery done in silver_munger |
| `clean_directories` | `() → None` | Cleanup downloads (disabled for testing) |

### 9.7 EnbridgePusher — `src/app/pipelines/enbridge/pusher.py`

| Method | Signature | Description |
|--------|-----------|-------------|
| `bronze_blob_path` | `(type, file) → str` | Construct bronze blob path (no pipe_code in path) |
| `silver_blob_path` | `(type, file) → str` | Construct silver blob path (no pipe_code in path) |
| `push_bronze` | `(dir, type) → None` | Upload all files in dir to bronze container |
| `push_silver` | `(dir, type) → None` | Upload all files in dir to silver container |
| `push_gold` | `(df) → None` | LakeMerge to Delta Lake (offloaded to thread) |
| `push_all` | `(munger, df, stats?) → None` | META bronze → parallel OA/SG/ST/NN push → gold merge → cleanup |
| `_push_dataset` | `(munger, df, type) → [DatasetDetail]` | Per-type bronze→silver→count, returns ADLS paths |

### 9.8 Core Modules

#### `core/browser.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `open_page` | `(headless, slow_mo) → AsyncContextManager[Page]` | Playwright browser with 20s timeout |
| `fill_date_box` | `(page, locator, date, fmt) → None` | Clear → backspace → type date → enter |
| `download_and_save` | `(page, selector, dir, name?) → Path` | Click download link, save file |

#### `core/cloud.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_blob_service_client` | `(conn?) → AsyncContextManager` | Azure Blob client |
| `upload_blob` | `(file, blob, container, client, tier) → None` | Upload single file |
| `push_directory` | `(dir, container, resolver, conn?, tier) → None` | Upload all files in dir |

#### `core/delta.py`

| Class/Method | Signature | Description |
|--------------|-----------|-------------|
| `DeltaLakeConfig` | `(BaseModel)` | Storage config with table_uri property |
| `LakeMerge.__init__` | `(df, parent_pipe, config?, optimize?)` | Load table, push, optionally prune |
| `LakeMerge._push_by_row_type` | `() → None` | Overwrite per RowType with predicate |
| `LakeMerge._prune_table` | `() → None` | Z-order + vacuum per EffGasMonth |

#### `core/azure_tables.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_table` | `(name, conn?) → ContextManager` | Azure Table client |
| `dump_pipe_configs` | `(dir, force?) → DataFrame?` | Cache + query PipeConfigs |
| `dump_segment_configs` | `(dir, force?) → DataFrame?` | Cache + query SegmentConfigs |
| `update_segment_configs` | `(df) → None` | Batch upsert to SegmentConfigs |

#### `core/transforms.py`

| Function | Signature | Description |
|----------|-----------|-------------|
| `batch_date_parse` | `(series, fmt) → Series` | Parse date strings |
| `batch_ymonth_parse` | `(series) → Series` | Date → YYYYMM int |
| `padded_string` | `(series, width=6) → Series` | Left-pad with zeros |
| `batch_float_parse` | `(series) → Series` | String with commas → float |
| `batch_absolute` | `(series) → Series` | Absolute value |
| `batch_fi_mapper` | `(series, map) → Series` | Flow indicator mapping |
| `compose_gfloc` | `(df) → LazyFrame` | Build GFLOC composite key |
| `add_modeling_columns` | `(df, type, parent) → LazyFrame` | Add EffGasMonth, GFLocID, RowType |

### 9.9 Base Types — `src/app/base/types.py`

| Type | Fields | Description |
|------|--------|-------------|
| `RowType` | `OA, SG, ST, NN, META` | StrEnum: OA=Operational Capacity, SG=Segment Capacity, ST=Storage Capacity, NN=No Notice Activity |
| `PipeConfig` | `pipe_code, parent_pipe, pipe_name, gf_pipe_id, oa_code, sg_code, st_code, nn_code, meta_code` | Pipeline config with computed `has_oa`, `has_sg`, `has_st`, `has_nn`, `has_meta` |
| `ScrapeResult` | `pipe_code, dataset_type, date, success, duration_s, error` | Single scrape outcome |
| `DatasetDetail` | `dataset_type, pipe_code, raw_records, silver_records, raw_paths, silver_paths, missing` | Push statistics with ADLS file URLs |
| `RunStats` | `pipeline, start_time, end_time, results, dataset_details` | Aggregated run stats with computed totals |

---

## Alternate Entry Points

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  scrapeToday ──▶ yesterday + today + NN → push → report    │
│                                                             │
│  scrapeSomeday(date) ──▶ single date + NN → push → report  │
│                                                             │
│  scrapeRange(start, end) ──▶ ProcessPool batches → push     │
│                                                             │
│  scrapeFailed ──▶ re-scrape from fails CSV → push → report │
│                                                             │
│  scrapeAllPipelines ──▶ loop registry → scrapeToday each    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

*Auto-generated architecture reference — GFScrapePackage*
