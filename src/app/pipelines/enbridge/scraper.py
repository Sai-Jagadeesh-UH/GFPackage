"""Enbridge scraper — implements BasePipeScraper.

Single-browser, single-page approach through infopost.enbridge.com.
Pipes and dataset availability are discovered live from the UI on every run —
no dependency on pre-loaded PipeConfigs or dataset code constants.

Scrape flow per run:
  1. Open one browser page, navigate to infopost root
  2. Discover all available pipes from "Select Business Unit" dropdown
  3. For each pipe (sequential, same page):
     a. Navigate to pipe via BU selector
     b. Discover which datasets are available (OA, SG, NN) from the nav/iframe
     c. Scrape MetaData → OA → SG → NN in order
     d. On unrecoverable failure: recreate page, continue from next pipe
  4. ST scraped where available (same OA page structure)

Retry: simple coroutine-based, 3 attempts with delay — no tenacity dependency.
Resilience > speed: sequential pipes, one page, new page only on hard failure.
"""

import asyncio
import pickle
import time
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright, Page, expect
from pydantic import BaseModel, Field

import pandas as pd

from app.base.scraper import BasePipeScraper
from app.base.types import ScrapeResult, RowType, PipeChange
from app.core.browser import BROWSER_TIMEOUT_MS
from app.core.azure_tables import dump_pipe_configs
from app.core.errors import track_fail
from app.core.logging import logger

from . import config as cfg

RETRY_ATTEMPTS = 3
RETRY_DELAY_S = 2.0


class Pipe(BaseModel, frozen=True):
    """A single Enbridge pipeline as discovered from the UI dropdown."""
    label: str = Field(min_length=2)   # full dropdown text e.g. "Algonquin (AGT)"
    name: str = Field(min_length=2)    # display name e.g. "Algonquin"
    code: str = Field(min_length=2)    # pipe code e.g. "AGT"


class EnbPipeConfig(BaseModel, frozen=True):
    """Per-pipe dataset availability — discovered live and cached daily."""
    pipe: Pipe
    has_OA: bool = False
    has_SG: bool = False
    has_NN: bool = False
    has_MetaData: bool = False


def _short_exc(exc: Exception) -> str:
    """First line of exception message only — avoids Playwright call log noise."""
    return str(exc).splitlines()[0]


async def _retry(coro_fn, label: str = "", attempts: int = RETRY_ATTEMPTS) -> None:
    """Retry a coroutine up to `attempts` times with fixed delay."""
    for attempt in range(1, attempts + 1):
        try:
            return await coro_fn()
        except Exception as exc:
            msg = _short_exc(exc)
            if attempt == attempts:
                logger.error(f"[{label}] failed after {attempts} attempts: {msg}")
                raise
            logger.warning(f"[{label}] attempt {attempt}/{attempts} failed: {msg} — retrying in {RETRY_DELAY_S}s")
            await asyncio.sleep(RETRY_DELAY_S)


class EnbridgeScraper(BasePipeScraper):
    """Scraper for all Enbridge pipeline data via infopost.enbridge.com."""

    def __init__(self, paths):
        self._paths = paths # root directory

    # ------------------------------------------------------------------
    # Availability cache — once per day, stored as pickle in config_files
    # ------------------------------------------------------------------

    @property
    def _avail_cache_path(self) -> Path:
        today = datetime.now().strftime("%Y-%m-%d")
        return self._paths.config_files / f"enbrdge_{today}_availability.pkl"

    def _load_avail_cache(self) -> list[EnbPipeConfig] | None:
        """Return today's cached list of EnbPipeConfig, or None if missing/stale."""
        p = self._avail_cache_path
        if not p.exists():
            return None
        # Stale if written before today
        if datetime.fromtimestamp(p.stat().st_mtime).date() < datetime.now().date():
            logger.info(f"Availability cache stale — will rebuild: {p.name}")
            return None
        try:
            configs: list[EnbPipeConfig] = pickle.loads(p.read_bytes())
            logger.info(f"Availability cache hit: {p.name} ({len(configs)} pipes)")
            return configs
        except Exception as exc:
            logger.warning(f"Availability cache read failed: {exc}")
            return None

    def _save_avail_cache(self, configs: list[EnbPipeConfig]) -> None:
        """Persist today's availability list to disk as pickle."""
        try:
            self._avail_cache_path.write_bytes(pickle.dumps(configs))
            logger.info(f"Availability cache saved: {self._avail_cache_path.name}")
        except Exception as exc:
            logger.warning(f"Availability cache write failed: {exc}")

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    @property
    def download_root(self) -> Path:
        return self._paths.downloads

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scrape_date(
        self,
        scrape_date: datetime,
        headless: bool = True,
        skip_meta: bool = False,
    ) -> tuple[list[ScrapeResult], list[PipeChange]]:
        """Discover all pipes + datasets live, scrape all on a single page.

        Returns (scrape_results, pipe_changes) where pipe_changes lists any
        new, removed, or renamed pipes vs the stored PipeConfigs.
        """
        # target_date = min(scrape_date, datetime.today())
        if scrape_date > datetime.today():
            logger.warning(f"Target date {scrape_date:%Y-%m-%d} is in the future — cannot be done")
            return [], []
        
        target_date : datetime = scrape_date
        

        results: list[ScrapeResult] = []
        pipe_changes: list[PipeChange] = []
        
        

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless, slow_mo=100)
            try:
                page = await self._new_page(browser)
                await _retry(lambda: page.goto(cfg.INFOPOST_URL), label="goto/infopost")

                pipes = await self._discover_pipes(page)
                logger.info(f"Discovered {len(pipes)} pipes: {[p.code for p in pipes]}")

                # pipe_changes = await asyncio.to_thread(self._check_pipe_changes, pipes)

                # Load or build availability cache
                pipe_configs: list[EnbPipeConfig] | None= await asyncio.to_thread(self._load_avail_cache)
                
                if pipe_configs is None:
                    logger.info("Building availability cache from live UI...")
                    pipe_configs = []
                    for pipe in pipes:
                        try:
                            await self._goto_pipe(page, pipe)
                            pc = await self._discover_datasets(page, pipe)
                            pipe_configs.append(pc)
                            logger.info(
                                f"  [{pipe.code}] OA={pc.has_OA} SG={pc.has_SG} NN={pc.has_NN} Meta={pc.has_MetaData}"
                            )
                            await _retry(lambda: page.goto(cfg.INFOPOST_URL), label="goto/infopost/avail-reset")
                        except Exception as exc:
                            logger.warning(f"  [{pipe.code}] availability check failed: {_short_exc(exc)}")
                            pipe_configs.append(EnbPipeConfig(pipe=pipe))
                    await asyncio.to_thread(self._save_avail_cache, pipe_configs)
                else:
                    logger.info("Using cached availability — skipping live discovery")

                for pc in pipe_configs:
                    if not any([pc.has_OA, pc.has_SG, pc.has_NN, pc.has_MetaData]):
                        logger.info(f"[{pc.pipe.code}] no datasets available — skipping")
                        continue
                    start = time.perf_counter()
                    try:
                        pipe_results = await self._scrape_pipe(page, pc, target_date, skip_meta=skip_meta)
                        results.extend(pipe_results)
                    except Exception as exc:
                        elapsed = time.perf_counter() - start
                        logger.error(f"[{pc.pipe.code}] unrecoverable failure ({_short_exc(exc)}) — creating new page")
                        track_fail(
                            self._paths.fail_file,
                            f"{pc.pipe.code}|ALL|{target_date:%Y/%m/%d}\n",
                        )
                        results.append(ScrapeResult(
                            pipe_code=pc.pipe.code,
                            dataset_type=RowType.OA,
                            date=target_date,
                            success=False,
                            duration_s=elapsed,
                            error=str(exc),
                        ))
                        try:
                            await page.close()
                        except Exception:
                            pass
                        page = await self._new_page(browser)

                    # Always reset to infopost root before next pipe
                    try:
                        await _retry(lambda: page.goto(cfg.INFOPOST_URL), label="goto/infopost/reset")
                    except Exception:
                        logger.error("Could not reach infopost — stopping scrape")
                        break
            finally:
                await browser.close()

        return results, pipe_changes

    async def scrape_metadata(self) -> None:
        """Scrape metadata (Locations download) for all pipes via browser."""
        start = time.perf_counter()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, slow_mo=100)
            try:
                page = await self._new_page(browser)
                await _retry(lambda: page.goto(cfg.INFOPOST_URL), label="goto/infopost/meta")

                pipes = await self._discover_pipes(page)

                for pipe in pipes:
                    try:
                        await _retry(
                            lambda: self._scrape_meta_single(page, pipe),
                            label=f"{pipe.code}/Meta",
                        )
                    except Exception as exc:
                        logger.warning(f"[{pipe.code}/Meta] skipping after retries: {exc}")
            finally:
                await browser.close()

        logger.info(f"metaDump completed in {time.perf_counter() - start:.2f}s")

    # ------------------------------------------------------------------
    # Per-pipe orchestration
    # ------------------------------------------------------------------

    async def _scrape_pipe(
        self, page: Page, pc: EnbPipeConfig, target_date: datetime, skip_meta: bool = False
    ) -> list[ScrapeResult]:
        """Navigate to pipe and scrape each available dataset in sequence."""
        results: list[ScrapeResult] = []
        pipe = pc.pipe

        await self._goto_pipe(page, pipe)

        # MetaData
        if pc.has_MetaData and not skip_meta:
            try:
                await _retry(
                    lambda: self._scrape_meta_single(page, pipe),
                    label=f"{pipe.code}/Meta",
                )
            except Exception as exc:
                logger.warning(f"[{pipe.code}/Meta] failed: {exc}")

        # OA
        if pc.has_OA:
            start = time.perf_counter()
            try:
                await _retry(
                    lambda: self._scrape_oa(page, pipe, target_date),
                    label=f"{pipe.code}/OA",
                )
                results.append(self._ok(pipe.code, RowType.OA, target_date, start))
            except Exception as exc:
                results.append(self._fail(pipe.code, RowType.OA, target_date, start, exc))
                track_fail(self._paths.fail_file, f"{pipe.code}|OA|{target_date:%Y/%m/%d}\n")

        # SG
        if pc.has_SG:
            start = time.perf_counter()
            try:
                await self._scrape_sg(page, pipe, target_date)
                results.append(self._ok(pipe.code, RowType.SG, target_date, start))
            except Exception as exc:
                results.append(self._fail(pipe.code, RowType.SG, target_date, start, exc))
                track_fail(self._paths.fail_file, f"{pipe.code}|SG|{target_date:%Y/%m/%d}\n")

        # NN
        if pc.has_NN:
            nn_date = min(target_date, datetime.today() - timedelta(days=cfg.NN_DATE_LAG_DAYS))
            start = time.perf_counter()
            try:
                await _retry(
                    lambda: self._scrape_nn(page, pipe, nn_date),
                    label=f"{pipe.code}/NN",
                )
                results.append(self._ok(pipe.code, RowType.NN, nn_date, start))
            except Exception as exc:
                results.append(self._fail(pipe.code, RowType.NN, nn_date, start, exc))
                track_fail(self._paths.fail_file, f"{pipe.code}|NN|{nn_date:%Y/%m/%d}\n")

        elapsed = sum(r.duration_s for r in results)
        logger.info(f"[{pipe.code}] done in {elapsed:.2f}s — {len(results)} datasets")
        return results

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------

    async def _discover_pipes(self, page: Page) -> list[Pipe]:
        """Read available pipes from 'Select Business Unit' dropdown."""
        await page.get_by_role("link", name="Select Business Unit").click()
        raw = await page.get_by_text("Algonquin (AGT)Bobcat Gas").all_inner_texts()
        selections = [x for x in raw[0].split("\n") if x.strip()]
        pipes = []
        for sel in selections:
            parts = sel.split("(", 1)
            if len(parts) == 2:
                name = parts[0].strip()
                code = parts[1].rstrip(")").strip()
                pipes.append(Pipe(label=sel, name=name, code=code))
        return pipes

    async def _discover_datasets(self, page: Page, pipe: Pipe) -> EnbPipeConfig:
        """Check the nav and iframe to see which datasets this pipe has."""
        has_oa = has_sg = has_nn = has_meta = False

        nav_options = (await page.locator("#navbar2").all_inner_texts())[0].split("\n")
        nav_options = [o for o in nav_options if o.strip()]

        if "Locations" in nav_options:
            has_meta = True

        if "Capacity" in nav_options:
            await page.get_by_role("link", name="Capacity").click()
            point_options_raw = await page.locator("ul.dropdown-menu:visible").all_inner_texts()
            point_options = [o for o in point_options_raw[0].split("\n") if o.strip()]

            if "Operationally Available" in point_options:
                await page.get_by_role("link", name="Operationally Available").click()
                iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame
                await (
                    iframe.locator("body")
                    .filter(has_not_text="Loading...")
                    .wait_for(timeout=15_000)
                )
                has_oa = (
                    await iframe.get_by_text("Operationally Available Capacity", exact=True).count() > 0
                    and await iframe.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).is_visible()
                )
                has_sg = await iframe.get_by_text(cfg.SG_MAPS_TEXT, exact=True).count() > 0

            # if "Storage" in point_options:
            #     has_st = True

            if "No-notice Meter Level Allocation" in point_options:
                await page.get_by_role("link", name="No-notice Meter Level Allocation").click()
                iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame
                await (
                    iframe.locator("body")
                    .filter(has_not_text="Loading...")
                    .wait_for(timeout=15_000)
                )
                # Extra wait: "no services" text renders a few seconds after loading spinner clears
                await asyncio.sleep(4)
                has_nn = (
                    await iframe.get_by_text(
                        "There are no no-notice services associated with this pipeline.",
                        exact=True,
                    ).count() == 0
                )

        return EnbPipeConfig(
            pipe=pipe,
            has_OA=has_oa,
            has_SG=has_sg,
            has_NN=has_nn,
            has_MetaData=has_meta,
        )

    # ------------------------------------------------------------------
    # Dataset scrapers
    # ------------------------------------------------------------------

    async def _scrape_meta_single(self, page: Page, pipe: Pipe) -> None:
        """Download location metadata CSV for one pipe."""
        await self._goto_pipe(page, pipe)
        await page.locator("body").wait_for(timeout=15_000)
        await page.get_by_role("link", name="Locations").click()
        async with page.expect_download() as dl_info:
            await page.get_by_role("link", name="Location Data Download").click()
        download = await dl_info.value
        await download.save_as(
            self._paths.meta_raw / f"{pipe.code}_{download.suggested_filename}"
        )
        logger.info(f"[{pipe.code}/Meta] saved: {download.suggested_filename}")

    async def _scrape_oa(self, page: Page, pipe: Pipe, target_date: datetime) -> None:
        """Download OA CSV for one pipe."""
        await self._goto_pipe(page, pipe)
        await page.get_by_role("link", name="Capacity").click()
        await page.get_by_role("link", name="Operationally Available").click()

        iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame
        await (
            iframe.locator("body")
            .filter(has_not_text="Loading...")
            .wait_for(timeout=15_000)
        )
        await self._choose_date(page, target_date, dataset="oa")

        async with page.expect_download() as dl_info:
            await iframe.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).click()
        download = await dl_info.value
        await download.save_as(
            self._paths.oa_raw / f"{pipe.code}_{download.suggested_filename}"
        )
        logger.info(f"[{pipe.code}/OA] saved: {download.suggested_filename}")


    async def _scrape_sg(self, page: Page, pipe: Pipe, target_date: datetime) -> None:
        """Download all SG segment CSVs for one pipe."""
        await self._goto_pipe(page, pipe)
        await page.get_by_role("link", name="Capacity").click()
        await page.get_by_role("link", name="Operationally Available").click()

        iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame
        await (
            iframe.locator("body")
            .filter(has_not_text="Loading...")
            .wait_for(timeout=15_000)
        )
        await iframe.locator("#divCapacityMap").wait_for(timeout=5_000)

        sg_options_raw = await iframe.locator("#divCapacityMap").all_inner_texts()
        if not sg_options_raw:
            logger.warning(f"[{pipe.code}/SG] no segments found")
            return

        segments = [s.strip() for s in sg_options_raw[0].strip().split("\n") if s.strip()][1:]
        logger.info(f"[{pipe.code}/SG] {len(segments)} segments: {segments}")

        for segment in segments:
            try:
                await self._scrape_sg_segment(page, pipe, segment, target_date)
            except Exception as exc:
                logger.error(f"[{pipe.code}/SG/'{segment}'] failed: {_short_exc(exc)}")
                track_fail(self._paths.fail_file, f"{pipe.code}|SG|{target_date:%Y/%m/%d}\n")
            # Return to OA page for next segment
            try:
                await page.get_by_role("link", name="Operationally Available").click()
                await (
                    iframe.locator("body")
                    .filter(has_not_text="Loading...")
                    .wait_for(timeout=15_000)
                )
            except Exception:
                pass

    async def _scrape_sg_segment(
        self, page: Page, pipe: Pipe, segment: str, target_date: datetime
    ) -> None:
        """Download one SG segment CSV."""
        iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame

        await iframe.get_by_role("link", name=segment).click()
        await self._choose_date(page, target_date, dataset="oa")

        # First click to trigger — check for "no data" error before expecting download
        await iframe.get_by_role("link", name=cfg.DOWNLOAD_CSV_TEXT).click()
        try:
            await expect(iframe.locator("#divList #spanError")).to_be_visible(timeout=1_000)
            logger.info(f"[{pipe.code}/SG/'{segment}'] no data for {target_date:%Y-%m-%d}")
            await page.get_by_role("link", name="Operationally Available").click() 
            return
        except Exception:
            pass

        # No error → click again to download
        async with page.expect_download() as dl_info:
            await iframe.get_by_role("link", name=cfg.DOWNLOAD_CSV_TEXT).click()
        download = await dl_info.value
        clean_segment = segment.replace("/", "").replace(" ", "_")
        filename = f"{pipe.code}_SG_{clean_segment}_{download.suggested_filename}"
        await download.save_as(self._paths.sg_raw / filename)
        logger.info(f"[{pipe.code}/SG/{segment}] saved: {filename}")

    async def _scrape_nn(self, page: Page, pipe: Pipe, target_date: datetime) -> None:
        """Download NN (No-Notice) CSV for one pipe."""
        await self._goto_pipe(page, pipe)
        await page.get_by_role("link", name="Capacity").click()
        await page.get_by_role("link", name="No-notice Meter Level Allocation").click()

        iframe = page.locator(cfg.IFRAME_SELECTOR).content_frame
        await (
            iframe.locator("body")
            .filter(has_not_text="Loading...")
            .wait_for(timeout=15_000)
        )
        await self._choose_date(page, target_date, dataset="nn")

        async with page.expect_download() as dl_info:
            await iframe.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).click()
        download = await dl_info.value
        await download.save_as(
            self._paths.nn_raw / f"{pipe.code}_{download.suggested_filename}"
        )
        logger.info(f"[{pipe.code}/NN] saved: {download.suggested_filename}")

    # ------------------------------------------------------------------
    # Pipe config change detection
    # ------------------------------------------------------------------

    # def _check_pipe_changes(self, live_pipes: list[Pipe]) -> list[PipeChange]:
    #     """Compare live discovered pipes against stored PipeConfigs.

    #     Detects: new pipes (in UI but not in config), removed pipes (in config
    #     but not in UI), and renamed pipes (same code, different name).
    #     Logs all changes and returns the list for reporting.
    #     """
    #     config_df = dump_pipe_configs(self._paths.config_files)
    #     if config_df is None:
    #         logger.warning("PipeConfigs unavailable — skipping pipe change check")
    #         return []

    #     enb_cfg = config_df[config_df["ParentPipe"] == cfg.PARENT_PIPE]
    #     config_map: dict[str, str] = dict(zip(
    #         enb_cfg["PipeCode"].str.upper(),
    #         enb_cfg["PipeName"],
    #     ))
    #     live_map: dict[str, str] = {p.code.upper(): p.name for p in live_pipes}

    #     changes: list[PipeChange] = []

    #     for code, live_name in live_map.items():
    #         if code not in config_map:
    #             changes.append(PipeChange(change_type="new", pipe_code=code, live_name=live_name))
    #             logger.warning(f"[PipeCheck] NEW pipe discovered: {code} — '{live_name}'")
    #         elif config_map[code].strip() != live_name.strip():
    #             changes.append(PipeChange(
    #                 change_type="renamed",
    #                 pipe_code=code,
    #                 live_name=live_name,
    #                 config_name=config_map[code],
    #             ))
    #             logger.warning(f"[PipeCheck] RENAMED: {code} — '{config_map[code]}' → '{live_name}'")

    #     for code, config_name in config_map.items():
    #         if code not in live_map:
    #             changes.append(PipeChange(change_type="removed", pipe_code=code, config_name=config_name))
    #             logger.warning(f"[PipeCheck] REMOVED pipe (no longer in UI): {code} — '{config_name}'")

    #     if not changes:
    #         logger.info("[PipeCheck] No pipe changes detected")

    #     return changes

    # ------------------------------------------------------------------
    # Navigation helpers
    # ------------------------------------------------------------------

    async def _goto_pipe(self, page: Page, pipe: Pipe) -> None:
        """Navigate to a pipe via the 'Select Business Unit' dropdown."""
        await page.get_by_role("link", name="Select Business Unit").click()
        await page.get_by_role("link", name=pipe.label).click()

    async def _choose_date(self, page: Page, target_date: datetime, dataset: str = "oa") -> None:
        """Fill the date input in the iframe using its ID selector."""
        date_input = page.locator(cfg.IFRAME_SELECTOR).content_frame.locator(
            f"#ctl00_MainContent_ctl01_{dataset}Default_ucDate_rdpDate_dateInput"
        )
        await date_input.click()
        await date_input.fill(target_date.strftime("%m/%d/%Y"))
        await date_input.press("Enter")

    @staticmethod
    async def _new_page(browser):
        """Create a new page with standard timeouts."""
        page = await browser.new_page()
        page.set_default_timeout(BROWSER_TIMEOUT_MS)
        page.set_default_navigation_timeout(BROWSER_TIMEOUT_MS)
        return page

    # ------------------------------------------------------------------
    # ScrapeResult helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ok(pipe_code: str, dataset_type: RowType, date: datetime, start: float) -> ScrapeResult:
        return ScrapeResult(
            pipe_code=pipe_code,
            dataset_type=dataset_type,
            date=date,
            success=True,
            duration_s=time.perf_counter() - start,
        )

    @staticmethod
    def _fail(pipe_code: str, dataset_type: RowType, date: datetime, start: float, exc: Exception) -> ScrapeResult:
        return ScrapeResult(
            pipe_code=pipe_code,
            dataset_type=dataset_type,
            date=date,
            success=False,
            duration_s=time.perf_counter() - start,
            error=str(exc),
        )
