"""Enbridge scraper — implements BasePipeScraper.

Handles OA, SG, ST, NN, and metadata scraping using Playwright browser
automation against rtba.enbridge.com and infopost.enbridge.com.

Scrape flow per pipe:
  1. Try short-way (rtba) for OA + SG + ST
  2. On failure, fall back to long-way (infopost) for OA + SG + ST
  3. NN scraped separately (concurrent, different URL pattern)
  4. Metadata downloaded via direct HTTP (no browser needed)

Retry strategy: tenacity with 3 attempts, exponential backoff (2s base).
Browser timeout: 20s (fail-fast).
"""

import asyncio
import ssl
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiofiles
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from playwright.async_api import TimeoutError as PlaywrightTimeout

from app.base.scraper import BasePipeScraper
from app.base.types import PipeConfig, ScrapeResult, RowType
from app.core.browser import open_page, fill_date_box
from app.core.errors import track_fail
from app.core.logging import logger

from . import config as cfg

# Retry config — 3 attempts with exponential backoff (2s, 4s)
RETRY_KWARGS = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((PlaywrightTimeout, TimeoutError, ConnectionError)),
    reraise=True,
)


class EnbridgeScraper(BasePipeScraper):
    """Scraper for all Enbridge pipeline data."""

    def __init__(self, paths):
        """
        Args:
            paths: PipelinePaths instance with download directories.
        """
        self._paths = paths
        self._browser_sem = asyncio.Semaphore(4)

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    @property
    def download_root(self) -> Path:
        return self._paths.downloads

    # ------------------------------------------------------------------
    # Public API (BasePipeScraper contract)
    # ------------------------------------------------------------------

    async def scrape(
        self,
        pipe_config: PipeConfig,
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape OA, SG, ST, and NN for a single pipe on a single date."""
        target_date = min(scrape_date, datetime.today())
        results: list[ScrapeResult] = []

        start = time.perf_counter()

        # Route long-way-only pipes directly (no short-way attempt)
        if pipe_config.pipe_code in cfg.LONG_WAY_ONLY_CODES:
            logger.info(f"{pipe_config.pipe_code} → long-way (configured)")
            try:
                await self._scrape_long_way_with_retry(pipe_config, target_date, headless)
                results.append(ScrapeResult(
                    pipe_code=pipe_config.pipe_code,
                    dataset_type=RowType.OA,
                    date=target_date,
                    success=True,
                    duration_s=time.perf_counter() - start,
                ))
            except Exception as e:
                elapsed = time.perf_counter() - start
                logger.error(f"{pipe_config.pipe_code} | OA long-way failed after retries: {e}")
                track_fail(
                    self._paths.fail_file,
                    f"{pipe_config.pipe_code}|OA|{target_date:%Y/%m/%d}\n",
                )
                results.append(ScrapeResult(
                    pipe_code=pipe_config.pipe_code,
                    dataset_type=RowType.OA,
                    date=target_date,
                    success=False,
                    duration_s=elapsed,
                    error=str(e),
                ))
            return results

        try:
            await self._scrape_short_way_with_retry(pipe_config, target_date, headless)
            results.append(ScrapeResult(
                pipe_code=pipe_config.pipe_code,
                dataset_type=RowType.OA,
                date=target_date,
                success=True,
                duration_s=time.perf_counter() - start,
            ))
        except Exception as e:
            logger.warning(
                f"{pipe_config.pipe_code} short-way failed ({e}), trying long way"
            )
            try:
                await self._scrape_long_way_with_retry(pipe_config, target_date, headless)
                results.append(ScrapeResult(
                    pipe_code=pipe_config.pipe_code,
                    dataset_type=RowType.OA,
                    date=target_date,
                    success=True,
                    duration_s=time.perf_counter() - start,
                ))
            except Exception as e2:
                elapsed = time.perf_counter() - start
                logger.error(
                    f"{pipe_config.pipe_code} | OA failed both paths after retries: {e2}"
                )
                track_fail(
                    self._paths.fail_file,
                    f"{pipe_config.pipe_code}|OA|{target_date:%Y/%m/%d}\n",
                )
                results.append(ScrapeResult(
                    pipe_code=pipe_config.pipe_code,
                    dataset_type=RowType.OA,
                    date=target_date,
                    success=False,
                    duration_s=elapsed,
                    error=str(e2),
                ))

        return results

    async def scrape_all(
        self,
        pipe_configs: list[PipeConfig],
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape OA/SG/ST for all pipes with at most 4 concurrent browsers."""
        target_date = min(scrape_date, datetime.today())

        eligible = [
            pc for pc in pipe_configs
            if (pc.has_oa or pc.has_sg or pc.has_st)
            and not (
                pc.pipe_code == "WE"
                and scrape_date < datetime.strptime(cfg.WE_AVAILABILITY_DATE, "%Y-%m-%d")
            )
        ]

        async def _scrape_one(pc: PipeConfig) -> list[ScrapeResult]:
            async with self._browser_sem:
                return await self.scrape(pc, target_date, headless)

        results: list[ScrapeResult] = []
        raw = await asyncio.gather(*[_scrape_one(pc) for pc in eligible], return_exceptions=True)
        for item in raw:
            if isinstance(item, BaseException):
                logger.error(f"scrape_all task failed: {item}")
            else:
                results.extend(item)

        return results

    async def scrape_metadata(self, pipe_configs: list[PipeConfig]) -> None:
        """Download metadata CSV files for all pipes with meta_code."""
        start = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            for pc in pipe_configs:
                if not pc.has_meta:
                    continue
                url = cfg.metadata_url(pc.meta_code)  # type: ignore
                local_path = self._paths.meta_raw / f"{pc.pipe_code}_AllPoints.csv"
                group.create_task(self._download_file_with_retry(url, local_path))

        logger.info(f"metaDump completed in {time.perf_counter() - start:.2f}s")

    # ------------------------------------------------------------------
    # NN scraping
    # ------------------------------------------------------------------

    async def scrape_nn(
        self,
        pipe_configs: list[PipeConfig],
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape NoNotice data for all pipes with nn_code.

        NN data lags by NN_DATE_LAG_DAYS days.
        Bounded by the shared browser semaphore (max 4 concurrent browsers).
        """
        target_date = min(
            scrape_date,
            datetime.today() - timedelta(days=cfg.NN_DATE_LAG_DAYS),
        )
        nn_codes = [pc for pc in pipe_configs if pc.has_nn]

        async def _scrape_one(pc: PipeConfig) -> ScrapeResult:
            async with self._browser_sem:
                return await self._scrape_nn_single_tracked(pc, target_date, headless)

        raw = await asyncio.gather(*[_scrape_one(pc) for pc in nn_codes], return_exceptions=True)
        results: list[ScrapeResult] = []
        for item in raw:
            if isinstance(item, BaseException):
                logger.error(f"scrape_nn task failed: {item}")
            else:
                results.append(item)

        return results

    async def _scrape_nn_single_tracked(
        self, pc: PipeConfig, target_date: datetime, headless: bool
    ) -> ScrapeResult:
        """Scrape NN for a single pipe, returning a ScrapeResult."""
        start = time.perf_counter()
        try:
            await self._scrape_nn_with_retry(pc.nn_code, target_date, headless)  # type: ignore
            elapsed = time.perf_counter() - start
            logger.info(f"{pc.nn_code} - {elapsed:.2f}s {'-' * 15}")
            return ScrapeResult(
                pipe_code=pc.pipe_code,
                dataset_type=RowType.NN,
                date=target_date,
                success=True,
                duration_s=elapsed,
            )
        except Exception as e:
            elapsed = time.perf_counter() - start
            track_fail(
                self._paths.fail_file,
                f"{pc.nn_code}|NN|{target_date:%Y/%m/%d}\n",
            )
            logger.error(f"{pc.nn_code} | NN scrape failed after retries: {e}")
            logger.info(f"{pc.nn_code} - {elapsed:.2f}s {'-' * 15}")
            return ScrapeResult(
                pipe_code=pc.pipe_code,
                dataset_type=RowType.NN,
                date=target_date,
                success=False,
                duration_s=elapsed,
                error=str(e),
            )

    # ------------------------------------------------------------------
    # Retry wrappers
    # ------------------------------------------------------------------

    async def _scrape_short_way_with_retry(
        self, pc: PipeConfig, target_date: datetime, headless: bool
    ) -> None:
        @retry(**RETRY_KWARGS, before_sleep=lambda rs: logger.info(
            f"{pc.pipe_code} short-way retry {rs.attempt_number}/3"
        ))
        async def _attempt():
            await self._scrape_short_way(pc, target_date, headless)
        await _attempt()

    async def _scrape_long_way_with_retry(
        self, pc: PipeConfig, target_date: datetime, headless: bool
    ) -> None:
        @retry(**RETRY_KWARGS, before_sleep=lambda rs: logger.info(
            f"{pc.pipe_code} long-way retry {rs.attempt_number}/3"
        ))
        async def _attempt():
            await self._scrape_long_way(pc, target_date, headless)
        await _attempt()

    async def _scrape_nn_with_retry(
        self, nn_code: str, target_date: datetime, headless: bool
    ) -> None:
        @retry(**RETRY_KWARGS, before_sleep=lambda rs: logger.info(
            f"{nn_code} NN retry {rs.attempt_number}/3"
        ))
        async def _attempt():
            await self._scrape_nn_single(nn_code, target_date, headless)
        await _attempt()

    # ------------------------------------------------------------------
    # Private: short-way scrape (rtba.enbridge.com)
    # ------------------------------------------------------------------

    async def _scrape_short_way(
        self, pc: PipeConfig, target_date: datetime, headless: bool
    ) -> None:
        async with open_page(headless=headless) as page:
            await page.goto(cfg.rtba_url(pc.pipe_code, "OA"))

            date_box = (
                page.get_by_text(cfg.DATE_BOX_LABEL)
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )
            await fill_date_box(page, date_box, target_date)

            # OA download
            if pc.has_oa:
                await self._download_oa(page)

            # ST (Storage Capacity) download — same structure as OA, different URL type
            if pc.has_st:
                await page.goto(cfg.rtba_url(pc.pipe_code, "ST"))
                st_date_box = (
                    page.get_by_text(cfg.DATE_BOX_LABEL)
                    .locator("xpath=./following-sibling::div")
                    .get_by_role("textbox")
                    .nth(0)
                )
                await fill_date_box(page, st_date_box, target_date)
                await self._download_st(page)

            # SG (Segment Capacity) download
            if pc.has_sg:
                sg_maps = page.get_by_text(cfg.SG_MAPS_TEXT)
                if sg_maps:
                    await self._scrape_sg(
                        mainpage=page,
                        pipecode=pc.pipe_code,
                        scrape_date=target_date,
                    )
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Private: long-way scrape (infopost.enbridge.com)
    # ------------------------------------------------------------------

    async def _scrape_long_way(
        self, pc: PipeConfig, target_date: datetime, headless: bool
    ) -> None:
        async with open_page(headless=headless) as page:
            await page.goto(cfg.infopost_home_url(pc.pipe_code))
            await page.locator(cfg.CAPACITY_MENU_SELECTOR).click()
            await page.get_by_role("link", name=cfg.OA_LINK_NAME).nth(0).click()

            iframe = page.frame_locator(cfg.IFRAME_SELECTOR)

            date_box = (
                iframe.get_by_text(cfg.DATE_BOX_LABEL)
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )
            await fill_date_box(page, date_box, target_date)

            if pc.has_oa:
                await self._download_oa(page, iframe)

            if pc.has_st:
                await self._download_st(page, iframe)

            if pc.has_sg:
                sg_maps = iframe.get_by_text(cfg.SG_MAPS_TEXT)
                if sg_maps:
                    await self._scrape_sg(
                        mainpage=page,
                        pipecode=pc.pipe_code,
                        scrape_date=target_date,
                        iframe=iframe,
                    )
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Private: OA download helper
    # ------------------------------------------------------------------

    async def _download_oa(self, page, iframe=None) -> None:
        """Download OA CSV via the 'Downloadable Format' link."""
        try:
            frame = iframe or page
            async with page.expect_download() as download_info:
                await frame.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).click()

            download = await download_info.value
            await download.save_as(
                self._paths.oa_raw / download.suggested_filename
            )
        except Exception:
            logger.error("OA download failed")
            raise ValueError("OA download failed")

    async def _download_st(self, page, iframe=None) -> None:
        """Download ST (Storage Capacity) CSV via the 'Downloadable Format' link."""
        try:
            frame = iframe or page
            async with page.expect_download() as download_info:
                await frame.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).click()

            download = await download_info.value
            await download.save_as(
                self._paths.st_raw / download.suggested_filename
            )
        except Exception:
            logger.error("ST download failed")
            raise ValueError("ST download failed")

    # ------------------------------------------------------------------
    # Private: SG scrape (multiple segments)
    # ------------------------------------------------------------------

    async def _scrape_sg(
        self, mainpage, pipecode: str, scrape_date: datetime, iframe=None
    ) -> None:
        """Scrape all SG segments — short-way first, fallback to refresh per segment."""
        try:
            frame = iframe or mainpage

            div_items = (
                frame.get_by_text(cfg.SG_MAPS_TEXT)
                .locator("xpath=./following-sibling::div")
                .get_by_role("link")
            )
            text_list = await div_items.all_text_contents()

            for count, ele_text in enumerate(text_list, start=1):
                try:
                    if iframe is not None:
                        raise ValueError("longway detected")

                    child_element = mainpage.get_by_role("link").get_by_text(ele_text)
                    await asyncio.sleep(1)

                    async with mainpage.expect_navigation():
                        await child_element.click()

                    async with mainpage.expect_download() as download_info:
                        await mainpage.get_by_text(cfg.DOWNLOAD_CSV_TEXT).click()

                    download = await download_info.value
                    filename = (
                        pipecode
                        + "_"
                        + download.suggested_filename.split("_", 1)[1]
                    ).replace("_OA_", f"_SG{count}_")

                    await download.save_as(self._paths.sg_raw / filename)
                    await asyncio.sleep(1)

                    async with mainpage.expect_navigation():
                        await mainpage.go_back()

                except Exception:
                    if ele_text in cfg.SG_SKIP_SEGMENTS:
                        pass
                    else:
                        for remaining_text in text_list[max(0, count - 1):]:
                            await self._sg_refresh_dump(
                                pipecode, mainpage, remaining_text, scrape_date
                            )
                            await asyncio.sleep(1)
                        break

        except Exception as e:
            track_fail(
                self._paths.fail_file,
                f"{pipecode}|SG|{scrape_date:%Y/%m/%d}\n",
            )
            logger.error(f"{pipecode} | SG scrape failed: {e}")

    async def _sg_refresh_dump(
        self, pipecode: str, mainpage, get_by_text: str, scrape_date: datetime
    ) -> None:
        """Long-way SG download: navigate from home, click segment, download CSV."""
        target_date = min(scrape_date, datetime.today())

        try:
            await mainpage.goto(cfg.infopost_home_url(pipecode))
            await mainpage.locator(cfg.CAPACITY_MENU_SELECTOR).click()
            await mainpage.get_by_role("link", name=cfg.OA_LINK_NAME).nth(0).click()

            iframe = mainpage.frame_locator(cfg.IFRAME_SELECTOR)

            date_box = (
                iframe.get_by_text(cfg.DATE_BOX_LABEL)
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )
            await fill_date_box(mainpage, date_box, target_date)

            child_element = iframe.get_by_role("link").get_by_text(get_by_text)
            await child_element.click()

            async with mainpage.expect_download() as download_info:
                await iframe.get_by_text(cfg.DOWNLOAD_CSV_TEXT).click()

            download = await download_info.value
            filename = (
                pipecode
                + "_"
                + download.suggested_filename.split("_", 1)[1]
            ).replace(
                "_OA_",
                f"_SG{get_by_text.replace('/', '').replace(' ', '')}_",
            )

            await download.save_as(self._paths.sg_raw / filename)
            await asyncio.sleep(1)

        except Exception as e:
            track_fail(
                self._paths.fail_file,
                f"{pipecode}|SG|{target_date:%Y/%m/%d}\n",
            )
            logger.error(f"{pipecode} | SG segment '{get_by_text}' failed: {e}")

    # ------------------------------------------------------------------
    # Private: NN scrape
    # ------------------------------------------------------------------

    async def _scrape_nn_single(
        self, nn_code: str, target_date: datetime, headless: bool
    ) -> None:
        """Scrape NN data for a single pipe code."""
        async with open_page(headless=headless) as page:
            await page.goto(cfg.rtba_url(nn_code, "NN"))

            date_box = (
                page.get_by_text(cfg.DATE_BOX_LABEL)
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )

            if date_box:
                await fill_date_box(page, date_box, target_date)

            async with page.expect_download() as download_info:
                await page.get_by_role("link", name=cfg.DOWNLOAD_LINK_NAME).click()

            download = await download_info.value
            await download.save_as(
                self._paths.nn_raw / download.suggested_filename
            )

    # ------------------------------------------------------------------
    # Private: HTTP file download (metadata)
    # ------------------------------------------------------------------

    async def _download_file_with_retry(self, url: str, local_path: Path) -> None:
        """Download with retry — 3 attempts, exponential backoff."""
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=2, max=10),
            retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
            reraise=True,
            before_sleep=lambda rs: logger.info(
                f"Meta download retry {rs.attempt_number}/3: {url}"
            ),
        )
        async def _attempt():
            await self._download_file(url, local_path)
        await _attempt()

    @staticmethod
    async def _download_file(url: str, local_path: Path) -> None:
        """Download a file via HTTP with SSL verification disabled."""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=ssl_context)
            ) as session:
                async with session.get(url, raise_for_status=True) as response:
                    async with aiofiles.open(local_path, mode="wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
        except aiohttp.ClientError:
            logger.error(f"Download failed: {url}")
            raise
