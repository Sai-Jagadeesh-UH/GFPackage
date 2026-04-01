from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, Page, Locator

from .logging import logger

# Browser timeout for navigation and actions (ms)
BROWSER_TIMEOUT_MS = 20_000


@asynccontextmanager
async def open_page(headless: bool = True, slow_mo: int = 100):
    """Async context manager for a Playwright Chromium page.

    Launches a browser, yields a Page, and ensures cleanup.
    Sets default timeout to BROWSER_TIMEOUT_MS (20s) for fail-fast.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, slow_mo=slow_mo)
        try:
            page = await browser.new_page()
            page.set_default_timeout(BROWSER_TIMEOUT_MS)
            page.set_default_navigation_timeout(BROWSER_TIMEOUT_MS)
            yield page
        finally:
            await browser.close()


# async def fill_date_box(
#     page: Page,
#     date_box: Locator,
#     target_date: datetime,
#     date_format: str = "%m/%d/%Y",
# ) -> None:
#     """Clear and fill a date input field, then press Enter.

#     This pattern is used across all Enbridge scrapers:
#     clear the field, backspace to remove residual characters,
#     type the formatted date, and submit.
#     """
#     await date_box.fill("")
#     for _ in range(10):
#         await page.keyboard.press("Backspace", delay=100)
#     await date_box.fill(target_date.strftime(date_format))
#     await page.keyboard.press("Enter")


# async def download_and_save(
#     page_or_frame,
#     link_selector: dict,
#     save_dir: Path,
#     filename: str | None = None,
# ) -> Path:
#     """Click a download link and save the file.

#     Args:
#         page_or_frame: Playwright Page or FrameLocator to trigger download from.
#         link_selector: kwargs for get_by_role, e.g. {"role": "link", "name": "Downloadable Format"}
#         save_dir: directory to save the downloaded file.
#         filename: override filename; if None uses the browser's suggested name.

#     Returns:
#         Path to the saved file.
#     """
#     parent = page_or_frame
#     # If this is a FrameLocator, we need the parent page for expect_download
#     if hasattr(page_or_frame, "page"):
#         parent = page_or_frame.page

#     async with parent.expect_download() as download_info:
#         await page_or_frame.get_by_role(**link_selector).click()

#     download = await download_info.value
#     save_path = save_dir / (filename or download.suggested_filename)
#     await download.save_as(save_path)
#     return save_path
