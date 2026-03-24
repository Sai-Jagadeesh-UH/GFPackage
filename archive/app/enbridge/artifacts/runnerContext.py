from contextlib import asynccontextmanager

from playwright.async_api import async_playwright

from .BaseLogWriters import baseLogger
from src.artifacts import error_detailed


@asynccontextmanager
async def openPage(headLess: bool = True, slow_mo: int = 100):
    '''
    context manager for aync Playwright invkes browser instance and handles graceful closing of browser instance

    :type headLess: bool = True
    :type slow_mo: int = 100
    '''
    async with async_playwright() as playwright:
        chromium = playwright.chromium
        browser = await chromium.launch(headless=headLess, slow_mo=100)

        try:
            page = await browser.new_page()
            # page.set_default_navigation_timeout(10000)
            # page.set_default_timeout(20000)
            yield page
        except Exception as e:
            baseLogger.error(f"Error in contextManager: {error_detailed(e)}")
        finally:
            await browser.close()
