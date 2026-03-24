
from ..utils import paths
from ..utils import logger, error_detailed

oa_downloads_path = paths.downloads / 'OA_raw'
oa_downloads_path.mkdir(exist_ok=True, parents=True)


async def scrape_OA(page, iframe=None):

    # raises error on failure caught and handles by caller
    try:
        if not iframe:
            async with page.expect_download() as download_info:
                await page.get_by_role("link", name="Downloadable Format").click()
        else:
            async with page.expect_download() as download_info:
                await iframe.get_by_role("link", name="Downloadable Format").click()

        download = await download_info.value

        await download.save_as(oa_downloads_path / download.suggested_filename)
    except Exception as e:
        logger.error(f"OA failed {error_detailed(e)}")
        raise ValueError("OA failed ")
