import asyncio

from datetime import datetime

from ..utils import paths, error_detailed, trackFail
from ..utils import logger


ocap_downloads_path = paths.downloads / 'OC_raw'
ocap_downloads_path.mkdir(exist_ok=True, parents=True)


async def refreshDump(pipecode: str, mainpage, getByText: str, scrape_date: datetime):

    # today or past else D day
    target_date = scrape_date if scrape_date <= datetime.today() else datetime.now()

    try:
        await mainpage.goto(
            f"https://infopost.enbridge.com/InfoPost/{pipecode}Home.asp?Pipe={pipecode}"
        )

        await mainpage.locator("li#Capacity.dropdown.sidebar-menu-item").click()

        await mainpage.get_by_role("link", name="Operationally Available").nth(0).click()

        iframe_locator = mainpage.frame_locator("#ContentPaneIframe")

        date_box = (
            iframe_locator.get_by_text("Gas Date: ")
            .locator("xpath=./following-sibling::div")
            .get_by_role("textbox")
            .nth(0)
        )

        await date_box.fill("")
        for i in range(10):
            await mainpage.keyboard.press("Backspace", delay=100)

        await date_box.fill((target_date).strftime("%m/%d/%Y"))

        await mainpage.keyboard.press("Enter")

        child_element = iframe_locator.get_by_role(
            "link").get_by_text(getByText)

        # await child_element.highlight()

        await child_element.click()

        async with mainpage.expect_download() as download_info:
            await iframe_locator.get_by_text("Download Csv").click()

        download = await download_info.value

        fileName = (pipecode+'_' + download.suggested_filename.split('_', 1)
                    [1]).replace('_OA_', f'_OC{getByText.replace('/', '').replace(' ', '')}_')

        await download.save_as(ocap_downloads_path / fileName)

        await asyncio.sleep(1)

    except Exception as e:
        trackFail(f"{pipecode}|OC|{target_date.strftime("%Y/%m/%d")}\n")
        logger.critical(
            f"{pipecode} | OC | {getByText} | {target_date.strftime("%Y/%m/%d")}")
        logger.error(f"""{pipecode} | OC | {getByText} | {target_date.strftime("%Y/%m/%d")}
                     - {error_detailed(e)}""")


async def scrape_OC(mainpage, pipecode: str, scrape_date: datetime, iframe=None, ):
    try:
        if iframe is None:
            page = mainpage
            frame = mainpage
        else:
            page = mainpage
            frame = iframe

        # get all OC links
        div_items = (
            frame.get_by_text("Operational Capacity Maps")
            .locator("xpath=./following-sibling::div")
            .get_by_role("link")
        )

        textList = await div_items.all_text_contents()

        for count, eleText in enumerate(textList, start=1):
            try:
                if (iframe is not None):
                    # kick it iterative scrape
                    raise ValueError("longway detected")

                child_element = page.get_by_role("link").get_by_text(eleText)

                # await child_element.highlight()
                # time.sleep(1)
                await asyncio.sleep(1)

                async with page.expect_navigation():
                    await child_element.click()

                async with page.expect_download() as download_info:
                    await page.get_by_text("Download Csv").click()

                download = await download_info.value

                fileName = (pipecode+'_' + download.suggested_filename.split('_', 1)
                            [1]).replace('_OA_', f'_OC{count}_')

                await download.save_as(ocap_downloads_path / fileName)

                await asyncio.sleep(1)

                async with page.expect_navigation():
                    await page.go_back()

            except Exception:
                # logger.error(f"Failed: in shortcut - {i}")
                if (eleText in ['TETLP Lease NJ/NY']):
                    pass
                else:
                    for eleText in textList[max(0, count-1):]:
                        # try long path only remaining OC's
                        await refreshDump(pipecode=pipecode, mainpage=page, getByText=eleText, scrape_date=scrape_date)
                        await asyncio.sleep(1)
                    break
    except Exception as e:
        trackFail(f"{pipecode}|OC|{scrape_date.strftime("%Y/%m/%d")}\n")
        logger.critical(
            f"{pipecode} | OC | initFailure | {scrape_date.strftime("%Y/%m/%d")}")
        logger.error(f"""{pipecode} | OC | initFailure | {scrape_date.strftime("%Y/%m/%d")}
                     - {error_detailed(e)}""")
