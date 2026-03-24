import time
from datetime import datetime, timedelta

from ..utils import openPage, paths, error_detailed, pipeConfigs_df, trackFail
from ..utils import logger

nn_downloads_path = paths.downloads / 'NN_raw'
nn_downloads_path.mkdir(exist_ok=True, parents=True)


async def run(pipecode: str, scrape_date: datetime, head_less: bool = True):
    target_date = scrape_date if (scrape_date and scrape_date <= datetime.today() - timedelta(days=4)) else (
        datetime.now() - timedelta(days=4))
    try:
        async with openPage(headLess=head_less) as page:
            await page.goto(
                rf"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipecode}&Type=NN"
            )

            date_box = (
                page.get_by_text("Gas Date: ")
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )

            if date_box:
                await date_box.fill("")
                for i in range(10):
                    await page.keyboard.press("Backspace", delay=100)

                await date_box.fill((target_date).strftime("%m/%d/%Y"))

                await page.keyboard.press("Enter")

            async with page.expect_download() as download_info:
                await page.get_by_role("link", name="Downloadable Format").click()

            download = await download_info.value

            await download.save_as(nn_downloads_path / download.suggested_filename)

    except Exception as e:
        trackFail(f"{pipecode}|NN|{target_date.strftime("%Y/%m/%d")}\n")
        logger.critical(
            f"{pipecode} | NN | initFailure | {target_date.strftime("%Y/%m/%d")}")
        logger.error(f"""{pipecode} | NN | initFailure | {target_date.strftime("%Y/%m/%d")}
                     - {error_detailed(e)}""")


async def runNN_Scrape(scrape_date: datetime, head_less: bool = True):

    # NN_List = [i for i in code2seg if 'NN' in code2seg[i]]
    NN_List = list(pipeConfigs_df['NoNoticeCode'].dropna())

    for pipecode in NN_List:
        start_time = time.perf_counter()
        try:
            await run(pipecode, head_less=head_less, scrape_date=scrape_date)
        except Exception:
            logger.error(f"failed: runNN_Scrape {pipecode=} {scrape_date=} ")
        finally:
            logger.info(
                f"{pipecode} - {time.perf_counter()-start_time: .2f}s {'-'*15} ")
