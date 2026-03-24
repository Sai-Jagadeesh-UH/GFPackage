import asyncio
from datetime import datetime

from ..utils import openPage, error_detailed, trackFail
from ..utils import logger, pipeConfigs_df
from .OprAvail import scrape_OA
from .OprCap import scrape_OC
from .enbridgeLongScrape import enbridgeLongRun


async def enbridgePipeScrape(pipecode: str, scrape_date: datetime, head_less: bool = True):
    """
    worker process to scrape given pipecode and scrape_date, if OA fails immediately starts long process.
    OA and OC are tried only if they are present for the pipecode as per pipeConfigs_df
    :param pipecode: Description
    :type pipecode: str
    :param scrape_date: Description
    :type scrape_date: datetime
    :param head_less: Descriptions
    :type head_less: bool
    """
    # today or past else D day
    target_date = scrape_date if scrape_date <= datetime.today() else datetime.now()
    # logger.critical(f"{pipecode} scrape started")
    try:
        if (pipecode in ['MNUS']):
            raise ValueError("MNUS - going Long Way")

        # try short way
        async with openPage(headLess=head_less) as page:
            await page.goto(
                f"https://rtba.enbridge.com/InformationalPosting/Default.aspx?bu={pipecode}&Type=OA"
            )

            date_box = (
                page.get_by_text("Gas Date: ")
                .locator("xpath=./following-sibling::div")
                .get_by_role("textbox")
                .nth(0)
            )

            await date_box.fill("")
            for i in range(10):
                await page.keyboard.press("Backspace", delay=100)

            await date_box.fill((target_date).strftime("%m/%d/%Y"))

            await page.keyboard.press("Enter")

            # check if OA fails immediated kick in long way
            if pipeConfigs_df.loc[pipeConfigs_df['PipeCode'] == pipecode, ['PointCapCode']].values[0][0]:
                # if 'OA' in code2seg[pipecode]:
                await scrape_OA(page=page)

            # check if OC is present
            if pipeConfigs_df.loc[pipeConfigs_df['PipeCode'] == pipecode, ['SegmentCapCode']].values[0][0]:

                op_cap = page.get_by_text("Operational Capacity Maps")

                if (op_cap):
                    await scrape_OC(mainpage=page, pipecode=pipecode, scrape_date=target_date)

                await asyncio.sleep(1)

    except Exception as e:
        logger.warning(
            f"""failed : enbridgeRun-{pipecode=} {scrape_date=} - trying long way {'*'*10}
            - {error_detailed(e)} """)
        try:
            await enbridgeLongRun(pipeCode=pipecode, head_less=head_less, scrape_date=target_date)
        except Exception as e:
            logger.critical(
                f"{pipecode} | OA | Full | {target_date.strftime("%Y/%m/%d")}")
            trackFail(f"{pipecode}|OA|{target_date.strftime("%Y/%m/%d")}\n")

            logger.error(f"""{pipecode} | OA | Full | {target_date.strftime("%Y/%m/%d")}
                        - {error_detailed(e)}""")
