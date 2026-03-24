import time
from datetime import datetime, timedelta

from src.enbridgescrape import metaDump
from src.enbridgescrape import runEnbridgeScrape
from src.enbridgescrape import runNN_Scrape

# from ..Munger import formatOA, formatOC
from ..utils import logger
from ..cloudPush import pushEnbridge


async def scrapeToday(head_less: bool = True):

    start_time = time.perf_counter()

    await metaDump()

    #  Yesterday
    target_date = datetime.today() - timedelta(days=1)
    logger.info(f"scrapeToday - {target_date=}")

    await runEnbridgeScrape(target_date, head_less=head_less)

    #  today
    target_date = datetime.today()
    logger.info(f"scrapeToday - {target_date=}")

    await runEnbridgeScrape(target_date, head_less=head_less)

    #  today (latest NN)
    logger.info(f"scrapeToday - runNN_Scrape {target_date=}")
    await runNN_Scrape(target_date, head_less=head_less)

    #  today (latest MetaData)
    logger.info(f"scrapeToday - metaDump {target_date=}")

    await pushEnbridge()

    logger.info(
        f"{'*'*15} completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")


async def scrapeSomeday(scrapeDay: datetime, head_less: bool = True):
    start_time = time.perf_counter()

    if scrapeDay > datetime.today():
        return
    # await metaDump()

    #  Yesterday
    logger.info(f"scrapeToday - {scrapeDay=}")

    await runEnbridgeScrape(scrapeDay, head_less=head_less)

    await runNN_Scrape(scrapeDay, head_less=head_less)

    #  today (latest MetaData)
    logger.info(f"scrapeToday - metaDump {scrapeDay=}")

    await pushEnbridge()

    logger.info(
        f"{'*'*15} completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")
