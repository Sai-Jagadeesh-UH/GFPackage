import time
import asyncio

from datetime import datetime, timedelta
import concurrent.futures

# from polars import head

from src.enbridgescrape import metaDump
from src.enbridgescrape import runEnbridgeScrape
from src.enbridgescrape import runNN_Scrape

# from ..Munger import formatOA, formatOC
from ..cloudPush import pushEnbridge
from ..utils import logger


# headLess = True


async def dateRunner(target_date: datetime):
    async with asyncio.TaskGroup() as group:
        group.create_task(runEnbridgeScrape(target_date))
        group.create_task(runNN_Scrape(target_date))


def runScrape(target_date: datetime):
    start_time = time.perf_counter()
    logger.info(f"{target_date} Process kicking in!!!")
    asyncio.run(dateRunner(target_date))
    logger.info(
        f"{target_date} scrape completed in {time.perf_counter()-start_time: .2f}s {'*'*15}")


async def scrapeHistoric(startDate: datetime = datetime.today() - timedelta(days=365*3 + 1), endDate: datetime = datetime.today(), head_less: bool = True):

    await metaDump()

    range_len = (endDate - startDate).days

    for dayRange in range(0, range_len, 100):
        listDates = [startDate+timedelta(days=i) for i in range(
            dayRange, dayRange + 100) if startDate+timedelta(days=i) <= datetime.today()]
        with concurrent.futures.ProcessPoolExecutor(max_workers=None) as executor:
            # Submit all dates to the executor
            # executor.map is simple for applying one function to many inputs
            executor.map(runScrape, listDates)
        # await dateRunner(startDate+timedelta(days=dayRange))

    await pushEnbridge()
    # await pushRawOA()
    # await pushRawOC()
    # await pushRawNN()
    # await pushRawLogs()

    # formatOA()
    # formatOC()
