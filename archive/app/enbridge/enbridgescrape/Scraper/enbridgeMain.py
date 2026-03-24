import time
import asyncio

from datetime import datetime
from .enbridgeScrape import enbridgePipeScrape

# from .enbridgeLongScrape import enbridgeLongRun
from ..utils import logger, pipeConfigs_df


async def runEnbridgeScrape(scrape_date: datetime, head_less: bool = True):
    """
    starts scrape on each pipe with OA or SG is present

    :param scrape_date: scrape_date
    :type scrape_date: datetime
    :param head_less: head_less
    :type head_less: bool
    """
    start_time = time.perf_counter()

    # multithreaded/AIO of all pipes
    async with asyncio.TaskGroup() as group:
        # for pipecode in code2seg.keys():
        for pipecode in pipeConfigs_df.dropna(how='all', subset=['PointCapCode', 'SegmentCapCode'])['PipeCode']:

            # skip WE before Oct 1 2025 Not Available
            if (pipecode == 'WE' and scrape_date < datetime(2025, 10, 1)):
                continue

            # logger.critical(f"{pipecode} - initiated")
            group.create_task(enbridgePipeScrape(
                pipecode=pipecode, head_less=head_less, scrape_date=scrape_date))
    logger.info(f"{' '*8} {time.perf_counter()-start_time: .2f}s {'-'*15} ")
