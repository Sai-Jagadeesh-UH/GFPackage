import asyncio

from .handleRaw import pushRawOA, pushRawOC, pushRawNN, pushRawMeta
# from .handleRaw import pushRawLogs
from .handleSilver import pushSilverOA, pushSilverOC, pushSilverNN
from .handleGold import pushGold


async def pushEnbridge():
    '''
    'pushRawMeta' followed by async pushOA, pushOC & pushNN then 'pushGold' and 'pushRawLogs'
    '''

    await pushRawMeta()

    async with asyncio.TaskGroup() as group:
        group.create_task(pushOA())
        group.create_task(pushOC())
        group.create_task(pushNN())

        # pushRaw
        # push Silver
        # push Gold
        # delete the raw silver and gold for next run

    pushGold()

    # await pushRawLogs()


async def pushOA():
    '''
    'pushRawOA' followed by 'pushSilverOA'
    '''
    await pushRawOA()
    await pushSilverOA()


async def pushOC():
    '''
    'pushRawOC' followed by 'pushSilverOC'
    '''
    await pushRawOC()
    await pushSilverOC()


async def pushNN():
    '''
    'pushRawNN' followed by 'pushSilverNN'
    '''
    await pushRawNN()
    await pushSilverNN()
