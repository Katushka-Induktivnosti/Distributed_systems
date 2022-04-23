import asyncio
import contextlib


class AtomicCounter:
    __counter: int
    __consistency: int
    __lock: asyncio.Lock
    __event: asyncio.Event

    def __init__(self, consistency=-1):
        self.__counter = int()
        self.__consistency = consistency
        self.__lock = asyncio.Lock()
        self.__event = asyncio.Event()
        self.__event.clear()

    async def increment(self) -> int:
        async with self.__lock:
            self.__counter += 1
            if self.__consistency > 0:
                if self.__counter >= self.__consistency:
                    self.__event.set()
            return self.__counter

    async def decrement(self) -> int:
        async with self.__lock:
            self.__counter -= 1
            if self.__consistency > 0:
                if self.__counter >= self.__consistency:
                    self.__event.set()
            return self.__counter

    async def getCounter(self) -> int:
        async with self.__lock:
            return self.__counter

    async def waitWOException(self, timeout=999999999999999):
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.__event.wait(), timeout)
        return self.__event.is_set()

    async def wait(self, timeout=999999999999999):
        await asyncio.wait_for(self.__event.wait(), timeout)
