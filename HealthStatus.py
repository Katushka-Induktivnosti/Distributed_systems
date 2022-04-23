import asyncio
from typing import Dict

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler


class HealthStatus:
    __healthStatus = Dict[str, str]
    __lock: asyncio.Lock
    __scheduler: AsyncIOScheduler
    __parent = None  # type: Server

    def __init__(self, parent):
        self.__healthStatus = {}
        self.__lock = asyncio.Lock()
        self.__parent = parent
        self.__scheduler = AsyncIOScheduler()
        self.__scheduler.add_job(self.sendHeartbeat, trigger='interval', seconds=1)
        self.__scheduler.add_job(self.checkHealthStatusDict, trigger='interval', seconds=1)

    def startScheduler(self):
        self.__scheduler.start()

    async def setStatus(self, key, status):
        async with self.__lock:
            oldStatus = self.__healthStatus.get(key)
            self.__healthStatus[key] = status
        if self.__parent.isMaster:
            if status == 'HEALTHY':
                if not oldStatus or oldStatus != 'HEALTHY':
                    await self.__parent.replicateMsg(key)


    async def getStatus(self, srv=None) -> Dict[str, str]:
        healhStatus = {}
        if not srv:
            async with self.__lock:
                healhStatus = self.__healthStatus.copy()
        else:
            async with self.__lock:
                healhStatus = self.__healthStatus.get(srv)
        return healhStatus

    async def removeServer(self, srv) -> None:
        async with self.__lock:
            self.__healthStatus.pop(srv)
        return

    async def sendHeartbeat(self) -> None:
        if self.__parent.isMaster is True:
            asyncio.ensure_future(self.sendHeartbeatM())
        elif self.__parent.isSlave is True:
            asyncio.ensure_future(self.sendHeartbeatS())
        return

    async def sendHeartbeatM(self) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for slave in self.__parent.slaves:
                tasks.append(asyncio.ensure_future(self.sendHeartbeatToEndpoint(session, slave)))
            await asyncio.gather(*tasks)
        return

    async def sendHeartbeatS(self) -> None:
        async with aiohttp.ClientSession() as session:
            await self.sendHeartbeatToEndpoint(session, self.__parent.master)
        return

    async def sendHeartbeatToEndpoint(self, session, endpoint):
        url = endpoint + '/health'
        params = {'from': f"http://{self.__parent.host}:{self.__parent.port}"}
        try:
            async with session.get(
                    url=url,
                    params=params
            ) as resp:
                response = resp
        except:
            await self.setStatus(endpoint, 'DEAD')
            return
        if response.status != 200:
            await self.setStatus(endpoint, 'UNHEALTHY')
            return
        elif response.status == 200:
            await self.setStatus(endpoint, 'HEALTHY')
            return
        return

    async def checkHealthStatusDict(self):
        if self.__parent.isMaster is True:
            srv_list = self.__healthStatus.copy().keys()
            for srv in srv_list:
                if srv not in self.__parent.slaves:
                    await self.removeServer(srv)
        elif self.__parent.isSlave is True:
            srv_list = self.__healthStatus.copy().keys()
            for srv in srv_list:
                if srv != self.__parent.master:
                    await self.removeServer(srv)
        else:
            self.__healthStatus.clear()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__scheduler.shutdown(wait=False)
        return
