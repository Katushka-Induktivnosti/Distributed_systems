import asyncio
import contextlib
import json
import random
import time

from typing import List, Tuple, Set
from urllib import parse

import aiohttp
from aiohttp import web
from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from HealthStatus import HealthStatus
from AtomicCounter import AtomicCounter


class Server:
    host: str
    port: int
    __in_memory: Set[Tuple[int, float, str]]
    slaves: List[str]
    routes: List[web.RouteDef]
    max_retries: int
    isMaster: bool
    isSlave: bool
    master: str
    healthStatus: HealthStatus
    __scheduler: AsyncIOScheduler
    __deduplication_job: Job
    __synchro_job: Job
    __messageAtomicCounter: AtomicCounter

    def __init__(self, host, port):
        print("Init Handler")
        self.host = host
        self.port = port
        self.__in_memory = set()
        self.slaves = []
        self.max_retries = 5
        self.consistency = 1
        self.isMaster = False
        self.isSlave = False
        self.master = ''
        self.healthStatus = HealthStatus(self)
        self.healthStatus.startScheduler()
        self.init_routes()
        self.__scheduler = AsyncIOScheduler()
        self.__scheduler.add_job(self.deduplicateMsg, trigger='interval', seconds=10)
        self.__scheduler.add_job(self.synchronizeMsg, trigger='interval', seconds=10)
        self.__messageAtomicCounter = AtomicCounter()

        pass

    def init_routes(self):
        self.routes = [
            web.get('/append', self.appendMsg),
            web.get('/show', self.showMsg),
            web.get('/health', self.getHealth),
            web.get('/local/show', self.showLocalMsg),
            web.post('/slave/messages/synchronize', self.slaveSynchronize),
            web.post('/slave/messages/replicate/all', self.slaveReplicateAll),
            web.get('/slave/messages/append', self.appendSlaveMsg),
            web.get('/slave/nodes/add', self.addMaster),
            web.get('/slave/nodes/remove', self.rmMaster),
            web.get('/slave/nodes/show', self.showMaster),
            web.get('/slave/connect/confirm', self.slaveConnectConfirm),
            web.get('/slave/disconnect/confirm', self.slaveDisconnectConfirm),
            web.get('/master/nodes/add', self.addSlave),
            web.get('/master/nodes/remove', self.rmSlave),
            web.get('/master/nodes/show', self.showSlaves),
            web.get('/master/connect/confirm', self.masterConnectConfirm),
            web.get('/master/disconnect/confirm', self.masterDisconnectConfirm),
        ]
        return

    async def appendMsg(self, request: web.Request):
        if self.isSlave:
            raise web.HTTPFound(location=self.master + request.path_qs)

        resp = web.StreamResponse()
        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)

        timestamp = time.time()
        msg = query_def.get('msg', None)
        consistency = int(query_def.get('w', [self.consistency])[0])
        timeout = float(query_def.get('timeout', [99999999999])[0])

        if not msg or len(msg) == 0:
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('No message could be written!'.encode())
            await resp.write_eof()
            return

        message = msg[0]
        msg_id = await self.__messageAtomicCounter.increment()

        atomic = AtomicCounter(consistency)
        await atomic.increment()
        asyncio.ensure_future(self.msgToSlaveWrapper((msg_id, timestamp, message), atomic))
        try:
            await atomic.wait(timeout)
        except asyncio.TimeoutError:
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not save your message!\n'.encode())
            await resp.write(message.encode())
            await resp.write_eof()
            return

        self.__in_memory.add((msg_id, timestamp, message))

        resp.set_status(200)
        resp.content_type = 'text/plain'
        await resp.prepare(request)
        await resp.write('Successfully saved your message!\n'.encode())
        await resp.write(message.encode())
        await resp.write_eof()
        return

    async def appendSlaveMsg(self, request: web.Request):
        resp = web.StreamResponse()
        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        msg_id = query_def.get('msg_id', None)
        timestamp = query_def.get('timestamp', None)
        msg = query_def.get('msg', None)

        if not timestamp or not msg or len(msg) == 0:
            resp.set_status(500)
            await resp.prepare(request)
            await resp.write_eof()
        else:
            resp.set_status(200)
            await resp.prepare(request)
            await resp.write_eof()
            # asyncio.sleep(30)
            self.__in_memory.add((int(msg_id[0]), float(timestamp[0]), msg[0]))
        return

    async def addSlave(self, request: web.Request):
        if self.isSlave:
            raise web.HTTPFound(location=self.master + request.path_qs)

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('url', None)

        if not url or len(url) == 0:
            resp = web.StreamResponse()
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not append slave!'.encode())
            await resp.write_eof()
            return
        else:
            if url[0] not in self.slaves:
                result = await self.sendConfirmToSlave(url[0])

                if not result:
                    resp = web.StreamResponse()
                    resp.set_status(500)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('Could not append this slave!\n'.encode())
                    await resp.write(url[0].encode())
                    await resp.write_eof()
                    return

                elif result:
                    self.isMaster = True
                    self.isSlave = False
                    self.slaves.append(url[0])
                    resp = web.StreamResponse()
                    resp.set_status(200)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('Slave has been successfully appended!\n'.encode())
                    await resp.write(url[0].encode())
                    await resp.write_eof()
                    await self.healthStatus.setStatus(url[0], 'HEALTHY')
                    return
            self.isMaster = True
            self.isSlave = False
            resp = web.StreamResponse()
            resp.set_status(201)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Slave has been already appended!\n'.encode())
            await resp.write(url[0].encode())
            await resp.write_eof()
            return
        return

    async def rmSlave(self, request: web.Request):
        if self.isSlave:
            raise web.HTTPFound(location=self.master + request.path_qs)

        resp = web.StreamResponse()
        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('url', None)

        if not url or len(url) == 0:
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not remove slave!'.encode())
            await resp.write_eof()
            return
        else:
            if url[0] in self.slaves:
                await self.sendConfirmToSlave(url[0], disconnect=True)
                self.slaves.remove(url[0])
                resp.set_status(200)
                resp.content_type = 'text/plain'
                await resp.prepare(request)
                await resp.write('Slave has been successfully removed!\n'.encode())
                await resp.write(url[0].encode())
                await resp.write_eof()
                if len(self.slaves) == 0:
                    self.isMaster = False
                return
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not find your slave to remove!\n'.encode())
            await resp.write(url[0].encode())
            await resp.write_eof()
            return
        return

    async def addMaster(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM MASTER!'.encode())
            await resp.write_eof()
            return
        elif self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('ALREADY HAVE MASTER!'.encode())
            await resp.write_eof()
            return

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('url', None)

        if not url or len(url) == 0:
            resp = web.StreamResponse()
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not append master!'.encode())
            await resp.write_eof()
            return
        elif url[0] == self.master:
            self.isMaster = False
            self.isSlave = True
            resp = web.StreamResponse()
            resp.set_status(201)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('This master has been already appended!\n'.encode())
            await resp.write(url[0].encode())
            await resp.write_eof()
        else:
            result = await self.sendConfirmToMaster(url[0])

            if not result:
                resp = web.StreamResponse()
                resp.set_status(500)
                resp.content_type = 'text/plain'
                await resp.prepare(request)
                await resp.write('Could not append this master!\n'.encode())
                await resp.write(url[0].encode())
                await resp.write_eof()
                return

            elif result:
                self.isMaster = False
                self.isSlave = True
                self.master = url[0]
                resp = web.StreamResponse()
                resp.set_status(200)
                resp.content_type = 'text/plain'
                await resp.prepare(request)
                await resp.write('Master has been successfully appended!\n'.encode())
                await resp.write(url[0].encode())
                await resp.write_eof()
                return
        return

    async def rmMaster(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM MASTER!'.encode())
            await resp.write_eof()
            return
        elif not self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('I DONT HAVE ANY MASTER!'.encode())
            await resp.write_eof()
            return

        resp = web.StreamResponse()
        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('url', None)

        if not url or len(url) == 0:
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not remove master!'.encode())
            await resp.write_eof()
            return
        else:
            if url[0] == self.master:
                await self.sendConfirmToMaster(url[0], disconnect=True)
                self.master = ''
                self.isSlave = False
                resp.set_status(200)
                resp.content_type = 'text/plain'
                await resp.prepare(request)
                await resp.write('Master has been successfully removed!\n'.encode())
                await resp.write(url[0].encode())
                await resp.write_eof()
                return
            resp.set_status(500)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('Could not find your master to remove!\n'.encode())
            await resp.write(url[0].encode())
            await resp.write_eof()
            return
        return

    async def showMsg(self, request: web.Request):
        if self.isSlave:
            raise web.HTTPFound(location=self.master + request.path_qs)
        resp = web.StreamResponse()
        resp.set_status(200)
        resp.content_type = 'text/plain'
        await resp.prepare(request)

        if len(self.__in_memory) == 0:
            await resp.write('No messages were appended!'.encode())
        else:
            msgs = self.__in_memory.copy()
            msgs = sorted(msgs, key=lambda key: key[0], reverse=False)
            await resp.write(str(msgs).encode())

        await resp.write_eof()
        return

    async def showLocalMsg(self, request: web.Request):
        resp = web.StreamResponse()
        resp.set_status(200)
        resp.content_type = 'text/plain'
        await resp.prepare(request)

        if len(self.__in_memory) == 0:
            await resp.write('No messages were appended!'.encode())
        else:
            msgs = list(self.__in_memory)
            msgs = sorted(msgs, key=lambda key: msgs[0], reverse=False)
            await resp.write(str(msgs).encode())

        await resp.write_eof()
        return

    async def showSlaves(self, request: web.Request):
        if not self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM NOT MASTER!'.encode())
            await resp.write_eof()
            return
        elif self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM SLAVE!'.encode())
            await resp.write_eof()
            return

        resp = web.StreamResponse()
        resp.set_status(200)
        resp.content_type = 'text/plain'
        await resp.prepare(request)

        if len(self.slaves) == 0:
            await resp.write('No slaves were appended!'.encode())
        else:
            slaves = await self.healthStatus.getStatus()
            await resp.write(str(slaves).encode())

        await resp.write_eof()
        return

    async def showMaster(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM MASTER!'.encode())
            await resp.write_eof()
            return
        elif not self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('I DONT HAVE ANY MASTER!'.encode())
            await resp.write_eof()
            return

        resp = web.StreamResponse()
        resp.set_status(200)
        resp.content_type = 'text/plain'
        await resp.prepare(request)

        if self.master == '':
            await resp.write('No master was appended!'.encode())
        else:
            slaves = await self.healthStatus.getStatus()
            await resp.write(str(slaves).encode())

        await resp.write_eof()
        return

    async def slaveConnectConfirm(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('LUKE, IM YOUR FATHER!'.encode())
            await resp.write_eof()
            return
        if self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('YOU ARE NOT MY FATHER!'.encode())
            await resp.write_eof()
            return

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('from', None)

        if not url:
            resp = web.StreamResponse()
            resp.set_status(400)
            await resp.prepare(request)
            await resp.write('WRONG HOME ADDRESS, DADDY!'.encode())
            await resp.write_eof()
            return

        self.isSlave = True
        self.isMaster = False
        self.master = url[0]
        resp = web.StreamResponse()
        resp.set_status(200)
        await resp.prepare(request)
        await resp.write('YES DADDY!\nUwU'.encode())
        await resp.write_eof()
        return

    async def slaveDisconnectConfirm(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('WHO ARE YOU?!'.encode())
            await resp.write_eof()
            return
        if not self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('YOU ARE NOT MY FATHER!'.encode())
            await resp.write_eof()
            return

        if self.isSlave:
            query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
            url = query_def.get('from', None)
            if not url or self.master != url[0]:
                resp = web.StreamResponse()
                resp.set_status(400)
                await resp.prepare(request)
                await resp.write('WRONG HOME ADDRESS, DADDY!'.encode())
                await resp.write_eof()
                return
            elif self.master == url[0]:
                self.isSlave = False
                self.isMaster = False
                self.master = ''
                resp = web.StreamResponse()
                resp.set_status(200)
                await resp.prepare(request)
                await resp.write('IM TOO OLD FOR THAT'.encode())
                await resp.write_eof()
                return
        return

    async def masterConnectConfirm(self, request: web.Request):
        if self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('IM SLAVE!'.encode())
            await resp.write_eof()
            return

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('from', None)

        if not url or len(url) == 0:
            resp = web.StreamResponse()
            resp.set_status(400)
            await resp.prepare(request)
            await resp.write('WRONG HOME ADDRESS, SON!'.encode())
            await resp.write_eof()
            return

        self.isSlave = False
        self.isMaster = True
        self.slaves.append(url[0])
        resp = web.StreamResponse()
        resp.set_status(200)
        await resp.prepare(request)
        await resp.write('WELCOME TO THE FAMILY!'.encode())
        await resp.write_eof()
        await self.healthStatus.setStatus(url[0], 'HEALTHY')
        return

    async def masterDisconnectConfirm(self, request: web.Request):
        if not self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('WHO ARE YOU?!'.encode())
            await resp.write_eof()
            return
        elif self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            await resp.prepare(request)
            await resp.write('IM SLAVE!'.encode())
            await resp.write_eof()
            return

        elif self.isMaster:
            query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
            url = query_def.get('from', None)
            if not url or url[0] not in self.slaves:
                resp = web.StreamResponse()
                resp.set_status(400)
                await resp.prepare(request)
                await resp.write('WRONG HOME ADDRESS, SON!'.encode())
                await resp.write_eof()
                return
            elif url[0] in self.slaves:
                resp = web.StreamResponse()
                resp.set_status(200)
                await resp.prepare(request)
                await resp.write('GOODBYE, SON'.encode())
                await resp.write_eof()
                self.slaves.remove(url[0])
                if len(self.slaves) == 0:
                    self.isMaster = False
                return
        return

    async def getHealth(self, request: web.Request):
        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('from', None)
        if url and len(url) > 0:
            if self.isSlave:
                if url[0] == self.master:
                    resp = web.StreamResponse()
                    resp.set_status(200)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('HEALTHY'.encode())
                    await resp.write_eof()
                    return
                else:
                    resp = web.StreamResponse()
                    resp.set_status(403)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('UNHEALTHY'.encode())
                    await resp.write_eof()
                    asyncio.ensure_future(self.sendConfirmToMaster(url[0], disconnect=True))
                    return
            if self.isMaster:
                if url[0] in self.slaves:
                    resp = web.StreamResponse()
                    resp.set_status(200)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('HEALTHY'.encode())
                    await resp.write_eof()
                    return
                else:
                    resp = web.StreamResponse()
                    resp.set_status(403)
                    resp.content_type = 'text/plain'
                    await resp.prepare(request)
                    await resp.write('UNHEALTHY'.encode())
                    await resp.write_eof()
                    asyncio.ensure_future(self.sendConfirmToSlave(url[0], disconnect=True))
                    return
            if not self.isSlave and not self.isMaster:
                resp = web.StreamResponse()
                resp.set_status(403)
                resp.content_type = 'text/plain'
                await resp.prepare(request)
                await resp.write('UNHEALTHY'.encode())
                await resp.write_eof()
                asyncio.ensure_future(self.sendConfirmToMaster(url[0], disconnect=True))
                asyncio.ensure_future(self.sendConfirmToSlave(url[0], disconnect=True))
                return
        else:
            resp = web.StreamResponse()
            resp.set_status(200)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('HEALTHY'.encode())
            await resp.write_eof()
            return
        return

    async def slaveSynchronize(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM MASTER!'.encode())
            await resp.write_eof()
            return
        elif not self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('I DONT HAVE ANY MASTER!'.encode())
            await resp.write_eof()
            return

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('from', None)
        if not url or len(url) == 0 or url[0] != self.master:
            resp = web.StreamResponse()
            resp.set_status(400)
            await resp.prepare(request)
            await resp.write('WRONG HOME ADDRESS, DADDY!'.encode())
            await resp.write_eof()
            return
        else:
            resp = web.StreamResponse()
            resp.set_status(200)
            await resp.prepare(request)
            await resp.write_eof()
            '''
            master_synchro = await request.json()

            # master_msg = sorted(master_msg, key=lambda key: key[0])
            ## TODO
            # remove messages that were not in master set
            # maybe leave all messages if masters last was within 2+ secs range
            self.__in_memory = set(filter(lambda key: key[0] < master_msg[0][0], self.__in_memory))
            for msg in master_msg:
                if msg not in self.__in_memory:
                    self.__in_memory.add(msg)
            '''
            return





    async def msgToSlaveWrapper(self, message: Tuple[int, float, str], atomic: AtomicCounter):
        async with aiohttp.ClientSession() as session:
            tasks_dict = {}
            was_iter = False
            while not was_iter or not await atomic.waitWOException(0):
                if was_iter:
                    await atomic.waitWOException(10)
                for slave in self.slaves:
                    if tasks_dict.get(slave):
                        task = tasks_dict.get(slave)
                        if task.done():
                            continue
                        elif not task.cancelled():
                            continue
                    # if no such task
                    # and consistency level reached
                    # dont add new task
                    if not tasks_dict.get(slave) and (was_iter and await atomic.waitWOException(0)):
                        continue
                    tasks_dict[slave] = asyncio.ensure_future(self.sendMsgToSlave(
                        session,
                        slave,
                        message,
                        atomic
                    ))
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(asyncio.gather(*tasks_dict.values())), 10)
                # check if servers were removed from slaves
                for srv in list(tasks_dict.keys()):
                    if srv not in self.slaves:
                        task = tasks_dict.pop(srv)
                        if task.done():
                            await atomic.decrement()
                        elif not task.done() and not task.cancelled():
                            task.cancel()
                was_iter = True
            for task in tasks_dict.values():
                if not task.done() and not task.cancelled():
                    task.cancel()
        return

    async def sendMsgToSlave(self, session, slave, msg: Tuple[int, float, str], atomic: AtomicCounter):
        retry = 0
        url = slave + '/slave/messages/append'
        params = {'msg_id': msg[0], 'timestamp': msg[1], 'msg': msg[2]}
        lastResponse = None
        while True:
            retry += 1
            print(retry)
            try:
                async with session.get(
                        url=url,
                        params=params,
                ) as resp:
                    lastResponse = resp
            except:
                if retry <= self.max_retries:
                    await asyncio.sleep(random.uniform(0, 1.2))
                else:
                    await asyncio.sleep(random.uniform(1.2, 5))
                continue
            if lastResponse.status == 500:
                if retry <= self.max_retries:
                    await asyncio.sleep(random.uniform(0, 0.8))
                else:
                    await asyncio.sleep(random.uniform(0.8, 3))
                continue
            if lastResponse.status == 200:
                await atomic.increment()
                return True, resp
            if retry <= self.max_retries:
                await asyncio.sleep(random.uniform(0, 1.2))
            else:
                await asyncio.sleep(random.uniform(1.2, 5))
        return False, lastResponse

    async def sendConfirmToSlave(self, slave, disconnect=False):
        if disconnect:
            url = slave + "/slave/disconnect/confirm"
        else:
            url = slave + "/slave/connect/confirm"

        params = {'from': f"http://{self.host}:{self.port}"}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                        url=url,
                        params=params,
                ) as resp:
                    lastResponse = resp
            except:
                if disconnect:
                    return True
                else:
                    return False
            if lastResponse.status == 200:
                return True
            elif lastResponse.status != 200:
                return False
        return False

    async def sendConfirmToMaster(self, slave, disconnect=False):
        if disconnect:
            url = slave + "/master/disconnect/confirm"
        else:
            url = slave + "/master/connect/confirm"

        params = {'from': f"http://{self.host}:{self.port}"}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                        url=url,
                        params=params,
                ) as resp:
                    lastResponse = resp
            except:
                if disconnect:
                    return True
                else:
                    return False
            if lastResponse.status == 200:
                return True
            elif lastResponse.status != 200:
                return False
        return False

    async def slaveReplicateAll(self, request: web.Request):
        if self.isMaster:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('IM MASTER!'.encode())
            await resp.write_eof()
            return
        elif not self.isSlave:
            resp = web.StreamResponse()
            resp.set_status(403)
            resp.content_type = 'text/plain'
            await resp.prepare(request)
            await resp.write('I DONT HAVE ANY MASTER!'.encode())
            await resp.write_eof()
            return

        query_def = parse.parse_qs(parse.urlparse(request.path_qs).query)
        url = query_def.get('from', None)
        if not url or len(url) == 0 or url[0] != self.master:
            resp = web.StreamResponse()
            resp.set_status(400)
            await resp.prepare(request)
            await resp.write('WRONG HOME ADDRESS, DADDY!'.encode())
            await resp.write_eof()
            return

        master_all_messages = await request.json()
        for msg in master_all_messages:
            self.__in_memory.add(tuple(msg))

        resp = web.StreamResponse()
        resp.set_status(200)
        await resp.prepare(request)
        await resp.write_eof()
        return

    async def replicateMsg(self, slave):
        if not self.isMaster:
            return
        if self.isSlave:
            return
        url = slave + "/slave/messages/replicate/all"
        params = {'from': f"http://{self.host}:{self.port}"}
        async with aiohttp.ClientSession() as session:
            while True:
                if slave not in self.slaves:
                    return
                if await self.healthStatus.getStatus(slave) != 'HEALTHY':
                    return
                try:
                    async with session.post(
                            url=url,
                            params=params,
                            data=str(json.dumps(list(self.__in_memory))).encode('utf-8')
                    ) as resp:
                        lastResponse = resp
                except Exception as e:
                    print(e)
                    continue
                if lastResponse.status == 200:
                    return
                elif lastResponse.status != 200:
                    continue



    async def deduplicateMsg(self):
        pass

    async def synchronizeMsg(self):
        pass
