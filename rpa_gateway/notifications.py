"""One Redis subscriber and one batched reconciliation per instance, not per socket."""
import asyncio
import json
import random
import uuid
import time
import logging
from dataclasses import dataclass,field
import redis.asyncio as aioredis
from starlette.concurrency import run_in_threadpool


@dataclass
class Connection:
    device: dict
    session: str
    queue: asyncio.Queue=field(default_factory=lambda:asyncio.Queue(maxsize=1))
    invalid: bool=False
    revision: int=-1


class Hub:
    def __init__(self,store,presence,metrics,max_connections=3000,instance_id=None):
        self.store,self.presence,self.metrics=store,presence,metrics
        self.max_connections=max_connections
        self.instance_id=instance_id or uuid.uuid4().hex
        self.clients={}
        self.by_device={}
        self.jobs=[]
        self.stopping=False

    def signal(self,connection):
        if not connection.queue.full():connection.queue.put_nowait(True)
        else:self.metrics.notifications.labels('coalesced').inc()

    async def add(self,device,session):
        if self.stopping or len(self.clients)>=self.max_connections:raise RuntimeError('Connection capacity reached')
        # A bounded local slot for each connection. Multiple connections cannot multiply device execution slots in PostgreSQL.
        key=self.instance_id+':'+uuid.uuid4().hex
        if not await run_in_threadpool(self.presence.acquire_connection,device,key):
            raise RuntimeError('Device connection limit reached')
        connection=Connection(device,session)
        self.clients[key]=connection
        self.by_device.setdefault((device['tenant'],device['id']),{})[key]=connection
        self.metrics.websockets.inc()
        self.signal(connection)
        return key,connection

    async def remove(self,key):
        connection=self.clients.pop(key,None)
        if connection is not None:
            owner=(connection.device['tenant'],connection.device['id'])
            self.by_device.get(owner,{}).pop(key,None)
            if not self.by_device.get(owner):self.by_device.pop(owner,None)
            self.metrics.websockets.dec()
            try:await run_in_threadpool(self.presence.release_connection,connection.device,key)
            except Exception:pass

    async def start(self,url):
        # Pub/Sub may legitimately be idle indefinitely. Bound commands and check
        # liveness with PING/PONG instead of treating an idle socket as a failure.
        self.redis=aioredis.Redis.from_url(url,decode_responses=True,socket_connect_timeout=1,socket_timeout=None,
                                          max_connections=4)
        self.jobs=[asyncio.create_task(self.subscribe()),asyncio.create_task(self.reconcile())]

    async def close(self):
        self.stopping=True
        for connection in self.clients.values():connection.invalid=True;self.signal(connection)
        for job in self.jobs:job.cancel()
        # Closing the transport first also unblocks a Redis subscription cancelled during startup.
        if self.redis:await self.redis.aclose()
        if self.jobs:
            _,pending=await asyncio.wait(self.jobs,timeout=3)
            for job in pending:job.cancel()

    async def subscribe(self):
        while not self.stopping:
            try:
                async with self.redis.pubsub() as subscription:
                    await asyncio.wait_for(subscription.subscribe(self.presence.channel), 3)
                    self.metrics.subscriber_up.set(1)
                    last_ping=time.monotonic();pending_ping=None
                    # On subscribe/reconnect always resync: Redis Pub/Sub can lose notifications.
                    for connection in self.clients.values():self.signal(connection)
                    while not self.stopping:
                        message=await subscription.get_message(timeout=1)
                        now=time.monotonic()
                        if message and message['type']=='pong':
                            pending_ping=None
                            self.metrics.notifications.labels('subscriber_pong').inc()
                        if pending_ping is not None and now-pending_ping>5:raise TimeoutError('Subscription PONG deadline')
                        if now-last_ping>=15 and pending_ping is None:
                            await asyncio.wait_for(subscription.ping(), 3)
                            last_ping=now;pending_ping=now
                        if not message or message['type']!='message':continue
                        try:
                            payload=json.loads(message['data'])
                            if not isinstance(payload,dict) or not isinstance(payload.get('tenant'),str) or not isinstance(payload.get('device'),str):raise ValueError()
                        except (ValueError,TypeError):
                            self.metrics.notifications.labels('invalid_notification').inc()
                            continue
                        for connection in tuple(self.by_device.get((payload['tenant'],payload['device']),{}).values()):
                            self.signal(connection)
            except asyncio.CancelledError:raise
            except Exception as exc:
                if self.stopping:break
                self.metrics.subscriber_up.set(0)
                self.metrics.subscriber_errors.labels(type(exc).__name__).inc()
                logging.warning('Gateway notification subscriber failed component=redis_pubsub error_type=%s',type(exc).__name__)
                await asyncio.sleep(random.uniform(.5,2))

    async def reconcile(self):
        while not self.stopping:
            await asyncio.sleep(25+random.uniform(0,5))
            clients=tuple(self.clients.values())
            if not clients:continue
            def read():
                with self.store.transaction() as db:
                    # Device IDs are globally unique. One batched query validates all local sockets.
                    return {row['id']:dict(row) for row in db.execute(
                        'SELECT id,tenant,token_hash,session,revoked,revision FROM devices WHERE id=ANY(%s)',
                        (list({c.device['id'] for c in clients}),))}
            try:
                rows=await run_in_threadpool(read)
                for connection in clients:
                    row=rows.get(connection.device['id'])
                    if not row or row['revoked'] or row['token_hash']!=connection.device['token_hash'] or row['session']!=connection.session:
                        connection.invalid=True
                    if connection.invalid or row['revision']!=connection.revision:self.signal(connection)
                self.metrics.notifications.labels('batch_reconcile').inc()
            except asyncio.CancelledError:raise
            except Exception:self.metrics.notifications.labels('reconcile_error').inc()
