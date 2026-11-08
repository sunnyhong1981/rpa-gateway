"""Shared I/O/decode bounds and read-only source connections for preparation workers."""
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from contextlib import contextmanager
import hashlib
import threading
import time
import uuid
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
import redis
from .objects import verify_image


class SourceInfrastructureUnavailable(RuntimeError):
    pass


class SourceReader:
    def __init__(self,dsn,size=1):
        if not 1<=size<=4:
            raise ValueError('Source read pool must stay within its explicit budget')
        def configure(connection):
            connection.execute("SET default_transaction_read_only=on")
            connection.execute("SET statement_timeout='3s'")
            connection.execute("SET lock_timeout='1s'")
        self.pool=ConnectionPool(dsn,min_size=0,max_size=size,max_waiting=32,timeout=4,open=True,
            configure=configure,kwargs={'autocommit':True,'row_factory':dict_row})

    def read(self,query,params=()):
        with self.pool.connection() as db:
            return list(db.execute(query,params))

    def close(self):
        self.pool.close()


ACQUIRE='''
local t=redis.call('TIME');local now=tonumber(t[1])+tonumber(t[2])/1000000
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[1]) then return 0 end
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[2]),ARGV[3]);redis.call('EXPIRE',KEYS[1],math.ceil(tonumber(ARGV[2]))+5)
return 1
'''
RENEW='''
local t=redis.call('TIME');local now=tonumber(t[1])+tonumber(t[2])/1000000
local expires=redis.call('ZSCORE',KEYS[1],ARGV[2])
if not expires or tonumber(expires)<=now then return 0 end
redis.call('ZADD',KEYS[1],now+tonumber(ARGV[1]),ARGV[2]);redis.call('EXPIRE',KEYS[1],math.ceil(tonumber(ARGV[1]))+5)
return 1
'''


class SourceResources:
    def __init__(self,presence,settings):
        if presence is None:
            raise ValueError('Redis is required for shared preparation limits')
        self.redis=presence.redis
        self.prefix=presence.prefix+'source_resources:'
        self.settings=settings
        self.images=ThreadPoolExecutor(max_workers=settings.image_workers,thread_name_prefix='source-image')

    @contextmanager
    def slot(self,kind,budget):
        limit={'network':self.settings.network_slots,'decode':self.settings.decode_slots}[kind]
        key=self.prefix+kind;owner=uuid.uuid4().hex;ttl=max(3,self.settings.lease_seconds)
        while True:
            budget.check()
            try:acquired=self.redis.eval(ACQUIRE,1,key,limit,ttl,owner)
            except redis.RedisError:raise SourceInfrastructureUnavailable('Shared source limits unavailable') from None
            if acquired:break
            time.sleep(.02)
        stopped=threading.Event();lost=threading.Event()
        def renew():
            while not stopped.wait(min(1,ttl/3)):
                try:owned=self.redis.eval(RENEW,1,key,ttl,owner)
                except redis.RedisError:owned=False
                if not owned:lost.set();return
        thread=threading.Thread(target=renew,name='source-resource-renew',daemon=True);thread.start()
        try:
            budget.check()
            yield
            if lost.is_set():raise SourceInfrastructureUnavailable('Shared source slot expired')
            budget.check()
        finally:
            stopped.set();thread.join(timeout=3)
            try:self.redis.zrem(key,owner)
            except redis.RedisError:pass

    def prepare(self,bridge,message,budget):
        attachments=iter(message['attachments']);futures={};result={};index=0
        def image(attachment):
            budget.check()
            with self.slot('network',budget):
                with bridge.http.budget(budget):
                    data,_=bridge.http.attachment(message['id'],attachment['id'])
            if len(data)!=attachment['size'] or hashlib.md5(data).hexdigest()!=attachment['md5']:
                raise ValueError('Signed source attachment manifest differs')
            cached=bridge.store.cached_media(bridge.tenant,hashlib.sha256(data).hexdigest())
            if cached:
                if cached['md5']!=attachment['md5'] or cached['size']!=len(data):
                    raise ValueError('Stored original manifest differs')
                budget.check()
                return cached
            with self.slot('decode',budget):
                meta=verify_image(data)
            budget.check()
            with self.slot('network',budget):
                return bridge.store.put_media(bridge.tenant,data,meta['mime'],_verified=meta)
        def fill():
            nonlocal index
            # One request cannot enqueue its entire nine-image batch ahead of every other request.
            while len(futures)<3:
                try:attachment=next(attachments)
                except StopIteration:return
                future=self.images.submit(image,attachment);futures[future]=index;index+=1
        try:
            fill()
            while futures:
                budget.check()
                done,_=wait(tuple(futures),timeout=.05,return_when=FIRST_COMPLETED)
                for future in done:result[futures.pop(future)]=future.result()
                fill()
            return [result[i] for i in range(index)]
        finally:
            for future in futures:future.cancel()

    def close(self):
        self.images.shutdown(wait=True,cancel_futures=True)
