"""Ephemeral Redis state only. Never grants a durable task lease or send permission."""
import hashlib
import json
import math
import time
import redis
from fastapi import HTTPException

RATE = '''
local t=redis.call('TIME'); local now=tonumber(t[1])+tonumber(t[2])/1000000
local v=redis.call('HMGET',KEYS[1],'tokens','at')
local available=math.min(tonumber(ARGV[2]),tonumber(v[1] or ARGV[2])+math.max(0,now-tonumber(v[2] or now))*tonumber(ARGV[1]))
local allowed=available>=1
if allowed then available=available-1 end
redis.call('HSET',KEYS[1],'tokens',available,'at',now)
redis.call('EXPIRE',KEYS[1],math.ceil(tonumber(ARGV[2])/tonumber(ARGV[1]))+2)
if allowed then return 0 else return math.ceil((1-available)/tonumber(ARGV[1])) end
'''


class Presence:
    def __init__(self,url,namespace,clock=time.time):
        if not namespace or len(namespace)>96 or not all(c.isalnum() or c in '_-' for c in namespace):
            raise ValueError('Invalid Redis namespace')
        self.redis=redis.Redis.from_url(url,decode_responses=True,socket_connect_timeout=1,socket_timeout=1,
                                        max_connections=32,health_check_interval=30)
        self.prefix='rpa:'+namespace+':'
        self.channel=self.prefix+'notify'
        self.clock=clock

    def key(self,kind,*parts):
        return self.prefix+kind+':'+hashlib.sha256(json.dumps(parts,separators=(',',':')).encode()).hexdigest()

    def ping(self):return self.redis.ping()
    def close(self):self.redis.close()

    def cached_device(self,token_hash):
        raw=self.redis.get(self.key('auth',token_hash))
        return json.loads(raw) if raw else None

    def cache_device(self,row):
        self.redis.set(self.key('auth',row['token_hash']),json.dumps(dict(row)),ex=20)

    def invalidate(self,row):
        self.redis.delete(self.key('auth',row['token_hash']),self.key('health',row['tenant'],row['id']))

    def beat(self,row,body):
        # Bind to credential and stable installation session. SQL rechecks every actual send fence.
        value={'session':body.session_id,'token_hash':row['token_hash'],'seen':self.clock(),**body.model_dump()}
        self.redis.set(self.key('health',row['tenant'],row['id']),json.dumps(value),ex=45)

    def health(self,row):
        raw=self.redis.get(self.key('health',row['tenant'],row['id']))
        value=json.loads(raw) if raw else {}
        if value.get('session')!=row['session'] or value.get('token_hash')!=row['token_hash']:
            return {}
        return value

    def ready(self,row):
        value=self.health(row)
        return (0<=self.clock()-value.get('seen',0)<=45 and all(value.get(k) is True
                for k in ('worker_alive','desktop_unlocked','wecom_logged_in','auto_send')))

    def check_rate(self,kind,key,rate,burst):
        retry=self.redis.eval(RATE,1,self.key('rate',kind,key),rate,burst)
        if retry:
            raise HTTPException(429,'Rate limit exceeded',headers={'Retry-After':str(max(1,retry))})

    def acquire_connection(self,device,connection_id):
        script="""
        local t=redis.call('TIME');local now=tonumber(t[1]);redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
        if redis.call('ZCARD',KEYS[1])>=2 then return 0 end
        redis.call('ZADD',KEYS[1],now+90,ARGV[1]);redis.call('EXPIRE',KEYS[1],95);return 1
        """
        return bool(self.redis.eval(script,1,self.key('connections',device['tenant'],device['id']),connection_id))

    def renew_connection(self,device,connection_id):
        script="""
        if not redis.call('ZSCORE',KEYS[1],ARGV[1]) then return 0 end
        local t=redis.call('TIME');redis.call('ZADD',KEYS[1],tonumber(t[1])+90,ARGV[1]);redis.call('EXPIRE',KEYS[1],95);return 1
        """
        return bool(self.redis.eval(script,1,self.key('connections',device['tenant'],device['id']),connection_id))

    def release_connection(self,device,connection_id):
        self.redis.zrem(self.key('connections',device['tenant'],device['id']),connection_id)

    def publish(self,payload):
        return self.redis.publish(self.channel,json.dumps(payload,separators=(',',':')))
