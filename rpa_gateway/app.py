import asyncio
import hmac
import io
import json
import logging
import os
import random
import time
import tempfile
import uuid
import resource
import sys
import shutil
import psycopg
from psycopg_pool import PoolTimeout
from botocore.exceptions import BotoCoreError, ClientError
import redis
import warnings
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.exceptions import RequestValidationError
from PIL import Image, UnidentifiedImageError
from starlette.concurrency import run_in_threadpool

from .models import Claim, Confirmation, DeviceCreate, Heartbeat, Hello, Receipt, Review, TaskCreate
from .store import Store, fail
from .presence import Presence
from .objects import Objects, verify_image
from .metrics import Metrics, HttpMetrics, CONTENT_TYPE_LATEST
from .notifications import Hub
from .uploads import Uploads
from .models import UploadCreate

MAX_BODY = 16 * 1024 * 1024
MAX_MEDIA = 12 * 1024 * 1024
Image.MAX_IMAGE_PIXELS = 30_000_000


@dataclass(frozen=True)
class Settings:
    database: str = field(repr=False)
    admin_token: str = field(repr=False)
    backend_token: str = field(repr=False)
    verifier_token: str = field(repr=False)
    redis_url: str = field(default='',repr=False)
    source_database_url: str = field(default='',repr=False)
    wecom_webhook: str = field(default='',repr=False)
    alert_channel: str = 'rpa_monitor'
    redis_namespace: str = 'gateway'
    object_config: dict = field(default_factory=dict,repr=False)
    embedded_worker: bool = False
    allow_media_proxy: bool = True
    media_days: int = 30
    event_days: int = 365
    instance_id: str = field(default_factory=lambda:uuid.uuid4().hex)
    database_pool_size: int = 4


    def __post_init__(self):
        tokens = (self.admin_token, self.backend_token, self.verifier_token)
        if any(len(t) < 32 for t in tokens) or len(set(tokens)) != 3:
            raise ValueError('Three distinct credentials of at least 32 characters are required')
        if not 1<=self.database_pool_size<=16:raise ValueError('Invalid database pool budget')

    @classmethod
    def from_env(cls):
        def secret(name):
            path = os.environ.get(name + '_FILE')
            return Path(path).read_text().strip() if path else os.environ.get(name, '')
        return cls(secret('RPA_GATEWAY_DATABASE_URL'),
                   secret('RPA_GATEWAY_ADMIN_TOKEN'),secret('RPA_GATEWAY_BACKEND_TOKEN'),secret('RPA_GATEWAY_VERIFIER_TOKEN'),
                   redis_url=secret('RPA_GATEWAY_REDIS_URL'),
                   source_database_url=secret('RPA_GATEWAY_SOURCE_DATABASE_URL'),
                   wecom_webhook=secret('RPA_GATEWAY_WECOM_WEBHOOK'),
                   redis_namespace=os.environ.get('RPA_GATEWAY_REDIS_NAMESPACE','gateway'),
                   object_config=json.loads(secret('RPA_GATEWAY_OBJECT_CONFIG') or '{}'),
                   embedded_worker=os.environ.get('RPA_GATEWAY_EMBEDDED_WORKER','0')=='1',
                   allow_media_proxy=os.environ.get('RPA_GATEWAY_MEDIA_PROXY','0')=='1',
                   media_days=int(os.environ.get('RPA_GATEWAY_MEDIA_DAYS','30')),
                   event_days=int(os.environ.get('RPA_GATEWAY_EVENT_DAYS','365')),
                   instance_id=os.environ.get('RPA_GATEWAY_INSTANCE_ID',uuid.uuid4().hex),
                   database_pool_size=int(os.environ.get('RPA_GATEWAY_POOL_SIZE','4')))


def make_store(settings,clock=None,pool_size=4):
    presence=Presence(settings.redis_url,settings.redis_namespace,**({'clock':clock} if clock else {})) if settings.redis_url else None
    objects=Objects(settings.object_config) if settings.object_config else None
    store=Store(settings.database,**({'clock':clock} if clock else {}),pool_size=pool_size,presence=presence,objects=objects)
    if settings.source_database_url:
        from .source_policy import validate_live_source
        store.source_validator=lambda payload:validate_live_source(settings.source_database_url,payload)
    return store


class BodyLimit:
    """Bound bytes actually received, including requests without Content-Length."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        # Raw images are streamed and bounded by the dedicated media endpoint.
        if scope.get('path','').endswith('/media') and scope.get('method')=='POST':
            return await self.app(scope,receive,send)
        data = bytearray()
        while True:
            event = await receive()
            if event['type'] == 'http.disconnect':
                return
            data.extend(event.get('body', b''))
            if len(data) > 256*1024:
                response = JSONResponse({'detail':'Control request exceeds 256 KiB'},status_code=413)
                return await response(scope, receive, send)
            if not event.get('more_body',False):
                break
        delivered = False
        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type':'http.request','body':bytes(data),'more_body':False}
            return await receive()
        await self.app(scope,replay,send)


def create_app(settings=None, clock=None):
    settings = settings or Settings.from_env()
    store=make_store(settings,clock,pool_size=settings.database_pool_size)
    metrics=Metrics()
    hub=Hub(store,store.presence,metrics,instance_id=settings.instance_id) if store.presence else None
    uploads=Uploads(store)

    @asynccontextmanager
    async def lifespan(app):
        from .worker import Worker
        jobs=[]
        if hub:await hub.start(settings.redis_url)
        if settings.embedded_worker:
            jobs.append(asyncio.create_task(Worker(store,metrics,settings.media_days,settings.event_days).run()))
        from .health_observer import HealthObserver
        jobs.append(asyncio.create_task(HealthObserver(store, metrics).run()))
        try:yield
        finally:
            for job in jobs:job.cancel()
            await asyncio.gather(*jobs,return_exceptions=True)
            if hub:await hub.close()
            if store.presence:await run_in_threadpool(store.presence.close)
            await run_in_threadpool(store.close)

    app = FastAPI(title='QUNL RPA Gateway',version='0.4.0',docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
    app.add_middleware(BodyLimit)
    app.add_middleware(HttpMetrics,metrics=metrics)
    app.state.store=store
    app.state.metrics=metrics
    app.state.hub=hub
    http_slots=asyncio.Semaphore(64)
    @app.middleware("http")
    async def bounded_http(request,call_next):
        try:await asyncio.wait_for(http_slots.acquire(),timeout=.1)
        except TimeoutError:return JSONResponse({"detail":"Gateway busy"},status_code=503,headers={"Retry-After":"1"})
        try:return await call_next(request)
        finally:http_slots.release()
    decode_slot=asyncio.Semaphore(1)

    async def dependency_failure(request,exc):
        logging.warning('Gateway dependency unavailable: %s',type(exc).__name__)
        return JSONResponse({'detail':'Gateway dependency unavailable; reconcile before retrying a submit'},status_code=503,headers={'Retry-After':'2'})
    for exception in (psycopg.OperationalError,PoolTimeout,redis.RedisError,BotoCoreError,ClientError):
        app.add_exception_handler(exception,dependency_failure)

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        logging.error('Gateway request failed: %s',type(exc).__name__)
        return JSONResponse({'detail':'Internal gateway error'},status_code=500)

    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        # Pydantic's default error includes rejected input; do not echo credentials or message bodies.
        return JSONResponse({'detail':'Invalid request fields'},status_code=422)

    def token(request: Request):
        auth=request.headers.get('authorization','')
        if not auth.startswith('Bearer ') or len(auth)>512 or not auth.isascii():
            fail(401,'Bearer credential required')
        return auth[7:]

    def role(expected):
        def check(value=Depends(token)):
            if not hmac.compare_digest(value,expected):
                fail(401,'Invalid role credential')
        return check

    admin=role(settings.admin_token)
    backend=role(settings.backend_token)
    verifier=role(settings.verifier_token)

    def device(value=Depends(token)):
        current=store.device(value)
        if store.presence:
            try:
                store.presence.check_rate('device',current['id'],10,30)
                store.presence.check_rate('tenant',current['tenant'],500,1000)
            except redis.RedisError:pass  # Receipts still reach PostgreSQL; send readiness separately fails closed.
        return current

    def backend_rate(tenant):
        if store.presence:
            try:store.presence.check_rate('backend',tenant,50,100)
            except redis.RedisError:fail(503,'Admission unavailable; retry with the same idempotency key')


    def tenant_id(tenant):
        import re
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,96}',tenant):
            fail(422,'Invalid tenant')
        return tenant

    @app.get('/healthz')
    def health():
        with store.transaction() as db:
            db.execute('SELECT 1').fetchone()
        return {'service':'rpa-gateway','version':'0.6.0-local','mode':'isolated_opt_in','ready':True,
                'legacy_connected':False,'storage':'postgresql','instance':settings.instance_id}

    @app.post('/v1/admin/alerts/ingest',dependencies=[Depends(admin)])
    def alerts_ingest(body:dict):
        from .alerting import Alerts
        try:return Alerts(store,settings.wecom_webhook,settings.alert_channel).ingest(body)
        except ValueError:fail(422,'Invalid alert batch or robot configuration')

    @app.get('/v1/admin/alerts/status',dependencies=[Depends(admin)])
    def alerts_status():
        with store.transaction() as db:
            rows=db.execute('SELECT state,count(*) n FROM alert_deliveries GROUP BY state').fetchall()
        return {'channel':settings.alert_channel,'configured':bool(settings.wecom_webhook) or settings.alert_channel=='rpa_monitor','counts':{r['state']:r['n'] for r in rows}}

    @app.get('/metrics',dependencies=[Depends(admin)])
    def metric_endpoint():return Response(metrics.render(),media_type=CONTENT_TYPE_LATEST)

    @app.get('/v1/admin/infrastructure',dependencies=[Depends(admin)])
    def infrastructure():
        def probe(fn):
            try:return bool(fn())
            except Exception:return False
        return {'redis':probe(store.presence.ping) if store.presence else False,
                'objects':probe(store.objects.ready) if store.objects else False,
                'pool':store.database.pool.get_stats(),'instance':settings.instance_id}

    @app.get('/openapi.json',dependencies=[Depends(admin)])
    def schema():
        return app.openapi()

    @app.post('/v1/admin/{tenant}/devices',dependencies=[Depends(admin)])
    def provision(tenant:str,body:DeviceCreate):
        return store.provision(tenant_id(tenant),body)

    @app.post('/v1/admin/{tenant}/devices/{device_id}/rotate',dependencies=[Depends(admin)])
    def rotate(tenant:str,device_id:str):
        return store.credentials(tenant_id(tenant),device_id,rotate=True)

    @app.post('/v1/admin/{tenant}/devices/{device_id}/revoke',dependencies=[Depends(admin)])
    def revoke(tenant:str,device_id:str):
        return store.credentials(tenant_id(tenant),device_id)

    @app.get('/v1/admin/{tenant}/status',dependencies=[Depends(admin)])
    def status(tenant:str):
        return store.status(tenant_id(tenant))

    @app.get('/v1/admin/{tenant}/tasks/{task_id}',dependencies=[Depends(admin)])
    def get_task(tenant:str,task_id:str):
        return store.get_task(tenant_id(tenant),task_id)

    @app.post('/v1/admin/{tenant}/tasks/{task_id}/review',dependencies=[Depends(admin)])
    def review(tenant:str,task_id:str,body:Review):
        return store.review(tenant_id(tenant),task_id,body)

    @app.post('/v1/backend/{tenant}/media',dependencies=[Depends(backend)])
    async def upload(tenant:str,request:Request):
        if not settings.allow_media_proxy:fail(409,'Use direct upload authorization or the dedicated media ingress')
        tenant=tenant_id(tenant)
        backend_rate(tenant)
        try:await asyncio.wait_for(decode_slot.acquire(),timeout=.1)
        except TimeoutError:fail(503,'Media validation busy; retry with the same original')
        try:
            # One compatibility ingress at a time per media process; direct S3 uploads bypass this path.
            with tempfile.SpooledTemporaryFile(max_size=1024*1024) as stream:
                size=0
                async for chunk in request.stream():
                    size+=len(chunk)
                    if size>MAX_MEDIA:fail(413,'Original exceeds 12 MiB')
                    stream.write(chunk)
                if not size:fail(413,'Empty image')
                stream.seek(0)
                try:return await run_in_threadpool(store.put_media,tenant,stream.read(),request.headers.get('content-type',''))
                except ValueError:fail(415,'Invalid, truncated or excessive image')
        finally:decode_slot.release()

    @app.post('/v1/backend/{tenant}/media/uploads',dependencies=[Depends(backend)])
    def create_upload(tenant:str,body:UploadCreate):
        backend_rate(tenant)
        return uploads.create(tenant_id(tenant),body)

    @app.post('/v1/backend/{tenant}/media/uploads/{upload_id}/complete',dependencies=[Depends(backend)])
    def complete_upload(tenant:str,upload_id:str):
        backend_rate(tenant)
        return uploads.complete(tenant_id(tenant),upload_id)

    @app.get('/v1/backend/{tenant}/media/uploads/{upload_id}',dependencies=[Depends(backend)])
    def upload_status(tenant:str,upload_id:str):
        backend_rate(tenant)
        return uploads.status(tenant_id(tenant),upload_id)

    @app.post('/v1/backend/{tenant}/tasks',dependencies=[Depends(backend)])
    def task(tenant:str,body:TaskCreate):
        backend_rate(tenant)
        return store.create_task(tenant_id(tenant),body)

    @app.post('/v1/verifier/{tenant}/tasks/{task_id}/confirm',dependencies=[Depends(verifier)])
    def confirm(tenant:str,task_id:str,body:Confirmation):
        return store.confirm(tenant_id(tenant),task_id,body)

    @app.post('/v1/device/hello')
    def hello(body:Hello,current=Depends(device)):
        return store.hello(current,body)

    @app.post('/v1/device/heartbeat')
    def heartbeat(body:Heartbeat,current=Depends(device)):
        return store.heartbeat(current,body)

    @app.post('/v1/device/tasks/claim')
    def claim(body:Claim,current=Depends(device)):
        return store.claim(current,body)

    @app.get('/v1/device/active')
    def active(session_id:str,current=Depends(device)):
        with store.transaction() as db:
            owner=store.session(db,current,session_id,lock=True)
            store.sweep(db,owner)
            rows=db.execute("SELECT * FROM tasks WHERE tenant=%s AND device=%s AND session=%s AND state IN ('leased','submitting','submitted','unknown')",
                            (current['tenant'],current['id'],session_id)).fetchall()
            return {'tasks':[store.present(row,device_view=True,db=db) for row in rows],
                    'instruction':'Resume only leased tasks after renewing; reconcile submitting/submitted/unknown without resending'}

    @app.get('/v1/device/sync')
    def sync(session_id:str,current=Depends(device)):
        return store.snapshot(current,session_id)

    @app.get('/v1/device/tasks/{task_id}/media/{media_id}/download')
    def download_link(task_id:str,media_id:str,current=Depends(device)):
        return store.download_link(current,task_id,media_id)

    @app.get('/v1/device/tasks/{task_id}')
    def task_status(task_id:str,current=Depends(device)):
        return store.device_task(current,task_id)

    @app.get('/v1/device/source-route')
    def source_route(inbox_id:str,session_id:str,current=Depends(device)):
        with store.transaction() as db:
            store.session(db,current,session_id)
            row=db.execute('SELECT tenant,inbox,device,mode,floor,generation FROM source_routes WHERE tenant=%s AND inbox=%s AND device=%s',
                           (current['tenant'],inbox_id,current['id'])).fetchone()
            if row is None:fail(404,'No source route assigned')
            return dict(row)

    @app.post('/v1/device/source-route/{inbox_id}/ready')
    def route_ready(inbox_id:str,body:Claim,current=Depends(device)):
        with store.transaction() as db:
            store.session(db,current,body.session_id,lock=True)
            row=db.execute('UPDATE source_routes SET client_ready_at=%s WHERE tenant=%s AND inbox=%s AND device=%s RETURNING mode,floor,generation',
                           (store.clock(),current['tenant'],inbox_id,current['id'])).fetchone()
            if row is None:fail(404,'No source route assigned')
            return dict(row)

    @app.post('/v1/device/tasks/{task_id}/receipts')
    def receipt(task_id:str,body:Receipt,current=Depends(device)):
        return store.receipt(current,task_id,body)

    @app.get('/v1/device/tasks/{task_id}/media/{media_id}')
    def media(task_id:str,media_id:str,current=Depends(device)):
        if not settings.allow_media_proxy:fail(409,'Use the signed original download endpoint')
        row=store.media_for_device(current,task_id,media_id)
        return Response(row['data'],media_type=row['mime'],headers={
            'ETag':'"'+row['sha256']+'"','X-Content-SHA256':row['sha256'],
            'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})

    @app.websocket('/v1/device/notifications')
    async def notifications(ws:WebSocket):
        await ws.accept()
        slot=None
        try:
            initial=await asyncio.wait_for(ws.receive_text(),timeout=5)
            if len(initial)>1024:return await ws.close(code=1008)
            auth=json.loads(initial)
            current=await run_in_threadpool(store.device,auth['token'])
            sid=auth['session_id']
            await run_in_threadpool(store.snapshot,current,sid) # SQL validates even cached credentials.
            if not hub:return await ws.close(code=1013,reason='Notification service not configured; use sync')
            slot,connection=await hub.add(current,sid)
            async def sender():
                previous=None
                while True:
                    await connection.queue.get()
                    if connection.invalid:return await ws.close(code=1008)
                    try:state=await run_in_threadpool(store.snapshot,current,sid)
                    except HTTPException:return await ws.close(code=1008)
                    connection.revision=state['cursor']
                    if state!=previous:
                        await asyncio.wait_for(ws.send_json(state),timeout=5)
                        previous=state
            async def receiver():
                while True:
                    ping=await asyncio.wait_for(ws.receive_text(),timeout=75)
                    if ping!='ping':return await ws.close(code=1008)
                    try:
                        if not await run_in_threadpool(store.presence.renew_connection,current,slot):return await ws.close(code=1013)
                    except redis.RedisError:return await ws.close(code=1013)
                    await asyncio.wait_for(ws.send_json({'type':'pong'}),timeout=5)
            jobs=[asyncio.create_task(sender()),asyncio.create_task(receiver())]
            try:
                done,pending=await asyncio.wait(jobs,return_when=asyncio.FIRST_COMPLETED)
                for job in done:job.result()
            finally:
                for job in jobs:job.cancel()
                await asyncio.gather(*jobs,return_exceptions=True)
        except (HTTPException,KeyError,ValueError,TypeError):
            try:await ws.close(code=1008)
            except Exception:pass
        except (RuntimeError,TimeoutError):
            try:await ws.close(code=1013)
            except Exception:pass
        except WebSocketDisconnect:pass
        finally:
            if slot:await hub.remove(slot)
    return app


def create_media_app():
    from dataclasses import replace
    return create_app(replace(Settings.from_env(),allow_media_proxy=True))
