import asyncio
import hmac
import io
import json
import logging
import os
import warnings
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.exceptions import RequestValidationError
from PIL import Image, UnidentifiedImageError
from starlette.concurrency import run_in_threadpool

from .models import Claim, Confirmation, DeviceCreate, Heartbeat, Hello, Receipt, Review, TaskCreate
from .store import Store, fail

MAX_BODY = 16 * 1024 * 1024
MAX_MEDIA = 12 * 1024 * 1024
Image.MAX_IMAGE_PIXELS = 30_000_000


@dataclass(frozen=True)
class Settings:
    database: str
    admin_token: str
    backend_token: str
    verifier_token: str

    def __post_init__(self):
        tokens = (self.admin_token, self.backend_token, self.verifier_token)
        if any(len(t) < 32 for t in tokens) or len(set(tokens)) != 3:
            raise ValueError('Three distinct credentials of at least 32 characters are required')

    @classmethod
    def from_env(cls):
        def secret(name):
            path = os.environ.get(name + '_FILE')
            return Path(path).read_text().strip() if path else os.environ.get(name, '')
        return cls(os.environ.get('RPA_GATEWAY_DATABASE', '/data/gateway.sqlite3'),
                   secret('RPA_GATEWAY_ADMIN_TOKEN'),secret('RPA_GATEWAY_BACKEND_TOKEN'),secret('RPA_GATEWAY_VERIFIER_TOKEN'))


class BodyLimit:
    """Bound bytes actually received, including requests without Content-Length."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        data = bytearray()
        while True:
            event = await receive()
            if event['type'] == 'http.disconnect':
                return
            data.extend(event.get('body', b''))
            if len(data) > MAX_BODY:
                response = JSONResponse({'detail':'Request exceeds 16 MiB'},status_code=413)
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
    store = Store(settings.database, **({'clock':clock} if clock else {}))

    @asynccontextmanager
    async def lifespan(app):
        async def sweep_loop():
            while True:
                await asyncio.sleep(5)
                try:
                    def sweep():
                        with store.transaction() as db:
                            store.sweep(db)
                    await run_in_threadpool(sweep)
                except Exception:
                    logging.exception('Gateway maintenance failed')
        task = asyncio.create_task(sweep_loop())
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    app = FastAPI(title='QUNL RPA Gateway',version='0.1.0',docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
    app.add_middleware(BodyLimit)
    app.state.store=store
    decode_slot=asyncio.Semaphore(1)

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
        return store.device(value)

    def tenant_id(tenant):
        import re
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,96}',tenant):
            fail(422,'Invalid tenant')
        return tenant

    @app.get('/healthz')
    def health():
        with store.transaction() as db:
            db.execute('SELECT 1').fetchone()
        return {'service':'rpa-gateway','version':'0.1.0','mode':'isolated_opt_in','ready':True,
                'legacy_connected':False}

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
        tenant=tenant_id(tenant)
        data=await request.body()
        if not data or len(data)>MAX_MEDIA:
            fail(413,'Media must be 1 byte to 12 MiB')
        def decode():
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('error',Image.DecompressionBombWarning)
                    with Image.open(io.BytesIO(data)) as image:
                        kind=image.format
                        if kind not in ('PNG','JPEG'):
                            fail(415,'Only original PNG and JPEG are supported')
                        image.verify()
                    # Force full decoding to reject truncated or corrupt images.
                    with Image.open(io.BytesIO(data)) as image:
                        image.load()
            except (UnidentifiedImageError,OSError,ValueError,SyntaxError,Image.DecompressionBombError,Image.DecompressionBombWarning):
                fail(415,'Invalid, truncated or excessive image')
            return store.put_media(tenant,data,{'PNG':'image/png','JPEG':'image/jpeg'}[kind])
        async with decode_slot:
            return await run_in_threadpool(decode)

    @app.post('/v1/backend/{tenant}/tasks',dependencies=[Depends(backend)])
    def task(tenant:str,body:TaskCreate):
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
            store.session(db,current,session_id)
            store.sweep(db)
            rows=db.execute("SELECT * FROM tasks WHERE tenant=? AND device=? AND session=? AND state IN ('leased','submitting','submitted','unknown')",
                            (current['tenant'],current['id'],session_id)).fetchall()
            return {'tasks':[store.present(row,device_view=True) for row in rows],
                    'instruction':'Resume only leased tasks after renewing; reconcile submitting/submitted/unknown without resending'}

    @app.post('/v1/device/tasks/{task_id}/receipts')
    def receipt(task_id:str,body:Receipt,current=Depends(device)):
        return store.receipt(current,task_id,body)

    @app.get('/v1/device/tasks/{task_id}/media/{media_id}')
    def media(task_id:str,media_id:str,current=Depends(device)):
        row=store.media_for_device(current,task_id,media_id)
        return Response(row['data'],media_type=row['mime'],headers={
            'ETag':'"'+row['sha256']+'"','X-Content-SHA256':row['sha256'],
            'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})

    @app.websocket('/v1/device/notifications')
    async def notifications(ws:WebSocket):
        # Tokens never appear in URL/query strings, browser history or access logs.
        await ws.accept()
        try:
            initial=await asyncio.wait_for(ws.receive_text(),timeout=5)
            if len(initial)>1024:
                return await ws.close(code=1008)
            auth=json.loads(initial)
            current=await run_in_threadpool(store.device,auth['token'])
            sid=auth['session_id']
            previous=None
            while True:
                def snapshot():
                    with store.transaction() as db:
                        store.session(db,current,sid)
                        rows=db.execute('SELECT id,state,updated FROM tasks WHERE tenant=? AND device=? ORDER BY created DESC LIMIT 100',
                                        (current['tenant'],current['id'])).fetchall()
                        return [dict(row) for row in rows]
                state=await run_in_threadpool(snapshot)
                if state!=previous:
                    await ws.send_json({'type':'tasks_changed','tasks':state,'claim_via':'HTTPS'})
                    previous=state
                try:
                    ping=await asyncio.wait_for(ws.receive_text(),timeout=1)
                    if ping!='ping':
                        return await ws.close(code=1008)
                    await ws.send_json({'type':'pong'})
                except TimeoutError:
                    pass
        except (HTTPException,KeyError,ValueError,TypeError,TimeoutError):
            await ws.close(code=1008)
        except WebSocketDisconnect:
            pass
    return app
