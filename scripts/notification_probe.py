"""Exercise the actual WebSocket transport on an isolated synthetic device."""
import asyncio
import json
import uuid
from pathlib import Path
from urllib.request import Request,urlopen
import websockets


def call(path,body):
    token=Path('/run/secrets/admin_token').read_text().strip()
    req=Request('http://127.0.0.1:8080'+path,data=json.dumps(body).encode(),headers={
        'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urlopen(req,timeout=10) as response:return json.load(response)


async def probe():
    from rpa_gateway.client import GatewayClient
    tenant='ws_'+uuid.uuid4().hex[:12]
    device='ws_sim_'+uuid.uuid4().hex
    registration=call(f'/v1/admin/{tenant}/devices',{'device_id':device,
        'bindings':[{'inbox_id':'test','room_id':'test'}]})
    try:
        client=GatewayClient('http://127.0.0.1:8080',registration['token'],'ws_install','synthetic',['text_v1','official_identity_v1'])
        hello=client.hello()
        async with websockets.connect('ws://127.0.0.1:8080/v1/device/notifications') as ws:
            await ws.send(json.dumps({'token':registration['token'],'session_id':hello['session_id']}))
            initial=json.loads(await asyncio.wait_for(ws.recv(),5))
            assert initial['type']=='tasks_changed' and initial['tasks']==[]
            await ws.send('ping')
            assert json.loads(await asyncio.wait_for(ws.recv(),5))['type']=='pong'
        print(json.dumps({'passed':True,'real_websocket':True,'authenticated':True,'notification_only':True,'ping_pong':True}))
    finally:
        call(f'/v1/admin/{tenant}/devices/{device}/revoke',{})

asyncio.run(probe())
