"""Run in one gateway container, notify another. No real WeCom send."""
import asyncio
import json
import time
import uuid
from pathlib import Path
from urllib.request import Request,urlopen
import websockets

async def main():
    tenant='scale_ws_'+uuid.uuid4().hex[:10];device='scale_ws_sim_'+uuid.uuid4().hex
    admin=(Path('/run/secrets/admin_token')).read_text().strip()
    backend=(Path('/run/secrets/backend_token')).read_text().strip()
    def call(method,path,token,body=None):
        request=Request('http://gateway-a:8080'+path,data=json.dumps(body).encode() if body is not None else None,
                        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method=method)
        with urlopen(request,timeout=10) as response:return json.load(response)
    registration=call('POST',f'/v1/admin/{tenant}/devices',admin,{'device_id':device,'bindings':[{'inbox_id':'synthetic','room_id':'synthetic'}]})
    token=registration['token']
    task=None
    try:
        hello=call('POST','/v1/device/hello',token,{'protocol':1,'installation_id':'scale_ws','runtime_version':'synthetic',
            'capabilities':['text_v1','official_identity_v1']})
        async with websockets.connect('ws://gateway-b:8080/v1/device/notifications') as ws:
            await ws.send(json.dumps({'token':token,'session_id':hello['session_id']}))
            assert json.loads(await asyncio.wait_for(ws.recv(),5))['type']=='tasks_changed'
            start=time.monotonic()
            task=call('POST',f'/v1/backend/{tenant}/tasks',backend,{'device_id':device,'inbox_id':'synthetic','room_id':'synthetic',
                'source_message_id':'cross_instance','waybill':'SYNTHETIC_CROSS_INSTANCE','recipient_id':'synthetic','robot_id':'synthetic',
                'caption':'模拟跨实例通知，不发送企业微信消息'})
            until=time.monotonic()+5
            while True:
                response=json.loads(await asyncio.wait_for(ws.recv(),5))
                if any(row['id']==task['id'] for row in response.get('tasks',[])):break
                if time.monotonic()>until:raise RuntimeError('Cross-instance event timeout')
            delay=(time.monotonic()-start)*1000
            call('POST',f"/v1/admin/{tenant}/tasks/{task['id']}/review",admin,
                 {'action':'cancel_before_submit','reason':'模拟跨实例通知已验证；未操作企微桌面，取消测试任务不重发。'})
        print(json.dumps({'passed':True,'http_instance':'gateway-a','websocket_instance':'gateway-b',
            'notice_ms':round(delay,3),'task_cancelled_before_submit':True,'real_wecom_messages_sent':0}))
    finally:call('POST',f'/v1/admin/{tenant}/devices/{device}/revoke',admin,{})

asyncio.run(main())
