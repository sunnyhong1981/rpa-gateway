import io
import os
import uuid
import json
from pathlib import Path
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo, conninfo_to_dict
import pytest
from fastapi.testclient import TestClient
from PIL import Image
from rpa_gateway.app import Settings, create_app

class Clock:
    def __init__(self): self.now=1793200000.0
    def __call__(self): return self.now
    def advance(self, seconds): self.now+=seconds

@pytest.fixture
def database_dsn():
    path=os.environ.get('RPA_GATEWAY_TEST_DSN_FILE')
    if not path:
        pytest.fail('RPA_GATEWAY_TEST_DSN_FILE must point to a private PostgreSQL test credential')
    dsn=Path(path).read_text().strip()
    if conninfo_to_dict(dsn).get('dbname')!='rpa_gateway_test':
        pytest.fail('Tests require the dedicated rpa_gateway_test database')
    schema='test_'+uuid.uuid4().hex
    with psycopg.connect(dsn,autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        try:
            yield make_conninfo(dsn, options='-c search_path='+schema)
        finally:
            admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))

@pytest.fixture
def env(database_dsn):
    clock=Clock()
    objects=json.loads(Path(os.environ.get('RPA_GATEWAY_TEST_OBJECT_CONFIG','runtime/scale-local/objects.json')).read_text())
    objects['prefix']='tests/'+uuid.uuid4().hex
    settings=Settings(database_dsn,'a'*48,'b'*48,'v'*48,redis_url=os.environ.get('RPA_GATEWAY_TEST_REDIS_URL','redis://127.0.0.1:36379/0'),
        redis_namespace='test_'+uuid.uuid4().hex,object_config=objects,embedded_worker=True,allow_media_proxy=True)
    app=create_app(settings,clock)
    with TestClient(app) as client:
        yield Harness(client,settings,clock,app)

class Harness:
    def __init__(self,client,settings,clock,app):
        self.client,self.settings,self.clock,self.app=client,settings,clock,app
        self.admin={'Authorization':'Bearer '+settings.admin_token}
        self.backend={'Authorization':'Bearer '+settings.backend_token}
        self.verifier={'Authorization':'Bearer '+settings.verifier_token}
        self.counter=0

    def register(self,tenant='test',device='desktop',caps=None,ready=True):
        response=self.client.post(f'/v1/admin/{tenant}/devices',headers=self.admin,json={
            'device_id':device,'bindings':[{'inbox_id':'inbox1','room_id':'room1'},{'inbox_id':'inbox2','room_id':'room2'}]})
        assert response.status_code==200,response.text
        token=response.json()['token']
        headers={'Authorization':'Bearer '+token}
        hello={'protocol':1,'installation_id':'install_'+device,'runtime_version':'test-1',
               'capabilities':caps if caps is not None else ['text_v1','caption_images_v1','official_identity_v1']}
        response=self.client.post('/v1/device/hello',headers=headers,json=hello)
        assert response.status_code==200,response.text
        result={'tenant':tenant,'device':device,'headers':headers,'session_id':response.json()['session_id'],'hello':hello}
        if ready:self.heartbeat(result)
        return result

    def heartbeat(self,d,**overrides):
        body=dict(session_id=d['session_id'],worker_alive=True,desktop_unlocked=True,wecom_logged_in=True,auto_send=True)
        body.update(overrides)
        return self.client.post('/v1/device/heartbeat',headers=d['headers'],json=body)

    def image(self,tenant='test',kind='PNG',color='red'):
        buffer=io.BytesIO()
        Image.new('RGB',(12,12),color).save(buffer,format=kind)
        data=buffer.getvalue()
        response=self.client.post(f'/v1/backend/{tenant}/media',headers={**self.backend,'Content-Type':'image/png'},content=data)
        assert response.status_code==200,response.text
        return response.json(),data

    def enqueue(self,d,**overrides):
        self.counter+=1
        payload=dict(device_id=d['device'],inbox_id='inbox1',room_id='room1',source_message_id=f'source{self.counter}',
                     waybill='JSE_TEST',recipient_id='official_member',robot_id='robot_test',caption='运单号：JSE_TEST 签收图片',media_ids=[])
        payload.update(overrides)
        return self.client.post(f"/v1/backend/{d['tenant']}/tasks",headers=self.backend,json=payload)

    def claim(self,d):
        return self.client.post('/v1/device/tasks/claim',headers=d['headers'],json={'session_id':d['session_id']})

    def receipt(self,d,task,state,**overrides):
        self.counter+=1
        body={'session_id':d['session_id'],'lease_token':task['lease_token'],'event_id':f'event{self.counter}','state':state}
        body.update(overrides)
        return self.client.post(f"/v1/device/tasks/{task['id']}/receipts",headers=d['headers'],json=body)

    def inspect(self,d,task):
        return self.client.get(f"/v1/admin/{d['tenant']}/tasks/{task['id']}",headers=self.admin)

    def evidence(self,d,task,media=(),prefix='arc'):
        base={'room_id':'room1','actor_id':'robot_test','occurred_at':self.clock()}
        events=[dict(base,event_id=prefix+'0',kind='text',text=task['payload']['caption'])]
        events.extend(dict(base,event_id=prefix+str(i+1),kind='image',md5=m['md5'],size=m['size']) for i,m in enumerate(media))
        return {'events':events}

    def confirm(self,d,task,body):
        return self.client.post(f"/v1/verifier/{d['tenant']}/tasks/{task['id']}/confirm",headers=self.verifier,json=body)
