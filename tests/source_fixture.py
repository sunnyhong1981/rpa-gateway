"""Loopback-only Chatwoot fixture plus real isolated PostgreSQL source views."""
from contextlib import contextmanager
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib,hmac,io,json,threading,time
from urllib.parse import urlsplit
from PIL import Image
from psycopg.types.json import Jsonb
from fastapi import HTTPException
from rpa_gateway.source_resources import SourceReader
from rpa_gateway.source_policy import eligible_source,source_fingerprint


class SourceFixture:
    def __init__(self,env,groups=2,devices=1):
        self.env=env;self.store=env.app.state.store;self.lock=threading.Lock();self.messages={};self.files={}
        self.identity_delays={};self.file_delays={};self.file_failures=set();self.receipts=[]
        self.active_files=0;self.max_files=0;self.file_started=threading.Event();self.counter=1000
        with self.store.transaction() as db:
            self.schema=db.execute('SELECT current_schema() name').fetchone()['name']
            db.execute('''CREATE TABLE source_messages(id bigint PRIMARY KEY,inbox_id text NOT NULL,message_type int NOT NULL,
              private boolean NOT NULL,content text NOT NULL,attributes jsonb NOT NULL,attachments jsonb NOT NULL,created_at timestamptz NOT NULL)''')
            db.execute('CREATE INDEX source_message_route_cursor ON source_messages(inbox_id,id)')
            db.execute('CREATE VIEW test_messages AS SELECT * FROM source_messages')
            db.execute('''CREATE TABLE source_archive(seq bigserial PRIMARY KEY,msgid text UNIQUE NOT NULL,room_id text NOT NULL,
              sender_id text NOT NULL,occurred_at double precision NOT NULL,payload jsonb NOT NULL)''')
            db.execute('CREATE VIEW test_archive AS SELECT * FROM source_archive')
        self.reader=SourceReader(env.settings.database,size=1)
        self.previous_validator=self.store.source_validator
        def validate(payload):
            rows=self.reader.read('SELECT * FROM '+self.schema+'.test_messages WHERE id=%s AND inbox_id=%s',
                                  (payload['context']['chatwoot_message_id'],payload['inbox_id']))
            if len(rows)!=1 or not eligible_source(rows[0]) or source_fingerprint(rows[0])!=payload['context']['source_fingerprint']:
                raise HTTPException(409,'Fixture authoritative source changed')
        self.store.source_validator=validate
        fixture=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.handle_call()
            def do_POST(self):self.handle_call()
            def handle_call(self):
                parts=urlsplit(self.path).path.split('/');raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
                try:
                    token=parts[5];inbox=token.removeprefix('fixture_');config=fixture.by_inbox[inbox]
                    prefix='/public/api/v1/inboxes/'+token;suffix=urlsplit(self.path).path[len(prefix):]
                    stamp=self.headers['X-Rpa-Timestamp'];sig=hmac.new(config['hmac_token'].encode(),stamp.encode()+b'.'+suffix.encode()+b'.'+raw,hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(sig,self.headers.get('X-Rpa-Signature','')):raise ValueError('Invalid signature')
                    body=json.loads(raw) if raw else None
                    if suffix=='/member_identity':
                        source=fixture.messages[body['source_message_id']]
                        if source['inbox_id']!=inbox or source['attributes']['rpa_sender_id']!=body['sender_id']:raise ValueError('Source mismatch')
                        time.sleep(fixture.identity_delays.get(source['id'],0))
                        self.json_reply({'status':'verified','source_message_id':source['id'],'sender_id':body['sender_id'],
                            'sender_name':'Member '+inbox,'room_id':body['room_id'],'origin':'official_fixture','verification_version':2})
                    elif '/rpa_file/' in suffix:
                        aid=int(suffix.rsplit('/',1)[1]);message_id=int(suffix.split('/messages/')[1].split('/')[0])
                        if fixture.messages[message_id]['inbox_id']!=inbox:raise ValueError('Source mismatch')
                        with fixture.lock:fixture.active_files+=1;fixture.max_files=max(fixture.max_files,fixture.active_files)
                        fixture.file_started.set()
                        try:
                            time.sleep(fixture.file_delays.get(aid,0))
                            if aid in fixture.file_failures:
                                self.send_error(503);return
                            data=fixture.files[aid];self.send_response(200);self.send_header('Content-Length',str(len(data)))
                            self.send_header('X-Rpa-File-Sha256',hashlib.sha256(data).hexdigest());self.end_headers();self.wfile.write(data)
                        finally:
                            with fixture.lock:fixture.active_files-=1
                    elif suffix.endswith('/rpa_receipt'):
                        mid=int(suffix.split('/messages/')[1].split('/')[0])
                        if fixture.messages[mid]['inbox_id']!=inbox:raise ValueError('Source mismatch')
                        with fixture.lock:fixture.receipts.append({'message_id':mid,**body})
                        self.json_reply({'ok':True,'status':body['status']})
                    else:self.send_error(404)
                except (BrokenPipeError,ConnectionResetError):pass
                except Exception:self.send_error(400)
            def json_reply(self,value):
                data=json.dumps(value).encode();self.send_response(200);self.send_header('Content-Length',str(len(data)))
                self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
        self.http=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.http.daemon_threads=True
        self.thread=threading.Thread(target=self.http.serve_forever,daemon=True);self.thread.start()
        self.configs=[];self.by_inbox={};self.devices=[]
        for i in range(devices):
            bindings=[{'inbox_id':'inbox'+str(g+1),'room_id':'room'+str(g+1)} for g in range(groups) if g%devices==i]
            name='source_device_'+str(i)
            r=env.client.post('/v1/admin/test/devices',headers=env.admin,json={'device_id':name,'bindings':bindings});assert r.status_code==200
            headers={'Authorization':'Bearer '+r.json()['token']}
            hello=env.client.post('/v1/device/hello',headers=headers,json={'protocol':1,'installation_id':'fixture_'+str(i),
                'runtime_version':'fixture','capabilities':['text_v1','caption_images_v1','official_identity_v1']});assert hello.status_code==200
            d={'tenant':'test','device':name,'headers':headers,'session_id':hello.json()['session_id']};self.devices.append(d);env.heartbeat(d)
        for g in range(groups):
            inbox='inbox'+str(g+1);config={'tenant':'test','inbox_id':inbox,'base_url':'http://127.0.0.1:'+str(self.http.server_port),
                'inbox_identifier':'fixture_'+inbox,'contact_source':'source_'+inbox,'public_conversation_id':1,
                'hmac_token':'local_fixture_only_'+inbox,'group_name':'Local group '+inbox,'delivery_client_id':'fixture',
                'archive_group_id':'room'+str(g+1),'archive_sender_id':'robot_test','source_schema':self.schema}
            self.configs.append(config);self.by_inbox[inbox]=config
            with self.store.transaction() as db:
                db.execute("INSERT INTO source_routes VALUES('test',%s,%s,'gateway',0,0,1,%s,%s)",
                           (inbox,self.devices[g%devices]['device'],env.clock(),env.clock()))

    def add(self,group=0,images=(),identity_delay=0,file_delay=0,age=0):
        config=self.configs[group];self.counter+=1;source_id=self.counter;self.counter+=1;mid=self.counter
        attachments=[]
        for i,data in enumerate(images):
            aid=mid*10+i;self.files[aid]=data;self.file_delays[aid]=file_delay
            attachments.append({'id':aid,'file_type':'image','size':len(data),'md5':hashlib.md5(data).hexdigest()})
        source={'id':source_id,'inbox_id':config['inbox_id'],'message_type':0,'private':False,'content':'查 JSELOCAL'+str(mid),
            'attributes':{'rpa_sender_id':'member_'+config['inbox_id'],'rpa_archive_room_id':config['archive_group_id']},'attachments':[]}
        reply={'id':mid,'inbox_id':config['inbox_id'],'message_type':1,'private':False,'content':'运单号：JSELOCAL'+str(mid),
            'attributes':{'in_reply_to':source_id,'rpa_recipient_id':'member_'+config['inbox_id'],
                          **({'rpa_pod':{'provider':'jsexp'}} if images else {})},'attachments':attachments}
        self.identity_delays[source_id]=identity_delay
        with self.store.transaction() as db:
            for message in [source,reply]:
                self.messages[message['id']]=message
                db.execute('INSERT INTO source_messages VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',
                           (message['id'],message['inbox_id'],message['message_type'],False,message['content'],Jsonb(message['attributes']),
                            Jsonb(message['attachments']),datetime.fromtimestamp(self.env.clock()-age,timezone.utc)))
        return reply

    def archive(self,task):
        payload=task['payload'];ids=[]
        records=[{'msgtype':'text','text':{'content':payload['caption']}}]
        with self.store.transaction() as db:
            for mid in payload['media_ids']:
                media=db.execute('SELECT md5,size FROM media WHERE id=%s',(mid,)).fetchone()
                records.append({'msgtype':'image','image':{'md5sum':media['md5'],'filesize':media['size']}})
            for i,event in enumerate(records):
                identity=task['id']+'_'+str(i);ids.append(identity)
                db.execute('INSERT INTO source_archive(msgid,room_id,sender_id,occurred_at,payload) VALUES(%s,%s,%s,%s,%s)',
                           (identity,payload['room_id'],payload['robot_id'],self.env.clock(),Jsonb(event)))
        return ids

    def close(self):
        self.http.shutdown();self.http.server_close();self.thread.join(timeout=2)
        self.reader.close();self.store.source_validator=self.previous_validator


def photo(color='red'):
    stream=io.BytesIO();Image.new('RGB',(32,32),color).save(stream,format='PNG');return stream.getvalue()


def eventually(callback,timeout=5):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        result=callback()
        if result:return result
        time.sleep(.02)
    raise AssertionError('Local fixture condition timed out')
