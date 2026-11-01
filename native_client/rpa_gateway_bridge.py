"""Optional gateway queue for text/POD replies; native files/notifications retain old routing."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import math
import time
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parent/'gateway_vendor'))
from gateway_transport.client import GatewayClient,GatewayError,GatewaySessionError,GatewayOwnershipError
from gateway_transport.source_policy import eligible_reply
from rpa_chatwoot import Bridge as LegacyBridge
from rpa_member_identity import valid_name
from scripts.wecom_session_health import runtime_snapshot


class GatewayBridge(LegacyBridge):
    def __init__(self,config,state_path):
        super().__init__(config,state_path)
        saved=Path(config['gateway_config_path'])
        settings=json.loads(saved.read_text(encoding='utf-8-sig'))
        if str(settings['inbox_id'])!=str(config['inbox_id']):raise ValueError('Gateway inbox scope differs')
        self.gateway=GatewayClient(settings['base_url'],settings['device_token'],settings['installation_id'],
            settings['runtime_version'],['text_v1','caption_images_v1','official_identity_v1'],ca_file=settings.get('ca_file'))
        self._announced_ready=False
        self.route={'mode':'gateway','floor':0,'generation':0}
        try:
            self.gateway.hello()
            self.route=self.gateway.request('/v1/device/source-route?inbox_id='+str(config['inbox_id'])+'&session_id='+self.gateway.session_id)
            self.gateway.request('/v1/device/source-route/'+str(config['inbox_id'])+'/ready',{'session_id':self.gateway.session_id})
            self._announced_ready=True
        except GatewayOwnershipError:
            raise
        except Exception:
            # A gateway outage must not prevent the other legacy groups starting.
            # Keep routed text/photos held; normal polling retries the connection.
            pass
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS gateway_attempts (
          scope TEXT NOT NULL,message_id INTEGER NOT NULL,task_id TEXT NOT NULL,lease_token TEXT NOT NULL,
          session_id TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL,intent_event TEXT NOT NULL,
          submit_event TEXT NOT NULL,entered_at REAL,receipt_error TEXT,PRIMARY KEY(scope,message_id))''')
        # Preserve the old journal's column layout for rollback without restoring state.
        self.db.execute('''CREATE TABLE IF NOT EXISTS gateway_desktop_releases (
          scope TEXT NOT NULL,message_id INTEGER NOT NULL,verified INTEGER NOT NULL DEFAULT 0,
          released INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,message_id))''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS gateway_quarantine_releases (scope TEXT NOT NULL,message_id INTEGER NOT NULL,released INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,message_id))''')
        self.db.commit()
        self.active_gateway_message=None
        self.next_heartbeat=0;self.next_source_poll=0;self.next_reconcile=0;self.next_route=0
        self.cached_legacy=[];self.dirty=threading.Event();self.dirty.set()
        self.stop_event=threading.Event()
        self.thread=threading.Thread(target=self._notifications,name='rpa-gateway-notifications',daemon=True)
        self.thread.start()

    def _notifications(self):
        async def listen():
            previous=None
            async for message in self.gateway.notifications(self.stop_event):
                self.notice_has_queue=any(row.get('state')=='queued' for row in message.get('tasks',[]))
                cursor=message.get('cursor')
                if cursor!=previous:self.dirty.set();previous=cursor
                if self.stop_event.is_set():break
        try:asyncio.run(listen())
        except Exception:self.dirty.set()

    def _row(self,message_id):
        return self.db.execute('''SELECT a.*,COALESCE(r.verified,0) desktop_verified,COALESCE(r.released,0) desktop_released,COALESCE(q.released,0) quarantine_released
          FROM gateway_attempts a LEFT JOIN gateway_desktop_releases r ON r.scope=a.scope AND r.message_id=a.message_id LEFT JOIN gateway_quarantine_releases q ON q.scope=a.scope AND q.message_id=a.message_id
          WHERE a.scope=? AND a.message_id=?''',(self.scope,message_id)).fetchone()

    def _task(self,row):return json.loads(row['payload'])

    def _receipt(self,row,state,event_id):
        return self.gateway.request('/v1/device/tasks/'+row['task_id']+'/receipts',{
            'session_id':row['session_id'],'lease_token':row['lease_token'],'event_id':event_id,'state':state})

    def _set(self,message_id,state,error=None):
        with self.db:
            self.db.execute('UPDATE gateway_attempts SET state=?,receipt_error=? WHERE scope=? AND message_id=?',(state,error,self.scope,message_id))

    def _mirror(self,message_id,state,receipt):
        with self.db:self.db.execute('UPDATE outgoing SET state=?,receipt=? WHERE scope=? AND message_id=?',(state,receipt,self.scope,message_id))

    def poll(self):
        now=time.monotonic()
        gateway_replies=[]
        health=runtime_snapshot()
        ready=health.get('state')=='logged_in'
        self.gateway_health=health
        try:
            if now>=self.next_route:
                if self.gateway.session_id is None:self.gateway.hello()
                self.route=self.gateway.request('/v1/device/source-route?inbox_id='+str(self.config['inbox_id'])+'&session_id='+self.gateway.session_id)
                if not self._announced_ready:
                    self.gateway.request('/v1/device/source-route/'+str(self.config['inbox_id'])+'/ready',{'session_id':self.gateway.session_id})
                    self._announced_ready=True
                self.next_route=now+60
            if now>=self.next_heartbeat:
                self.gateway.heartbeat(worker_alive=True,desktop_unlocked=ready,wecom_logged_in=ready,
                                       auto_send=self.config.get('gateway_auto_send',False))
                self.next_heartbeat=now+15
            changed=self.dirty.is_set()
            self._gateway_notice_pending=changed
            self.dirty.clear()
            # Login uncertainty pauses native dispatch, not source ingestion.
            if ready:gateway_replies=self._poll_gateway(changed,now)
        except GatewaySessionError:
            self.gateway.session_id=None
            self._announced_ready=False
            self.next_route=self.next_heartbeat=0
        except GatewayError as error:
            self.gateway_last_error=type(error).__name__
        if gateway_replies and self.next_source_poll==0:self.next_source_poll=now+25
        if now>=self.next_source_poll or self.dirty.is_set():
            self.cached_legacy=super().poll()
            self.next_source_poll=now+25
        else:self.flush_image_batch_receipts()
        replies=list(gateway_replies)
        for reply in self.cached_legacy:
            current=self.db.execute('SELECT state FROM outgoing WHERE scope=? AND message_id=?',(self.scope,reply['message_id'])).fetchone()
            if not current or current['state']!='queued':continue
            routed=(self.route['mode']=='gateway' and reply['message_id']>self.route['floor'] and eligible_reply(reply))
            if not routed:replies.append(reply)
        return sorted(replies,key=lambda r:r['message_id'])

    def review_blocks_dispatch(self, message_id):
        row=self._row(message_id)
        return not (row and row['state']=='unknown' and row['quarantine_released'])

    def needs_desktop_recovery(self):
        return bool(self.db.execute("SELECT 1 FROM gateway_attempts a LEFT JOIN gateway_quarantine_releases r ON r.scope=a.scope AND r.message_id=a.message_id WHERE a.scope=? AND a.state='unknown' AND COALESCE(r.released,0)=0",(self.scope,)).fetchone())

    def release_quarantined_attempts(self):
        # Called only after the native adapter has independently verified the
        # exact group, an empty composer and no image/file preview dialog.
        if runtime_snapshot().get('state')!='logged_in':return
        rows=self.db.execute("SELECT * FROM gateway_attempts WHERE scope=? AND state='unknown'",(self.scope,)).fetchall()
        for row in rows:
            if self._row(row['message_id'])['quarantine_released']:continue
            task=self.gateway.request('/v1/device/tasks/'+row['task_id'])
            if task['state']!='unknown':continue
            released_at=task.get('quarantine_released_at')
            if released_at is not None:
                if type(released_at) not in (int,float) or not math.isfinite(released_at) or released_at<=0:
                    raise RuntimeError('Invalid authoritative quarantine recovery receipt')
                # A committed release can lose its HTTP acknowledgement before
                # SQLite is updated. Adopt its audit event after native checks;
                # ordinary desktop_released_at is deliberately insufficient.
                with self.db:self.db.execute('INSERT INTO gateway_quarantine_releases VALUES(?,?,1) ON CONFLICT(scope,message_id) DO UPDATE SET released=1',(self.scope,row['message_id']))
                continue
            if time.time()-task['updated']<300:continue
            result=self.gateway.request('/v1/device/tasks/'+row['task_id']+'/receipts',{
                'session_id':self.gateway.session_id,'lease_token':row['lease_token'],
                'event_id':'quarantine-release-'+row['intent_event']+'-'+self.gateway.session_id,'state':'quarantine_released',
                'desktop_idle_verified':True,'input_cleared':True,'group_verified':True,
                'reason':'Fresh native idle/group verification; uncertain task remains in audit without retry'})
            if result['state']=='unknown':
                with self.db:self.db.execute('INSERT INTO gateway_quarantine_releases VALUES(?,?,1) ON CONFLICT(scope,message_id) DO UPDATE SET released=1',(self.scope,row['message_id']))

    def _poll_gateway(self,changed,now):
        replies=[]
        leased=self.db.execute("SELECT * FROM gateway_attempts WHERE scope=? AND state='leased' ORDER BY message_id LIMIT 1",(self.scope,)).fetchone()
        if leased:
            try:
                self._receipt(leased,'renew',uuid.uuid4().hex)
                replies.append(self._reply(self._task(leased)))
            except GatewayError:
                # Only the pre-fence lease can be relinquished. A fenced journal never enters this branch.
                self._set(leased['message_id'],'cancelled','Pre-submit lease no longer live')
        elif self.route['mode']=='gateway' and ((changed and getattr(self,'notice_has_queue',True)) or now>=getattr(self,'next_claim',0)):
            self.next_claim=now+25
            task=self.gateway.claim()
            if task:
                context=task['payload'].get('context') or {}
                message_id=context.get('chatwoot_message_id')
                if (context.get('provider')!='chatwoot_official' or type(message_id) is not int
                    or str(task['payload']['inbox_id'])!=str(self.config['inbox_id'])
                    or task['payload']['room_id']!=self.config['archive_group_id']):
                    raise RuntimeError('Gateway task is not an authorized Chatwoot source for this group')
                source_state=self.db.execute('SELECT state FROM outgoing WHERE scope=? AND message_id=?',(self.scope,message_id)).fetchone()
                if source_state and source_state['state'] in ('delivered','discarded','unknown','confirming','sending'):
                    self.gateway.receipt(task,'failed_pre_submit',reason='Source already handled or under review in the legacy journal')
                    return replies
                previous=self._row(message_id)
                if previous and previous['state'] not in ('leased','cancelled'):
                    raise RuntimeError('Existing submission journal prevents another Enter')
                with self.db:
                    self.db.execute('INSERT OR IGNORE INTO outgoing(scope,message_id,content) VALUES(?,?,?)',(self.scope,message_id,task['payload']['caption']))
                    self.db.execute('''INSERT INTO gateway_attempts(scope,message_id,task_id,lease_token,session_id,payload,state,intent_event,submit_event,entered_at,receipt_error) VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL)
                      ON CONFLICT(scope,message_id) DO UPDATE SET task_id=excluded.task_id,lease_token=excluded.lease_token,
                      session_id=excluded.session_id,payload=excluded.payload,state=excluded.state,intent_event=excluded.intent_event,
                      submit_event=excluded.submit_event,entered_at=NULL,receipt_error=NULL''',
                      (self.scope,message_id,task['id'],task['lease_token'],self.gateway.session_id,json.dumps(task),'leased',uuid.uuid4().hex,uuid.uuid4().hex))
                    self.db.execute('DELETE FROM gateway_desktop_releases WHERE scope=? AND message_id=?',(self.scope,message_id))
                replies.append(self._reply(task))
        return replies

    def _reply(self,task):
        payload=task['payload'];context=payload['context'];message_id=context['chatwoot_message_id']
        target=dict(context['reply_target'])
        if (target.get('sender_id')!=payload['recipient_id'] or target.get('room_id')!=self.config['archive_group_id']
            or target.get('identity_status')!='verified' or not str(target.get('identity_origin','')).startswith('official_')
            or not valid_name(target.get('sender_name'))):raise RuntimeError('Official gateway identity does not match task')
        with self.db:
            self.db.execute('INSERT INTO incoming_reply_sources VALUES(?,?,?) ON CONFLICT(scope,message_id) DO UPDATE SET payload=excluded.payload',
                            (self.scope,target['source_message_id'],json.dumps(target,ensure_ascii=False)))
            self.db.execute('INSERT INTO outgoing_reply_targets VALUES(?,?,?) ON CONFLICT(scope,message_id) DO UPDATE SET payload=excluded.payload',
                            (self.scope,message_id,json.dumps(target,ensure_ascii=False)))
        # This is the existing native fast-path for an exact server-verified name, not an OCR nickname mapping.
        target['sender_name_origin']='verified_cache'
        reply={'message_id':message_id,'content':payload['caption'],'reply_target':target}
        if task.get('media'):
            reply['document']={'image_batch':True,'combined':True,'pod_fallback':True,'archive_sender_id':payload['robot_id'],
                               'attachments':[{'id':m['id'],'file_type':'image'} for m in task['media']]}
        return reply

    def prepare_member_reply(self,reply,desktop):
        return True if self._row(reply['message_id']) else super().prepare_member_reply(reply,desktop)

    def begin_send(self,message_id):
        self.active_legacy_message_id=message_id
        row=self._row(message_id)
        self.active_gateway_message=message_id if row else None
        if row:
            if row['state']!='leased':raise RuntimeError('Submission journal is not safely leased')
            self._receipt(row,'renew',uuid.uuid4().hex)
        return super().begin_send(message_id)

    def before_enter(self):
        message_id=self.active_gateway_message
        if message_id is None:return
        row=self._row(message_id)
        if not row or row['state']!='leased':raise RuntimeError('Gateway journal blocks a repeated send key')
        self._set(message_id,'intent_pending')
        task=self._task(row)
        # Network failure or replayed acknowledgement MUST NOT authorize a key press.
        self.gateway.begin_submit(task,row['intent_event'])
        with self.db:
            self.db.execute("UPDATE gateway_attempts SET state='enter_marked',entered_at=? WHERE scope=? AND message_id=?",(time.time(),self.scope,message_id))

    def download_image_batch(self,message_id,document):
        row=self._row(message_id)
        if not row:return super().download_image_batch(message_id,document)
        task=self._task(row)
        directory=self.attachment_directory/'gateway'/task['id'];directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        def download(media):
            from PIL import Image
            import io
            data=self.gateway.download(task,media)
            if len(data)>5*1024*1024:raise ValueError('Original exceeds the native 5 MiB limit')
            with Image.open(io.BytesIO(data)) as image:
                expected={'image/jpeg':'JPEG','image/png':'PNG'}.get(media['mime'])
                if image.format!=expected:raise ValueError('Original MIME differs from actual bytes')
                image.verify()
            suffix='.jpg' if media['mime']=='image/jpeg' else '.png'
            path=directory/(media['id']+suffix);path.write_bytes(data);path.chmod(0o600)
            return {'path':str(path),'sha256':media['sha256'],'md5':media['md5'],'size':media['size'],'image':True,'attachment_id':media['id']}
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:files=list(executor.map(download,task['media']))
        return {'image_batch':True,'files':files,'archive_sender_id':task['payload']['robot_id'],'combined':True,'caption':task['payload']['caption']}

    def verify_desktop_submission(self):
        row=self._row(self.active_gateway_message)
        if not row or row['state']!='enter_marked' or row['entered_at'] is None:
            raise RuntimeError('Native post-submit verification has no durable Enter boundary')
        with self.db:self.db.execute('''INSERT INTO gateway_desktop_releases(scope,message_id,verified,released) VALUES(?,?,1,0)
          ON CONFLICT(scope,message_id) DO UPDATE SET verified=1,released=0''',(self.scope,row['message_id']))

    def _report_submitted(self,row):
        row=self._row(row['message_id'])
        self._receipt(row,'submitted',row['submit_event'])
        if row['desktop_verified'] and not row['desktop_released']:
            self.gateway.request('/v1/device/tasks/'+row['task_id']+'/receipts',{
                'session_id':row['session_id'],'lease_token':row['lease_token'],
                'event_id':'release-'+row['submit_event'],'state':'desktop_released',
                'native_submit_complete':True,'input_cleared':True,'group_verified':True})
            with self.db:self.db.execute('UPDATE gateway_desktop_releases SET released=1 WHERE scope=? AND message_id=?',
                                        (self.scope,row['message_id']))

    def _submitted(self,message_id):
        row=self._row(message_id)
        if row['state']!='enter_marked':raise RuntimeError('A native Enter was not fenced in the durable journal')
        self._set(message_id,'submitted')
        self._mirror(message_id,'confirming','Gateway submitted once; official archive pending')
        try:self._report_submitted(self._row(message_id))
        except Exception as error:self._set(message_id,'submitted',type(error).__name__)
        self.active_gateway_message=None

    def submit_image_batch(self,message_id,downloaded,after,receipt):
        if self._row(message_id):return self._submitted(message_id)
        return super().submit_image_batch(message_id,downloaded,after,receipt)

    def finish_send(self,message_id,state,receipt):
        if not self._row(message_id):return super().finish_send(message_id,state,receipt)
        if state=='delivered':return self._submitted(message_id)
        return self._unknown(message_id,receipt)

    def _unknown(self,message_id,reason):
        row=self._row(message_id)
        self._set(message_id,'unknown',reason[:200]);self._mirror(message_id,'unknown','Gateway uncertain; never resend')
        try:self._receipt(row,'unknown','unknown-'+row['intent_event'])
        except Exception:pass
        self.active_gateway_message=None

    def defer_send(self,message_id,receipt):
        row=self._row(message_id)
        if not row:return super().defer_send(message_id,receipt)
        if row['state']!='leased':return self._unknown(message_id,receipt)
        self.active_gateway_message=None
        return super().defer_send(message_id,receipt)

    def fail_image_reply(self,message_id,reason,send_not_attempted=False,downloaded=None):
        row=self._row(message_id)
        if not row:return super().fail_image_reply(message_id,reason,send_not_attempted,downloaded)
        if row['state']=='leased' and send_not_attempted:
            try:self._receipt(row,'failed_pre_submit','failed-'+row['intent_event'])
            except Exception:return self._unknown(message_id,reason)
            self._set(message_id,'cancelled',reason[:200]);self._mirror(message_id,'discarded','Gateway safe pre-submit image failure')
            self.active_gateway_message=None
        else:self._unknown(message_id,reason)

    def flush_image_batch_receipts(self):
        super().flush_image_batch_receipts()
        if not getattr(self,'_gateway_notice_pending',False) and time.monotonic()<getattr(self,'next_reconcile',0):return
        self._gateway_notice_pending=False
        self.next_reconcile=time.monotonic()+15
        rows=self.db.execute("SELECT * FROM gateway_attempts WHERE scope=? AND state IN ('intent_pending','enter_marked','submitted','unknown')",(self.scope,)).fetchall()
        for row in rows:
            try:
                if row['state']=='submitted':
                    try:self._report_submitted(row)
                    except GatewayError:pass
                task=self.gateway.request('/v1/device/tasks/'+row['task_id'])
                if task['state']=='confirmed':
                    self._set(row['message_id'],'confirmed');self._mirror(row['message_id'],'delivered','Verified by gateway official archive bridge')
                    directory=self.attachment_directory/'gateway'/row['task_id']
                    for path in directory.glob('*'):
                        if path.is_file():path.unlink()
                elif task['state']=='unknown':
                    self._set(row['message_id'],'unknown','Gateway task quarantined; no retry')
                    self._mirror(row['message_id'],'unknown','Gateway uncertain; never resend')
                elif row['state'] in ('intent_pending','enter_marked'):
                    self._unknown(row['message_id'],'Recovered fenced journal; no automatic Enter')
            except Exception as error:self._set(row['message_id'],row['state'],type(error).__name__)

    def verify_legacy_file_delivery(self,filename,sha256,after):
        """Read exact official file proof; a native Enter alone never confirms a file."""
        from datetime import datetime
        message_id=self.active_legacy_message_id
        if self._row(message_id):raise RuntimeError('Gateway image/text task cannot use legacy file proof')
        expected=Path(filename)
        if hashlib.sha256(expected.read_bytes()).hexdigest()!=sha256:raise RuntimeError('Spreadsheet changed')
        md5=hashlib.md5(expected.read_bytes()).hexdigest()
        deadline=time.monotonic()+40
        while True:
            page=self.request('GET',self.message_path(self.ensure_link()),params={'before':message_id+1})
            for message in page:
                if int(message['id'])!=message_id:continue
                attrs=message.get('content_attributes') or {}
                if isinstance(attrs,str):attrs=json.loads(attrs)
                proof=attrs.get('rpa_archive_delivery') or {};delivery=attrs.get('rpa_delivery') or {}
                stamp=proof.get('delivered_at','')
                try:occurred=datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp()
                except (TypeError,ValueError):occurred=0
                if (proof.get('room_id')==self.config['archive_group_id'] and proof.get('filename')==expected.name
                    and (proof.get('md5sum') or '').lower()==md5 and proof.get('msgid')
                    and after-2<=occurred<=time.time()+5 and delivery.get('status')=='delivered'
                    and delivery.get('source')=='official_archive_file'):
                    return {'message_id':message_id,'archive_event_id':proof['msgid'],'md5':md5,'sha256':sha256}
            if time.monotonic()>=deadline:raise RuntimeError('Official spreadsheet archive not confirmed; do not resend')
            time.sleep(.5)
