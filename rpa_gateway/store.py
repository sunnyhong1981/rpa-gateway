import hashlib
import json
import secrets
import time
import uuid
import redis
from collections import Counter
from contextlib import contextmanager
from fastapi import HTTPException
from .database import Database

ACTIVE = ('leased', 'submitting', 'submitted', 'unknown')
TERMINAL = ('confirmed', 'failed', 'expired', 'cancelled', 'closed_unconfirmed')
MAX_PENDING_ARCHIVES = 3


def fail(code, message):
    raise HTTPException(code, message)


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


class Store:
    def __init__(self, dsn, clock=time.time, pool_size=8, presence=None, objects=None):
        self.clock = clock
        self.presence = presence
        self.objects = objects
        self.source_validator = None
        self.database = Database(dsn, pool_size=pool_size)
        self.database.initialize()

    def close(self):
        self.database.close()

    @contextmanager
    def transaction(self):
        with self.database.transaction() as db:
            yield db

    def lock_device(self, db, tenant, device_id):
        row = db.execute('SELECT * FROM devices WHERE tenant=%s AND id=%s FOR UPDATE',
                         (tenant, device_id)).fetchone()
        if row is None:
            fail(404, 'Device not found')
        return row

    def notify(self, db, tenant, device, kind='tasks_changed'):
        owner=db.execute('UPDATE devices SET revision=revision+1 WHERE tenant=%s AND id=%s RETURNING revision',
                         (tenant,device)).fetchone()
        db.execute('INSERT INTO outbox(tenant,device,revision,kind,created) VALUES(%s,%s,%s,%s,%s)',
                   (tenant,device,owner['revision'],kind,self.clock()))

    def event(self, db, task, state, event_id=None, details=None):
        db.execute('INSERT INTO events(tenant,task,event_id,at,state,details) VALUES(%s,%s,%s,%s,%s,%s)',
                   (task['tenant'], task['id'], event_id or uuid.uuid4().hex,
                    self.clock(), state, json.dumps(details or {}, sort_keys=True)))
        self.notify(db,task['tenant'],task['device'])

    def transition(self, db, task, state, details=None):
        db.execute('UPDATE tasks SET state=%s,updated=%s WHERE id=%s', (state, self.clock(), task['id']))
        self.event(db, task, state, details=details)

    def sweep(self, db, device=None):
        # Always lock device before task. Background workers skip busy devices;
        # API calls sweep only the device they have already locked.
        now = self.clock()
        due = "((state IN ('queued','leased') AND deadline<=%s) OR (state IN ('leased','submitting','submitted') AND lease_until<=%s))"
        if device is None:
            devices = db.execute(
                "SELECT d.* FROM devices d WHERE EXISTS (SELECT 1 FROM tasks t WHERE t.tenant=d.tenant AND t.device=d.id AND " + due + ") "
                "ORDER BY d.tenant,d.id LIMIT 100 FOR UPDATE OF d SKIP LOCKED", (now,now)).fetchall()
        else:
            devices = [device]
        for owner in devices:
            rows = db.execute("SELECT * FROM tasks WHERE tenant=%s AND device=%s AND " + due + " ORDER BY id FOR UPDATE",
                              (owner['tenant'],owner['id'],now,now)).fetchall()
            for task in rows:
                state = task['state']
                if state in ('queued','leased') and task['deadline'] <= now:
                    self.transition(db, task, 'expired', {'reason':'request_deadline'})
                elif state == 'leased' and task['lease_until'] <= now:
                    self.transition(db, task, 'queued', {'reason':'pre_submit_lease_expired'})
                elif state in ('submitting','submitted') and task['lease_until'] <= now:
                    self.transition(db, task, 'unknown', {'reason':'archive_or_submission_timeout_no_retry'})

    def device(self, token):
        token_hash=digest(token)
        if self.presence:
            try:
                cached=self.presence.cached_device(token_hash)
                if cached:return cached
            except redis.RedisError:pass
        with self.transaction() as db:
            row=db.execute('SELECT * FROM devices WHERE token_hash=%s AND revoked=0',(token_hash,)).fetchone()
            if row is None:fail(401,'Invalid device credential')
            result=dict(row)
        if self.presence:
            try:self.presence.cache_device(result)
            except redis.RedisError:pass
        return result

    def provision(self, tenant, body):
        token = secrets.token_urlsafe(36)
        with self.transaction() as db:
            row = db.execute('INSERT INTO devices(tenant,id,token_hash,scope) VALUES(%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING RETURNING id',
                             (tenant,body.device_id,digest(token),json.dumps(body.model_dump()))).fetchone()
            if row is None:
                fail(409, 'Device already exists; rotate explicitly')
        return {'device_id': body.device_id, 'token': token}

    def credentials(self, tenant, device_id, rotate=False):
        token = secrets.token_urlsafe(36)
        with self.transaction() as db:
            # A device credential is rechecked inside every mutation transaction.
            row = db.execute('SELECT * FROM devices WHERE tenant=%s AND id=%s FOR UPDATE', (tenant, device_id)).fetchone()
            if row is None:
                fail(404, 'Device not found')
            db.execute('UPDATE devices SET token_hash=%s,revoked=%s,session=NULL,seen=NULL WHERE tenant=%s AND id=%s',
                       (digest(token), 0 if rotate else 1, tenant, device_id))
            self.notify(db,tenant,device_id,'credential_changed')
            if self.presence:
                try:self.presence.invalidate(row)
                except redis.RedisError:pass
        return {'device_id': device_id, **({'token': token} if rotate else {'revoked': True})}

    def current_device(self, db, device, lock=False):
        row = db.execute('SELECT * FROM devices WHERE tenant=%s AND id=%s AND token_hash=%s AND revoked=0' + (' FOR UPDATE' if lock else ''),
                         (device['tenant'], device['id'], device['token_hash'])).fetchone()
        if row is None:
            fail(401, 'Device credential revoked or rotated')
        return row

    def session(self, db, device, session_id, ready=False, lock=False):
        row = self.current_device(db, device, lock=lock)
        if row['session'] != session_id:
            fail(409, 'Session superseded; handshake required')
        if ready:
            if self.presence:
                try:is_ready=self.presence.ready(row)
                except redis.RedisError:fail(503,'Online state unavailable; new sends paused')
            else:
                status=json.loads(row['heartbeat'] or '{}')
                is_ready=row['seen'] is not None and self.clock()-row['seen']<=45 and all(
                    status.get(k) is True for k in ('worker_alive','desktop_unlocked','wecom_logged_in','auto_send'))
            if not is_ready:fail(409,'Device not ready or heartbeat stale')
        return row

    def hello(self, device, body):
        with self.transaction() as db:
            row = self.current_device(db, device, lock=True)
            # Persist the same session for retrying handshakes from an installation;
            # a different installation may take over only without active desktop work.
            active = db.execute("SELECT 1 FROM tasks WHERE tenant=%s AND device=%s AND state IN (%s,%s,%s,%s)",
                                (row['tenant'],row['id'],*ACTIVE)).fetchone()
            if row['installation'] != body.installation_id and active:
                fail(409, 'Active or uncertain work prevents installation takeover')
            if row['session'] and row['installation'] == body.installation_id:
                if json.loads(row['hello']) != body.model_dump():
                    fail(409, 'Capabilities cannot change during a session; use credential rotation')
                return {'session_id': row['session'], 'protocol': 1, 'heartbeat_seconds': 15}
            sid = uuid.uuid4().hex
            db.execute('UPDATE devices SET session=%s,installation=%s,hello=%s,heartbeat=NULL,seen=NULL WHERE tenant=%s AND id=%s',
                       (sid, body.installation_id, json.dumps(body.model_dump()),row['tenant'],row['id']))
            self.notify(db,row['tenant'],row['id'],'session_changed')
            if self.presence:
                try:self.presence.invalidate(row)
                except redis.RedisError:pass
            return {'session_id': sid, 'protocol': 1, 'heartbeat_seconds': 15}

    def heartbeat(self, device, body):
        if self.presence:
            if device['session']!=body.session_id:fail(409,'Session superseded; handshake required')
            try:self.presence.beat(device,body)
            except redis.RedisError:fail(503,'Online state unavailable; new sends paused')
            return {'accepted':True}
        with self.transaction() as db:
            self.session(db,device,body.session_id,lock=True)
            db.execute('UPDATE devices SET heartbeat=%s,seen=%s WHERE tenant=%s AND id=%s',
                       (json.dumps(body.model_dump()),self.clock(),device['tenant'],device['id']))
        return {'accepted':True}

    def put_media(self, tenant, data, mime, _verified=None):
        from .objects import verify_image
        meta=_verified or verify_image(data)
        sha=meta['sha256']
        object_key=None
        if self.objects:
            object_key=self.objects.key(tenant,sha)+'-'+uuid.uuid4().hex
            self.objects.put(object_key,data,meta['mime'])
        try:
            with self.transaction() as db:
                inserted=db.execute('INSERT INTO media(tenant,id,sha256,md5,size,mime,data,object_key,created) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant,sha256) DO NOTHING RETURNING id',
                    (tenant,uuid.uuid4().hex,sha,meta['md5'],meta['size'],meta['mime'],None if self.objects else data,object_key,self.clock())).fetchone()
                row=db.execute('SELECT id,sha256,md5,size,mime FROM media WHERE tenant=%s AND sha256=%s',(tenant,sha)).fetchone()
                if object_key and inserted is None:
                    db.execute('INSERT INTO media_gc(object_key,created) VALUES(%s,%s) ON CONFLICT DO NOTHING',(object_key,self.clock()))
                return dict(row)
        except Exception:
            # Commit acknowledgement can be lost after commit. Never delete here:
            # delayed orphan reconciliation checks the database before any object deletion.
            raise

    def create_task(self, tenant, body):
        payload = body.model_dump()
        if len(set(body.media_ids)) != len(body.media_ids):
            fail(422, 'Duplicate media in one task')
        # TTL is not part of identity: retries must not extend the original request deadline.
        identity = [body.inbox_id,body.source_message_id,body.waybill,'caption_images' if body.media_ids else 'text']
        dedup = digest(json.dumps(identity))
        canonical = json.dumps(payload, sort_keys=True)
        with self.transaction() as db:
            old = db.execute('SELECT * FROM tasks WHERE tenant=%s AND dedup=%s', (tenant,dedup)).fetchone()
            if old:
                if old['payload'] != canonical:
                    fail(409, 'Idempotency conflict: original task content differs')
                return self.present(old)
            device = db.execute('SELECT * FROM devices WHERE tenant=%s AND id=%s AND revoked=0 FOR UPDATE', (tenant,body.device_id)).fetchone()
            if device is None:
                fail(404, 'Device not found')
            scope = json.loads(device['scope'])
            if {'inbox_id':body.inbox_id,'room_id':body.room_id} not in scope['bindings']:
                fail(403, 'Inbox or room outside device scope')
            total = 0
            for media_id in body.media_ids:
                row = db.execute('SELECT size FROM media WHERE tenant=%s AND id=%s AND deleting=false FOR SHARE',(tenant,media_id)).fetchone()
                if row is None:
                    fail(422, 'Media missing in this tenant')
                total += row['size']
            if total > 50*1024*1024:
                fail(422, 'Batch exceeds 50 MiB')
            task_id, now = uuid.uuid4().hex, self.clock()
            inserted = db.execute('INSERT INTO tasks(tenant,id,device,dedup,payload,state,created,deadline,updated) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant,dedup) DO NOTHING RETURNING id',
                       (tenant,task_id,body.device_id,dedup,canonical,'queued',now,now+body.ttl_seconds,now)).fetchone()
            if inserted is None:
                existing = db.execute('SELECT * FROM tasks WHERE tenant=%s AND dedup=%s', (tenant,dedup)).fetchone()
                if existing['payload'] != canonical:
                    fail(409, 'Idempotency conflict: original task content differs')
                return self.present(existing)
            task = db.execute('SELECT * FROM tasks WHERE id=%s', (task_id,)).fetchone()
            self.event(db,task,'queued')
            return self.present(task)

    def present(self, task, device_view=False, db=None):
        result = {k: task[k] for k in ('id','tenant','device','state','created','deadline','updated','desktop_released_at')}
        result['payload'] = json.loads(task['payload'])
        if device_view:
            # Immutable expected hashes accompany the task, independent of download headers.
            if db is None:
                raise RuntimeError('Device task serialization requires the current transaction')
            result['media'] = [dict(db.execute('SELECT id,sha256,md5,size,mime FROM media WHERE tenant=%s AND id=%s',
                (task['tenant'],mid)).fetchone()) for mid in result['payload']['media_ids']]
        if device_view:
            result.update(lease_token=task['lease_token'],lease_until=task['lease_until'])
        return result

    def claim(self, device, body):
        with self.transaction() as db:
            row = self.session(db, device, body.session_id, ready=True, lock=True)
            self.sweep(db, row)
            if db.execute("SELECT 1 FROM tasks WHERE tenant=%s AND device=%s AND (state IN ('leased','submitting','unknown') OR (state='submitted' AND desktop_released_at IS NULL))",
                          (row['tenant'],row['id'])).fetchone():
                return {'task': None, 'reason': 'device_busy_or_awaiting_confirmation'}
            pending=db.execute("SELECT count(*) n FROM tasks WHERE tenant=%s AND device=%s AND state='submitted' AND desktop_released_at IS NOT NULL",
                               (row['tenant'],row['id'])).fetchone()['n']
            if pending>=MAX_PENDING_ARCHIVES:
                return {'task':None,'reason':'archive_confirmation_backlog'}
            hello = json.loads(row['hello'])
            caps = set(hello['capabilities'])
            task = db.execute("SELECT * FROM tasks WHERE tenant=%s AND device=%s AND state='queued' ORDER BY created,id LIMIT 1 FOR UPDATE SKIP LOCKED",
                              (row['tenant'],row['id'])).fetchone()
            if task is None:
                return {'task': None, 'reason': 'empty'}
            payload = json.loads(task['payload'])
            required = {'official_identity_v1','caption_images_v1' if payload['media_ids'] else 'text_v1'}
            if not required <= caps:
                return {'task': None, 'reason': 'capability_missing'}
            db.execute("UPDATE tasks SET state='leased',lease_token=%s,lease_until=%s,session=%s,updated=%s WHERE id=%s",
                       (secrets.token_urlsafe(32),min(self.clock()+30,task['deadline']),body.session_id,self.clock(),task['id']))
            task = db.execute('SELECT * FROM tasks WHERE id=%s', (task['id'],)).fetchone()
            self.event(db,task,'leased')
            return {'task': self.present(task,device_view=True,db=db)}

    def owned_task(self, db, device, task_id, lock=False):
        self.current_device(db, device)
        task = db.execute('SELECT * FROM tasks WHERE id=%s AND tenant=%s AND device=%s' + (' FOR UPDATE' if lock else ''),
                          (task_id,device['tenant'],device['id'])).fetchone()
        if task is None:
            fail(404,'Task not found')
        return task

    def receipt(self, device, task_id, body):
        with self.transaction() as db:
            owner = self.session(db,device,body.session_id,lock=True)
            self.sweep(db,owner)
            task = self.owned_task(db,device,task_id,lock=True)
            if task['session'] != body.session_id or task['lease_token'] != body.lease_token:
                fail(409,'Stale lease; never send with this token')
            old = db.execute('SELECT * FROM events WHERE task=%s AND event_id=%s',(task_id,body.event_id)).fetchone()
            receipt_body = body.model_dump()
            receipt_body.pop('lease_token')
            if body.state!='desktop_released':
                for field in ('native_submit_complete','input_cleared','group_verified'):receipt_body.pop(field)
            if old:
                if json.loads(old['details']) != receipt_body:
                    fail(409,'Receipt idempotency conflict')
                return {'state':task['state'],'duplicate':True}
            state, now = task['state'], self.clock()
            if body.state == 'renew':
                if state != 'leased':
                    fail(409,'Only unsubmitted leases can be renewed')
                self.session(db,device,body.session_id,ready=True)
                db.execute('UPDATE tasks SET lease_until=%s,updated=%s WHERE id=%s',(min(now+30,task['deadline']),now,task_id))
            elif body.state == 'submitting':
                if state != 'leased':
                    fail(409,'Submission requires a live unsubmitted lease')
                self.session(db,device,body.session_id,ready=True)
                if db.execute("SELECT 1 FROM tasks WHERE tenant=%s AND device=%s AND state='unknown' AND id<>%s",
                              (owner['tenant'],owner['id'],task_id)).fetchone():
                    fail(409,'Uncertain work requires reconciliation before another native Enter')
                snapshot=json.loads(task['payload'])
                if (snapshot.get('context') or {}).get('source_fingerprint'):
                    if self.source_validator is None:fail(503,'Official source validator unavailable; no Enter allowed')
                    self.source_validator(snapshot)
                # This is the irreversible fence. A timeout after this is unknown, never requeued.
                db.execute("UPDATE tasks SET state='submitting',intent_at=%s,lease_until=%s,updated=%s WHERE id=%s",(now,now+90,now,task_id))
            elif body.state == 'submitted':
                if state != 'submitting':
                    fail(409,'Submit intent must be durably acknowledged before UI Enter')
                db.execute("UPDATE tasks SET state='submitted',submitted_at=%s,lease_until=%s,updated=%s WHERE id=%s",(now,now+90,now,task_id))
            elif body.state == 'desktop_released':
                if not all((body.native_submit_complete,body.input_cleared,body.group_verified)):
                    fail(409,'Native submission, cleared input and exact group must all be verified')
                if state not in ('submitted','confirmed') or task['submitted_at'] is None:
                    fail(409,'Desktop can only be released after a persisted submitted receipt')
                if task['desktop_released_at'] is not None:
                    fail(409,'Desktop release already recorded; retry the original event identity')
                db.execute('UPDATE tasks SET desktop_released_at=%s,updated=%s WHERE id=%s',(now,now,task_id))
            elif body.state == 'unknown':
                if state not in ('submitting','submitted','unknown'):
                    fail(409,'No submission intent exists')
                db.execute("UPDATE tasks SET state='unknown',updated=%s WHERE id=%s",(now,task_id))
            else:
                if state != 'leased':
                    fail(409,'Cannot mark an attempted submission as safe to retry')
                db.execute("UPDATE tasks SET state='failed',updated=%s WHERE id=%s",(now,task_id))
            updated = db.execute('SELECT * FROM tasks WHERE id=%s',(task_id,)).fetchone()
            self.event(db,updated,updated['state'],body.event_id,receipt_body)
            return {'state':updated['state'],'duplicate':False}

    def media_for_device(self, device, task_id, media_id, include_bytes=True):
        with self.transaction() as db:
            task=self.owned_task(db,device,task_id)
            if media_id not in json.loads(task['payload'])['media_ids']:fail(404,'Media not in task')
            if task['state'] not in ACTIVE:fail(409,'Task not active')
            row=db.execute('SELECT * FROM media WHERE tenant=%s AND id=%s AND deleting=false',
                           (device['tenant'],media_id)).fetchone()
            if row is None:fail(410,'Original media has expired')
            result=dict(row)
        if include_bytes and result['data'] is None:
            if not self.objects:fail(503,'Object storage not configured')
            result['data']=self.objects.get(result['object_key'])
            if hashlib.sha256(result['data']).hexdigest()!=result['sha256'] or len(result['data'])!=result['size']:
                fail(502,'Original object integrity mismatch')
        return result

    def download_link(self, device, task_id, media_id):
        row=self.media_for_device(device,task_id,media_id,include_bytes=False)
        if not row['object_key'] or not self.objects:fail(409,'Object migration pending; use authenticated media endpoint')
        return {'url':self.objects.download(row['object_key']), 'expires_in':120,
                **{k:row[k] for k in ('id','sha256','md5','size','mime')}}

    def snapshot(self,device,session_id):
        with self.transaction() as db:
            row=self.session(db,device,session_id)
            tasks=[dict(t) for t in db.execute("SELECT id,state,updated FROM tasks WHERE tenant=%s AND device=%s AND state IN ('queued','leased','submitting','submitted','unknown') ORDER BY created,id LIMIT 100",
                                              (device['tenant'],device['id']))]
            return {'type':'tasks_changed','tasks':tasks,'cursor':row['revision'],'claim_via':'HTTPS'}

    def device_task(self,device,task_id):
        with self.transaction() as db:
            task=self.owned_task(db,device,task_id)
            result=self.present(task)
            result.update(intent_at=task['intent_at'],submitted_at=task['submitted_at'])
            return result

    def lookup_task(self, db, tenant, task_id, sweep=False):
        task = db.execute('SELECT * FROM tasks WHERE tenant=%s AND id=%s',(tenant,task_id)).fetchone()
        if task is None:
            fail(404,'Task not found')
        owner = self.lock_device(db,tenant,task['device'])
        if sweep:
            self.sweep(db,owner)
        return db.execute('SELECT * FROM tasks WHERE tenant=%s AND id=%s FOR UPDATE',(tenant,task_id)).fetchone()

    def get_task(self, tenant, task_id):
        with self.transaction() as db:
            task = self.lookup_task(db,tenant,task_id,sweep=True)
            result = self.present(task)
            result['events'] = [dict(r) for r in db.execute('SELECT seq,at,state,details FROM events WHERE task=%s ORDER BY seq',(task_id,))]
            for event in result['events']:
                event['details'] = json.loads(event['details'])
            return result

    def confirm(self, tenant, task_id, body):
        with self.transaction() as db:
            task = self.lookup_task(db,tenant,task_id)
            if task['state'] not in ('submitting','submitted','unknown','confirmed'):
                fail(409,'Task has no submission intent')
            payload = json.loads(task['payload'])
            events = body.events
            ids = [e.event_id for e in events]
            if len(ids) != len(set(ids)):
                fail(422,'Repeated archive event')
            if any(e.room_id != payload['room_id'] or e.actor_id != payload['robot_id'] or
                   e.occurred_at < task['intent_at']-2 or e.occurred_at > self.clock()+5 or
                   e.occurred_at > task['intent_at']+600 for e in events):
                fail(422,'Archive scope, actor or submission time mismatch')
            text = [e.text for e in events if e.kind=='text']
            allowed=[payload['caption']]
            context=payload.get('context') or {}
            name=(context.get('reply_target') or {}).get('sender_name','')
            if context.get('provider')=='chatwoot_official' and name and len(name)<=120:
                allowed.extend(['@'+name+' '+payload['caption'], '回复 '+name+'：\n'+payload['caption']])
            if len(text)!=1 or text[0] not in allowed:
                fail(422,'Expected exact single caption')
            expected = Counter()
            for media_id in payload['media_ids']:
                media = db.execute('SELECT md5,size FROM media WHERE tenant=%s AND id=%s',(tenant,media_id)).fetchone()
                expected[(media['md5'],media['size'])] += 1
            actual = Counter((e.md5,e.size) for e in events if e.kind=='image')
            if actual != expected:
                fail(422,'Original image count, MD5 or size mismatch')
            for event_id in sorted(ids):
                existing = db.execute('SELECT task FROM archive_claims WHERE tenant=%s AND event_id=%s',(tenant,event_id)).fetchone()
                if existing and existing['task'] != task_id:
                    fail(409,'Archive event already belongs to another task')
            if task['state']=='confirmed':
                claimed = {r['event_id'] for r in db.execute('SELECT event_id FROM archive_claims WHERE task=%s',(task_id,))}
                if claimed != set(ids):
                    fail(409,'Confirmation idempotency conflict')
                return {'state':'confirmed','duplicate':True}
            for event_id in sorted(ids):
                inserted = db.execute('INSERT INTO archive_claims(tenant,event_id,task) VALUES(%s,%s,%s) ON CONFLICT(tenant,event_id) DO NOTHING RETURNING task', (tenant,event_id,task_id)).fetchone()
                if inserted is None:
                    existing = db.execute('SELECT task FROM archive_claims WHERE tenant=%s AND event_id=%s',(tenant,event_id)).fetchone()
                    if existing['task'] != task_id:
                        fail(409,'Archive event already belongs to another task')
            self.transition(db,task,'confirmed',{'archive_event_ids':ids,'original_images':sum(expected.values()),'verifier':'trusted_archive_adapter'})
            return {'state':'confirmed','duplicate':False}

    def review(self, tenant, task_id, body):
        with self.transaction() as db:
            task = self.lookup_task(db,tenant,task_id,sweep=True)
            if body.action=='cancel_before_submit' and task['state'] in ('queued','leased'):
                state='cancelled'
            elif body.action=='close_unknown_without_retry' and task['state']=='unknown':
                state='closed_unconfirmed'
            else:
                fail(409,'Review action is not allowed in this state')
            self.transition(db,task,state,{'reason':body.reason,'action':body.action})
            return {'state':state,'automatically_retried':False}

    def status(self, tenant):
        with self.transaction() as db:
            counts={r['state']:r['n'] for r in db.execute('SELECT state,COUNT(*) n FROM tasks WHERE tenant=%s GROUP BY state',(tenant,))}
            devices=[]
            for row in db.execute('SELECT * FROM devices WHERE tenant=%s',(tenant,)):
                devices.append({'device_id':row['id'],'revoked':bool(row['revoked']),'seen':row['seen'],
                                'stale':row['seen'] is None or self.clock()-row['seen']>45,
                                'health':json.loads(row['heartbeat'] or '{}'),
                                'hello':json.loads(row['hello'] or '{}')})
                if self.presence:
                    try:
                        health=self.presence.health(row)
                        devices[-1].update(health={k:v for k,v in health.items() if k not in ('token_hash','session')},seen=health.get('seen'),stale=not health or self.clock()-health.get('seen',0)>45)
                    except redis.RedisError:devices[-1].update(health={},stale=True)
            return {'mode':'isolated_opt_in','tasks':counts,'devices':devices}
