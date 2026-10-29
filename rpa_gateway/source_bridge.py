"""Scoped Chatwoot -> gateway -> official archive bridge. No desktop or history replay."""
import hashlib
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from .app import Settings,make_store
from .chatwoot_http import Chatwoot,SourceUnavailable
from .models import TaskCreate,Confirmation
from .source_policy import eligible_source,source_fingerprint


class SourceBridge:
    def __init__(self,store,source_dsn,config):
        self.store,self.source_dsn,self.config=store,source_dsn,config
        self.http=Chatwoot(config)
        self.tenant=config['tenant'];self.inbox=str(config['inbox_id'])

    def read(self,query,params=()):
        # A separate role can read only two security-barrier test-inbox views.
        with psycopg.connect(self.source_dsn,row_factory=dict_row,connect_timeout=3) as db:
            return list(db.execute(query,params))

    def route(self):
        with self.store.transaction() as db:
            return db.execute('SELECT * FROM source_routes WHERE tenant=%s AND inbox=%s',(self.tenant,self.inbox)).fetchone()

    def capture(self):
        route=self.route()
        if not route or route['mode']=='paused':return
        rows=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id>%s ORDER BY id LIMIT 100',(route['cursor'],))
        with self.store.transaction() as db:
            # One source process holds a leader lock in run(); unique job keys also prevent replay.
            for message in rows:
                if message['message_type']==1 and not message['private'] and not eligible_source(message):
                    self.store.notify(db,self.tenant,route['device'],'legacy_source_changed')
                if eligible_source(message):
                    db.execute('INSERT INTO source_jobs(tenant,inbox,message_id) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                               (self.tenant,self.inbox,message['id']))
            if rows:db.execute('UPDATE source_routes SET cursor=GREATEST(cursor,%s) WHERE tenant=%s AND inbox=%s',
                               (rows[-1]['id'],self.tenant,self.inbox))

    def materialize(self):
        route=self.route()
        if not route or route['mode']!='gateway' or not route['client_ready_at']:return
        with self.store.transaction() as db:
            jobs=db.execute("SELECT * FROM source_jobs WHERE tenant=%s AND inbox=%s AND task IS NULL AND state='pending' AND next_at<=%s AND message_id>%s ORDER BY message_id LIMIT 5",
                            (self.tenant,self.inbox,self.store.clock(),route['floor'])).fetchall()
        for job in jobs:
            try:
                # Recover the source link after a commit acknowledgement is lost, without regenerating task context.
                with self.store.transaction() as db:
                    existing=db.execute("SELECT id FROM tasks WHERE tenant=%s AND payload::jsonb->'context'->>'chatwoot_message_id'=%s LIMIT 1",(self.tenant,str(job['message_id']))).fetchone()
                    if existing:
                        db.execute("UPDATE source_jobs SET task=%s,state='queued' WHERE tenant=%s AND inbox=%s AND message_id=%s",(existing['id'],self.tenant,self.inbox,job['message_id']))
                        continue
                messages=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s',(job['message_id'],))
                if len(messages)!=1 or not eligible_source(messages[0]):raise SourceUnavailable('Source no longer eligible')
                message=messages[0];attrs=message['attributes']
                delivery=attrs.get('rpa_delivery') or {}
                if delivery.get('status') in ('delivered','discarded','cancelled','unknown','sending'):
                    with self.store.transaction() as db:
                        db.execute("UPDATE source_jobs SET state='source_closed',next_at=%s WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.store.clock()+86400,self.tenant,self.inbox,message['id']))
                    continue
                originals=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s',(attrs['in_reply_to'],))
                if len(originals)!=1:raise SourceUnavailable('Exact source not in bound conversation')
                if self.store.clock()-message['created_at'].timestamp()>180:
                    if message['attachments']:
                        self.http.receipt(message['id'],'image_failed',reason='图片在网关提交前等待超时，提供一次原图链接兜底',send_not_attempted=True)
                    else:self.http.receipt(message['id'],'discarded',reason='回复在网关提交前已过期，未发送且不自动重放')
                    with self.store.transaction() as db:
                        db.execute("UPDATE source_jobs SET state='failed',reported_state='failed' WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.tenant,self.inbox,message['id']))
                    continue
                source=originals[0]
                if source['attributes'].get('rpa_sender_id')!=attrs['rpa_recipient_id']:raise SourceUnavailable('Recipient differs from original question')
                identity=self.http.identity(source)
                if identity['room_id']!=self.config['archive_group_id']:raise SourceUnavailable('Official room mismatch')
                caption=(message['content'] or '').replace('**','').strip()
                if not caption:raise SourceUnavailable('Empty source caption')
                fallback=attrs.get('rpa_pod_link_fallback') or {}
                if fallback.get('image_message_id'):
                    with self.store.transaction() as db:
                        previous=db.execute('SELECT t.state FROM tasks t JOIN source_jobs s ON s.task=t.id WHERE s.tenant=%s AND s.inbox=%s AND s.message_id=%s',(self.tenant,self.inbox,fallback['image_message_id'])).fetchone()
                    if previous and previous['state'] in ('submitting','submitted','unknown'):
                        raise SourceUnavailable('Original image awaits authoritative reconciliation')
                    if previous and previous['state']=='confirmed':
                        self.http.receipt(message['id'],'discarded',reason='原图已获得官方存档确认，不重复发送链接兜底')
                        with self.store.transaction() as db:db.execute("UPDATE source_jobs SET state='source_closed' WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.tenant,self.inbox,message['id']))
                        continue
                media=[]
                def download(attachment):
                    data,filename=self.http.attachment(message['id'],attachment['id'])
                    if len(data)!=attachment['size'] or hashlib.md5(data).hexdigest()!=attachment['md5']:
                        raise SourceUnavailable('Signed source attachment differs from registered blob')
                    return self.store.put_media(self.tenant,data,'image/png')
                with ThreadPoolExecutor(max_workers=3) as pool:media=list(pool.map(download,message['attachments']))
                target={'source_message_id':source['id'],'sender_id':identity['sender_id'],'recipient_id':identity['sender_id'],
                    'sender_name':identity['sender_name'],'source_content':source['content'] or '', 'room_id':identity['room_id'],
                    'identity_status':'verified','identity_origin':identity['origin'],'identity_policy':'official_only',
                    'identity_verified_at':identity.get('verified_at'),'identity_verification_version':identity.get('verification_version')}
                found=re.search(r'\bJSE[A-Z0-9]{8,40}\b',caption,re.I)
                waybill=found.group(0).upper() if found else 'message-'+str(message['id'])
                with self.store.transaction() as db:
                    backlog=db.execute("SELECT count(*) n FROM tasks WHERE tenant=%s AND device=%s AND state IN ('queued','leased','submitting','submitted')",(self.tenant,route['device'])).fetchone()['n']
                task=self.store.create_task(self.tenant,TaskCreate(device_id=route['device'],inbox_id=self.inbox,
                    room_id=identity['room_id'],source_message_id=str(message['id']),waybill=waybill,
                    recipient_id=identity['sender_id'],robot_id=self.config['archive_sender_id'],caption=caption,
                    media_ids=[m['id'] for m in media],ttl_seconds=min(3600,max(180,180+30*backlog)),
                    context={'provider':'chatwoot_official','chatwoot_message_id':message['id'],'question_message_id':source['id'],
                        'route_generation':route['generation'],'source_fingerprint':source_fingerprint(message),'reply_target':target,'attachment_ids':[a['id'] for a in message['attachments']]}))
                with self.store.transaction() as db:
                    db.execute("UPDATE source_jobs SET task=%s,state='queued',error_type='' WHERE tenant=%s AND inbox=%s AND message_id=%s",
                               (task['id'],self.tenant,self.inbox,message['id']))
            except Exception as exc:
                with self.store.transaction() as db:
                    db.execute('UPDATE source_jobs SET attempts=attempts+1,error_type=%s,next_at=%s WHERE tenant=%s AND inbox=%s AND message_id=%s',
                               (type(exc).__name__,self.store.clock()+5,self.tenant,self.inbox,job['message_id']))

    def reconcile(self):
        with self.store.transaction() as db:
            rows=db.execute("SELECT t.*,s.message_id,s.reported_state FROM tasks t JOIN source_jobs s ON s.task=t.id WHERE s.tenant=%s AND s.inbox=%s AND (t.state IN ('submitting','submitted','unknown') OR (t.state='confirmed' AND s.reported_state IS DISTINCT FROM 'delivered') OR (t.state IN ('failed','expired') AND s.reported_state IS DISTINCT FROM 'failed')) ORDER BY t.created LIMIT 30",
                            (self.tenant,self.inbox)).fetchall()
        for task in rows:
            payload=json.loads(task['payload'])
            if task['state'] in ('failed','expired'):
                try:
                    if payload['media_ids']:
                        self.http.receipt(task['message_id'],'image_failed',reason='网关未提交签收图，提供一次原图链接兜底',send_not_attempted=True)
                    else:
                        self.http.receipt(task['message_id'],'discarded',reason='网关任务开始前已到期，未发送，不自动重放')
                    with self.store.transaction() as db:
                        db.execute("UPDATE source_jobs SET reported_state='failed',state='failed' WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.tenant,self.inbox,task['message_id']))
                except SourceUnavailable:pass
                continue
            if task['state']!='confirmed':
                events=self.read('SELECT * FROM rpa_gateway_source.test_archive WHERE occurred_at>=%s AND occurred_at<=%s ORDER BY seq LIMIT 100',
                                 (task['intent_at']-2,min(task['intent_at']+600,self.store.clock()+5)))
                used=set()
                with self.store.transaction() as db:
                    used={row['event_id'] for row in db.execute('SELECT event_id FROM archive_claims WHERE tenant=%s',(self.tenant,))}
                    expected=[dict(row) for mid in payload['media_ids'] for row in db.execute('SELECT md5,size FROM media WHERE tenant=%s AND id=%s',(self.tenant,mid))]
                target=(payload.get('context') or {})['reply_target'];name=target['sender_name']
                allowed={payload['caption'],'@'+name+' '+payload['caption'],'回复 '+name+'：\n'+payload['caption']}
                captions=[event for event in events if event['msgid'] not in used and event['payload'].get('msgtype')=='text'
                          and event['payload'].get('text',{}).get('content','').replace('\r\n','\n') in allowed]
                if not captions:continue
                caption=captions[0]
                start=next(index for index,event in enumerate(events) if event['msgid']==caption['msgid'])+1
                end=next((index for index in range(start,len(events)) if events[index]['payload'].get('msgtype')=='text'),len(events))
                # Each native combined send starts with its caption. Never borrow
                # an image beyond the next caption when multiple sends await archive.
                batch=[event for event in events[start:end] if event['msgid'] not in used
                       and event['payload'].get('msgtype')=='image']
                want=Counter((media['md5'],media['size']) for media in expected)
                actual=Counter((event['payload']['image'].get('md5sum'),event['payload']['image'].get('filesize')) for event in batch)
                if actual!=want:continue
                proof=[self.evidence(caption)]
                for media in expected:
                    matching=[event for event in batch if event['msgid'] not in used
                              and event['payload'].get('image',{}).get('md5sum')==media['md5']
                              and event['payload'].get('image',{}).get('filesize')==media['size']]
                    if not matching:break
                    selected=matching[0];used.add(selected['msgid']);proof.append(self.evidence(selected))
                if len(proof)!=len(expected)+1:continue
                self.store.confirm(self.tenant,task['id'],Confirmation(events=proof))
            try:
                result=self.http.receipt(task['message_id'],'delivered',payload['caption'],after=task['intent_at'])
                if result.get('status')!='delivered' or result.get('ok') is False:continue
                with self.store.transaction() as db:
                    db.execute("UPDATE source_jobs SET reported_state='delivered',state='confirmed' WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.tenant,self.inbox,task['message_id']))
            except SourceUnavailable:pass

    @staticmethod
    def evidence(event):
        value={'event_id':event['msgid'],'room_id':event['room_id'],'actor_id':event['sender_id'],'occurred_at':float(event['occurred_at'])}
        if event['payload']['msgtype']=='text':return {**value,'kind':'text','text':event['payload']['text']['content'].replace('\r\n','\n')}
        return {**value,'kind':'image','md5':event['payload']['image']['md5sum'],'size':event['payload']['image']['filesize']}

    def run(self):
        # Dedicated session-level leader lock; no long transaction or lock on business tables.
        with self.store.database.pool.connection() as lock:
            if not lock.execute("SELECT pg_try_advisory_lock(hashtext(current_database()),hashtext('source:'||%s)) locked",(self.tenant+':'+self.inbox,)).fetchone()['locked']:
                raise RuntimeError('A source bridge already owns this route')
            tick=0
            while True:
                if tick%10==0:lock.execute('SELECT 1').fetchone()
                error=''
                try:self.capture();self.materialize();self.reconcile()
                except Exception as exc:
                    error=type(exc).__name__
                    print(json.dumps({'event':'source_bridge_error','type':error}),flush=True)
                if tick%10==0:
                    with self.store.transaction() as db:
                        db.execute('INSERT INTO worker_health(name,updated,error_type) VALUES(%s,%s,%s) ON CONFLICT(name) DO UPDATE SET updated=EXCLUDED.updated,error_type=EXCLUDED.error_type',('source:'+self.inbox,self.store.clock(),error))
                tick+=1
                time.sleep(.5)


def main():
    config=json.loads(Path(os.environ['RPA_GATEWAY_SOURCE_CONFIG_FILE']).read_text())
    source_dsn=Path(os.environ['RPA_GATEWAY_SOURCE_DATABASE_URL_FILE']).read_text().strip()
    store=make_store(Settings.from_env(),pool_size=2)
    try:SourceBridge(store,source_dsn,config).run()
    finally:store.close()

if __name__=='__main__':main()
