"""Scoped Chatwoot -> gateway -> official archive bridge. No desktop or history replay."""
import json
import re
import uuid
from collections import Counter
from .chatwoot_http import Chatwoot,SourceUnavailable
from .models import TaskCreate,Confirmation
from .source_policy import eligible_source,source_fingerprint
from .source_queue import SourceQueue,SourceSettings,PreparationLease,LeaseLost
from .source_resources import SourceResources,SourceReader,SourceInfrastructureUnavailable


class SourceBridge:
    def __init__(self,store,source_dsn,config,*,settings=None,reader=None,resources=None,queue=None):
        self.store,self.source_dsn,self.config=store,source_dsn,config
        self.http=Chatwoot(config)
        self.tenant=config['tenant'];self.inbox=str(config['inbox_id'])
        self.settings=settings or SourceSettings()
        self.reader=reader;self.resources=resources
        self.owns_reader=reader is None;self.owns_resources=resources is None
        self.queue=queue or SourceQueue(store,[(self.tenant,self.inbox)],self.settings)
        self.source_schema=config.get('source_schema','rpa_gateway_source')
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',self.source_schema):raise ValueError('Invalid source view schema')

    def read(self,query,params=()):
        if self.reader is None:self.reader=SourceReader(self.source_dsn,size=1)
        return self.reader.read(query.replace('rpa_gateway_source.',self.source_schema+'.'),params)

    def close(self):
        if self.owns_reader and self.reader:self.reader.close()
        if self.owns_resources and self.resources:self.resources.close()

    def route(self):
        with self.store.transaction() as db:
            return db.execute('SELECT * FROM source_routes WHERE tenant=%s AND inbox=%s',(self.tenant,self.inbox)).fetchone()

    def capture(self):
        route=self.route()
        if not route or route['mode']=='paused':return
        owner=uuid.uuid4().hex
        if not self.queue.capture_lease(self.tenant,self.inbox,owner):return
        try:
            rows=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id>%s AND inbox_id=%s ORDER BY id LIMIT 100',(route['cursor'],self.inbox))
            with self.store.transaction() as db:
                current=db.execute('SELECT * FROM source_routes WHERE tenant=%s AND inbox=%s FOR UPDATE',
                                   (self.tenant,self.inbox)).fetchone()
                held=db.execute('SELECT capture_owner,capture_until FROM source_scheduling WHERE tenant=%s AND inbox=%s',
                                (self.tenant,self.inbox)).fetchone()
                if (current['mode']=='paused' or current['generation']!=route['generation']
                    or held['capture_owner']!=owner or held['capture_until']<=self.store.clock()):return
                # Capture cursor and jobs commit together; stale captures cannot rewind either.
                for message in rows:
                    if message['id']<=current['cursor']:continue
                    if message['message_type']==1 and not message['private'] and not eligible_source(message):
                        self.store.notify(db,self.tenant,route['device'],'legacy_source_changed')
                    if eligible_source(message):
                        self.queue.enqueue(db,self.tenant,self.inbox,message['id'],message.get('created_at'))
                if rows:db.execute('UPDATE source_routes SET cursor=GREATEST(cursor,%s) WHERE tenant=%s AND inbox=%s',
                                   (rows[-1]['id'],self.tenant,self.inbox))
        finally:self.queue.release_capture(self.tenant,self.inbox,owner)

    def materialize(self):
        # Compatibility entry point; split workers call prepare() directly.
        job=self.queue.claim((self.tenant,self.inbox))
        if job is None:return 0
        self.prepare(job)
        return 1

    def prepare(self,job,cancellation=None):
        try:
            with PreparationLease(self.queue,job,cancellation) as budget:
                with self.http.budget(budget):
                    body=self.prepare_body(job,budget)
                if body is None:return None
                budget.check()
                return self.queue.finalize(job,body)
        except LeaseLost as exc:
            self.queue.retry(job,exc,infrastructure=True)
            return None
        except Exception as exc:
            self.queue.retry(job,exc,infrastructure=isinstance(exc,SourceInfrastructureUnavailable))
            return None

    def prepare_body(self,job,budget):
        with self.store.transaction() as db:
            existing=db.execute("SELECT payload FROM tasks WHERE tenant=%s AND payload::jsonb->>'inbox_id'=%s AND payload::jsonb->'context'->>'chatwoot_message_id'=%s LIMIT 1",
                                (self.tenant,self.inbox,str(job['message_id']))).fetchone()
        if existing:return TaskCreate(**json.loads(existing['payload']))
        messages=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s AND inbox_id=%s',(job['message_id'],self.inbox))
        if len(messages)!=1 or not eligible_source(messages[0]):
            self.queue.close_source(job,'SourceNoLongerEligible');return None
        message=messages[0];attrs=message['attributes'];delivery=attrs.get('rpa_delivery') or {}
        if delivery.get('status') in ('delivered','discarded','cancelled','unknown','sending'):
            self.queue.close_source(job,'SourceAlreadyHandled');return None
        originals=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s AND inbox_id=%s',(attrs['in_reply_to'],self.inbox))
        if len(originals)!=1:raise SourceUnavailable('Exact source not in bound conversation')
        source=originals[0]
        if source['attributes'].get('rpa_sender_id')!=attrs['rpa_recipient_id']:raise SourceUnavailable('Recipient differs from original question')
        identity=self.http.identity(source);budget.check()
        if identity['room_id']!=self.config['archive_group_id']:raise SourceUnavailable('Official room mismatch')
        caption=(message['content'] or '').replace('**','').strip()
        if not caption:raise SourceUnavailable('Empty source caption')
        fallback=attrs.get('rpa_pod_link_fallback') or {}
        if fallback.get('image_message_id'):
            with self.store.transaction() as db:
                previous=db.execute('SELECT t.state task_state,s.state source_state FROM source_jobs s LEFT JOIN tasks t ON s.task=t.id WHERE s.tenant=%s AND s.inbox=%s AND s.message_id=%s',
                                    (self.tenant,self.inbox,fallback['image_message_id'])).fetchone()
            if previous and (previous['source_state'] in ('pending','preparing')
                             or previous['task_state'] in ('queued','leased','submitting','submitted','unknown')):
                raise SourceInfrastructureUnavailable('Original image awaits reconciliation')
            if previous and previous['task_state']=='confirmed':
                self.queue.close_source(job,'OriginalImageConfirmed');return None
            originals=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s AND inbox_id=%s',
                                (fallback['image_message_id'],self.inbox))
            if originals:
                status=(originals[0]['attributes'].get('rpa_delivery') or {}).get('status')
                if status=='delivered':
                    self.queue.close_source(job,'OriginalImageConfirmed');return None
                if status in ('sending','unknown'):
                    raise SourceInfrastructureUnavailable('Original image awaits authoritative reconciliation')
        if message['attachments']:
            if self.resources is None:self.resources=SourceResources(self.store.presence,self.settings)
            media=self.resources.prepare(self,message,budget)
        else:media=[]
        target={'source_message_id':source['id'],'sender_id':identity['sender_id'],'recipient_id':identity['sender_id'],
                'sender_name':identity['sender_name'],'source_content':source['content'] or '','room_id':identity['room_id'],
                'identity_status':'verified','identity_origin':identity['origin'],'identity_policy':'official_only',
                'identity_verified_at':identity.get('verified_at'),'identity_verification_version':identity.get('verification_version')}
        found=re.search(r'\bJSE[A-Z0-9]{8,40}\b',caption,re.I)
        waybill=found.group(0).upper() if found else 'message-'+str(message['id'])
        with self.store.transaction() as db:
            backlog=db.execute("SELECT count(*) n FROM tasks WHERE tenant=%s AND device=%s AND state IN ('queued','leased','submitting','submitted')",(self.tenant,job['device'])).fetchone()['n']
        return TaskCreate(device_id=job['device'],inbox_id=self.inbox,room_id=identity['room_id'],
            source_message_id=str(message['id']),waybill=waybill,recipient_id=identity['sender_id'],
            robot_id=self.config['archive_sender_id'],caption=caption,media_ids=[m['id'] for m in media],
            ttl_seconds=min(3600,180+30*backlog),context={'provider':'chatwoot_official','chatwoot_message_id':message['id'],
                'question_message_id':source['id'],'route_generation':job['route_generation'],
                'source_fingerprint':source_fingerprint(message),'reply_target':target,
                'attachment_ids':[a['id'] for a in message['attachments']]})

    def reconcile(self,*,publish_receipts=True):
        with self.store.transaction() as db:
            selector="(t.state IN ('submitting','submitted','unknown') OR (t.state='confirmed' AND s.reported_state IS DISTINCT FROM 'delivered') OR (t.state IN ('failed','expired') AND s.reported_state IS DISTINCT FROM 'failed'))" if publish_receipts else "t.state IN ('submitting','submitted','unknown')"
            rows=db.execute("SELECT t.*,s.message_id,s.reported_state,(SELECT min(e.at) FROM events e WHERE e.task=t.id AND e.details::jsonb->>'state'='quarantine_released') quarantine_release_at FROM tasks t JOIN source_jobs s ON s.task=t.id WHERE s.tenant=%s AND s.inbox=%s AND "+selector+" ORDER BY quarantine_release_at NULLS FIRST,t.created LIMIT 30",
                            (self.tenant,self.inbox)).fetchall()
        for task in rows:
            payload=json.loads(task['payload'])
            if task['state'] in ('failed','expired'):
                if not publish_receipts:continue
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
                until=min(task['intent_at']+600,self.store.clock()+5)
                if task['quarantine_release_at'] is not None:until=min(until,task['quarantine_release_at'])
                events=self.read('SELECT * FROM rpa_gateway_source.test_archive WHERE occurred_at>=%s AND occurred_at<=%s AND room_id=%s AND sender_id=%s ORDER BY occurred_at,seq LIMIT 100',
                                 (task['intent_at']-2,until,self.config['archive_group_id'],self.config['archive_sender_id']))
                if task['quarantine_release_at'] is not None:
                    events=[event for event in events if event['occurred_at']<=task['quarantine_release_at']]
                # seq is ingestion order: an image may arrive before its caption
                # despite having a later official occurrence time. Keep native
                # batch boundaries chronological; ties retain archive order.
                events=sorted(events,key=lambda event:(event['occurred_at'],event.get('seq',0)))
                used=set()
                with self.store.transaction() as db:
                    used={row['event_id'] for row in db.execute('SELECT event_id FROM archive_claims WHERE tenant=%s AND event_id=ANY(%s)',
                                                              (self.tenant,[event['msgid'] for event in events]))}
                    expected=[dict(row) for mid in payload['media_ids'] for row in db.execute('SELECT md5,size FROM media WHERE tenant=%s AND id=%s',(self.tenant,mid))]
                target=(payload.get('context') or {})['reply_target'];name=target['sender_name']
                allowed={payload['caption'],'@'+name+' '+payload['caption'],'回复 '+name+'：\n'+payload['caption']}
                captions=[event for event in events if event['msgid'] not in used and event['payload'].get('msgtype')=='text'
                          and event['payload'].get('text',{}).get('content','').replace('\r\n','\n') in allowed]
                if not captions:continue
                caption=captions[0]
                start=next(index for index,event in enumerate(events) if event['msgid']==caption['msgid'])+1
                end=next((index for index in range(start,len(events)) if events[index]['payload'].get('msgtype')=='text'),len(events))
                # A native combined send starts with its caption in occurrence
                # order. Never borrow images beyond the next chronological caption.
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
            if not publish_receipts:continue
            try:
                result=self.http.receipt(task['message_id'],'delivered',payload['caption'],after=task['intent_at'])
                if result.get('status')!='delivered' or result.get('ok') is False:continue
                with self.store.transaction() as db:
                    db.execute("UPDATE source_jobs SET reported_state='delivered',state='confirmed' WHERE tenant=%s AND inbox=%s AND message_id=%s",(self.tenant,self.inbox,task['message_id']))
            except SourceUnavailable:pass

    def publish_report(self,job):
        try:
            if job['task']:
                payload=json.loads(job['payload'])
                if job['task_state']=='confirmed':
                    result=self.http.receipt(job['message_id'],'delivered',payload['caption'],after=job['intent_at'])
                    if result.get('status')!='delivered' or result.get('ok') is False:
                        self.queue.finish_report(job);return False
                    return self.queue.finish_report(job,'delivered')
                if job['intent_at'] is not None or job['submitted_at'] is not None:
                    raise SourceUnavailable('An attempted submission must not create a fallback')
                if payload['media_ids']:
                    self.http.receipt(job['message_id'],'image_failed',reason='签收图片未能提交，提供一次原图链接兜底',send_not_attempted=True)
                else:self.http.receipt(job['message_id'],'discarded',reason='未提交回复已过期，不自动重放')
                return self.queue.finish_report(job,'failed')
            messages=self.read('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s AND inbox_id=%s',(job['message_id'],self.inbox))
            if len(messages)!=1 or not eligible_source(messages[0]):
                return self.queue.finish_report(job,'discarded')
            message=messages[0];delivery=message['attributes'].get('rpa_delivery') or {}
            if delivery.get('status') in ('delivered','discarded','cancelled','unknown','sending'):
                return self.queue.finish_report(job,'discarded')
            if job['state']=='source_closed':
                self.http.receipt(job['message_id'],'discarded',reason='原图已确认送达，不重复发送链接兜底')
                return self.queue.finish_report(job,'discarded')
            if message['attachments']:
                self.http.receipt(job['message_id'],'image_failed',reason='签收图片准备未完成，提供一次原图链接兜底',send_not_attempted=True)
            else:self.http.receipt(job['message_id'],'discarded',reason='回复准备已超过处理时限，未发送且不自动重放')
            return self.queue.finish_report(job,'failed')
        except Exception as exc:
            self.queue.finish_report(job,error=exc)
            return False

    @staticmethod
    def evidence(event):
        value={'event_id':event['msgid'],'room_id':event['room_id'],'actor_id':event['sender_id'],'occurred_at':float(event['occurred_at'])}
        if event['payload']['msgtype']=='text':return {**value,'kind':'text','text':event['payload']['text']['content'].replace('\r\n','\n')}
        return {**value,'kind':'image','md5':event['payload']['image']['md5sum'],'size':event['payload']['image']['filesize']}

    def run(self):
        from .source_pipeline import SourcePipeline
        pipeline=SourcePipeline(self.store,self.source_dsn,[self.config],settings=self.settings)
        try:pipeline.run('all')
        finally:pipeline.close()


def main():
    from .source_pipeline import main as pipeline_main
    pipeline_main()


if __name__=='__main__':main()
