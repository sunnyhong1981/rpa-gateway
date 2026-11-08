"""Durable, bounded preparation admission with fair routes and fenced completion."""
from dataclasses import dataclass
import json
import threading
import uuid
from datetime import datetime,timezone


class LeaseLost(RuntimeError):
    pass


class PreparationTimeout(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceSettings:
    workers: int = 4
    global_jobs: int = 8
    per_group_jobs: int = 2
    image_workers: int = 8
    network_slots: int = 8
    decode_slots: int = 2
    lease_seconds: float = 15
    prepare_seconds: float = 60
    queue_seconds: float = 300
    max_attempts: int = 3
    report_workers: int = 2
    ready_per_device: int = 16

    def __post_init__(self):
        for name in ('workers','global_jobs','per_group_jobs','image_workers','network_slots','decode_slots','max_attempts','report_workers','ready_per_device'):
            if not 1 <= getattr(self,name) <= 64:
                raise ValueError('Invalid bounded source setting: '+name)
        if not 1 <= self.lease_seconds <= self.prepare_seconds <= 300 or not 1 <= self.queue_seconds <= 3600:
            raise ValueError('Invalid source deadlines')


class SourceQueue:
    def __init__(self,store,routes,settings=None):
        self.store=store
        self.settings=settings or SourceSettings()
        self.routes=tuple(sorted(set((str(a),str(b)) for a,b in routes)))
        if not self.routes:
            raise ValueError('An explicit authorized route list is required')
        self.allowed=json.dumps([{'tenant':a,'inbox':b} for a,b in self.routes])
        with self.store.transaction() as db:
            for name in ('global_jobs','per_group_jobs','network_slots','decode_slots','ready_per_device'):
                value=getattr(self.settings,name)
                db.execute('INSERT INTO source_runtime_limits(name,value) VALUES(%s,%s) ON CONFLICT DO NOTHING',(name,value))
                if db.execute('SELECT value FROM source_runtime_limits WHERE name=%s',(name,)).fetchone()['value']!=value:
                    raise ValueError('Source workers disagree on shared limit: '+name)

    def enqueue(self,db,tenant,inbox,message_id,created_at=None):
        if (str(tenant),str(inbox)) not in self.routes:
            raise ValueError('Source route is not authorized')
        now=self.store.clock();s=self.settings
        created=(created_at.replace(tzinfo=timezone.utc) if created_at.tzinfo is None else created_at).timestamp() if isinstance(created_at,datetime) else now
        db.execute('''INSERT INTO source_jobs(tenant,inbox,message_id,enqueued_at,source_created_at,queue_until,deadline)
          VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
          (tenant,str(inbox),message_id,now,created,created+s.queue_seconds,created+s.queue_seconds+s.prepare_seconds))

    def claim(self,only=None):
        s=self.settings
        allowed=self.allowed if only is None else json.dumps([{'tenant':str(only[0]),'inbox':str(only[1])}])
        if only is not None and (str(only[0]),str(only[1])) not in self.routes:
            raise ValueError('Source route is not authorized')
        with self.store.transaction() as db:
            if not db.execute("SELECT pg_try_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema()||':source-admission')) locked").fetchone()['locked']:
                return None
            now=self.store.clock()
            db.execute('''INSERT INTO source_scheduling(tenant,inbox)
              SELECT tenant,inbox FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
              ON CONFLICT DO NOTHING''',(self.allowed,))
            # Adopt old unsubmitted pending rows without resetting submitted work or history.
            db.execute('''UPDATE source_jobs j SET enqueued_at=%s,queue_until=%s,deadline=%s
              WHERE j.task IS NULL AND j.state='pending' AND j.queue_until IS NULL
              AND EXISTS(SELECT 1 FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
                         WHERE a.tenant=j.tenant AND a.inbox=j.inbox)''',
              (now,now+s.queue_seconds,now+s.queue_seconds+s.prepare_seconds,self.allowed))
            db.execute("""UPDATE source_jobs j SET state='pending',lease_token=NULL,lease_until=NULL,
              error_type='PreparationLeaseExpired' WHERE task IS NULL AND state='preparing' AND lease_until<=%s
              AND EXISTS(SELECT 1 FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
                         WHERE a.tenant=j.tenant AND a.inbox=j.inbox)""",(now,self.allowed))
            db.execute("""UPDATE source_jobs j SET state='failed',error_type=CASE
                WHEN started_at IS NULL AND queue_until<=%s THEN 'QueueDeadlineExceeded' ELSE 'PreparationBudgetExceeded' END,
                lease_token=NULL,lease_until=NULL
              WHERE task IS NULL AND state='pending' AND
                ((started_at IS NULL AND queue_until<=%s) OR deadline<=%s OR attempts>=%s)
              AND EXISTS(SELECT 1 FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
                         WHERE a.tenant=j.tenant AND a.inbox=j.inbox)""",
              (now,now,now,s.max_attempts,self.allowed))
            active=db.execute("SELECT count(*) n FROM source_jobs WHERE task IS NULL AND state='preparing' AND lease_until>%s",(now,)).fetchone()['n']
            if active>=s.global_jobs:
                return None
            job=db.execute("""SELECT j.*,r.device,r.generation FROM source_jobs j
              JOIN source_routes r ON r.tenant=j.tenant AND r.inbox=j.inbox
              JOIN source_scheduling f ON f.tenant=j.tenant AND f.inbox=j.inbox
              WHERE j.task IS NULL AND j.state='pending' AND j.next_at<=%s
                AND r.mode='gateway' AND r.client_ready_at IS NOT NULL AND j.message_id>r.floor
                AND EXISTS(SELECT 1 FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
                           WHERE a.tenant=j.tenant AND a.inbox=j.inbox)
                AND (SELECT count(*) FROM source_jobs busy WHERE busy.tenant=j.tenant AND busy.inbox=j.inbox
                     AND busy.state='preparing' AND busy.lease_until>%s)<%s
                AND ((SELECT count(*) FROM tasks ready WHERE ready.tenant=r.tenant AND ready.device=r.device
                     AND ready.state IN ('queued','leased','submitting','submitted','unknown'))+
                     (SELECT count(*) FROM source_jobs reserved JOIN source_routes owner
                        ON owner.tenant=reserved.tenant AND owner.inbox=reserved.inbox
                      WHERE owner.tenant=r.tenant AND owner.device=r.device AND reserved.state='preparing'
                        AND reserved.task IS NULL AND reserved.lease_until>%s))<%s
              ORDER BY f.last_claimed,j.enqueued_at,j.message_id LIMIT 1
              FOR UPDATE OF j,f SKIP LOCKED""",(now,allowed,now,s.per_group_jobs,now,s.ready_per_device)).fetchone()
            if job is None:
                return None
            now=self.store.clock()
            if job['deadline']<=now or (job['started_at'] is None and job['queue_until']<=now):return None
            token=uuid.uuid4().hex;until=min(now+s.prepare_seconds,job['deadline'])
            db.execute("""UPDATE source_jobs SET state='preparing',attempts=attempts+1,
              started_at=COALESCE(started_at,%s),attempt_until=%s,lease_token=%s,lease_until=%s,route_generation=%s
              WHERE tenant=%s AND inbox=%s AND message_id=%s""",
              (now,until,token,min(now+s.lease_seconds,until),job['generation'],job['tenant'],job['inbox'],job['message_id']))
            db.execute('UPDATE source_scheduling SET last_claimed=%s WHERE tenant=%s AND inbox=%s',
                       (now,job['tenant'],job['inbox']))
            return {**dict(job),'lease_token':token,'attempt_until':until,'route_generation':job['generation'],
                    'attempts':job['attempts']+1,'lease_until':min(now+s.lease_seconds,until)}

    def renew(self,job):
        with self.store.transaction() as db:
            row=db.execute('SELECT * FROM source_jobs WHERE tenant=%s AND inbox=%s AND message_id=%s FOR UPDATE',
                           (job['tenant'],job['inbox'],job['message_id'])).fetchone()
            now=self.store.clock()
            if (not row or row['task'] is not None or row['state']!='preparing' or row['lease_token']!=job['lease_token']
                or row['lease_until']<=now or row['attempt_until']<=now):return False
            db.execute('UPDATE source_jobs SET lease_until=LEAST(%s,attempt_until) WHERE tenant=%s AND inbox=%s AND message_id=%s',
                       (now+self.settings.lease_seconds,job['tenant'],job['inbox'],job['message_id']))
            return True

    def close_source(self,job,reason):
        with self.store.transaction() as db:
            return db.execute("""UPDATE source_jobs SET state='source_closed',error_type=%s,reported_state=%s,lease_token=NULL,lease_until=NULL
              WHERE tenant=%s AND inbox=%s AND message_id=%s AND state='preparing' AND task IS NULL AND lease_token=%s AND lease_until>%s
              RETURNING message_id""",(reason,None if reason=='OriginalImageConfirmed' else 'source_closed',
              job['tenant'],job['inbox'],job['message_id'],job['lease_token'],self.store.clock())).fetchone() is not None

    def capture_lease(self,tenant,inbox,owner):
        now=self.store.clock()
        with self.store.transaction() as db:
            db.execute('INSERT INTO source_scheduling(tenant,inbox) VALUES(%s,%s) ON CONFLICT DO NOTHING',(tenant,inbox))
            row=db.execute('''UPDATE source_scheduling SET capture_owner=%s,capture_until=%s
              WHERE tenant=%s AND inbox=%s AND (capture_until IS NULL OR capture_until<=%s)
              RETURNING inbox''',(owner,now+10,tenant,inbox,now)).fetchone()
            return row is not None

    def release_capture(self,tenant,inbox,owner):
        with self.store.transaction() as db:
            db.execute('UPDATE source_scheduling SET capture_owner=NULL,capture_until=NULL WHERE tenant=%s AND inbox=%s AND capture_owner=%s',
                       (tenant,inbox,owner))

    def claim_report(self):
        now=self.store.clock()
        with self.store.transaction() as db:
            db.execute('''INSERT INTO source_scheduling(tenant,inbox)
              SELECT tenant,inbox FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
              ON CONFLICT DO NOTHING''',(self.allowed,))
            row=db.execute("""SELECT j.*,t.state task_state,t.payload,t.intent_at,t.submitted_at
              FROM source_jobs j JOIN source_scheduling f ON f.tenant=j.tenant AND f.inbox=j.inbox
              LEFT JOIN tasks t ON t.id=j.task
              WHERE (j.report_until IS NULL OR j.report_until<=%s) AND j.report_next_at<=%s
                AND EXISTS(SELECT 1 FROM jsonb_to_recordset(%s::jsonb) AS a(tenant text,inbox text)
                           WHERE a.tenant=j.tenant AND a.inbox=j.inbox)
                AND ((j.task IS NULL AND (j.state='failed' OR (j.state='source_closed' AND j.error_type='OriginalImageConfirmed')) AND j.reported_state IS NULL)
                     OR (t.state='confirmed' AND j.reported_state IS DISTINCT FROM 'delivered')
                     OR (t.state IN ('failed','expired') AND j.reported_state IS DISTINCT FROM 'failed'))
              ORDER BY f.last_reported,j.enqueued_at,j.message_id LIMIT 1 FOR UPDATE OF j,f SKIP LOCKED""",
              (now,now,self.allowed)).fetchone()
            if row is None:return None
            # Recover a legacy task/link split commit before ever issuing a fallback.
            if row['task'] is None:
                old=db.execute("SELECT id FROM tasks WHERE tenant=%s AND payload::jsonb->>'inbox_id'=%s AND payload::jsonb->'context'->>'chatwoot_message_id'=%s LIMIT 1",
                               (row['tenant'],row['inbox'],str(row['message_id']))).fetchone()
                if old:
                    db.execute("UPDATE source_jobs SET task=%s,state='queued' WHERE tenant=%s AND inbox=%s AND message_id=%s",
                               (old['id'],row['tenant'],row['inbox'],row['message_id']))
                    return None
            token=uuid.uuid4().hex
            db.execute('UPDATE source_jobs SET report_token=%s,report_until=%s WHERE tenant=%s AND inbox=%s AND message_id=%s',
                       (token,now+30,row['tenant'],row['inbox'],row['message_id']))
            db.execute('UPDATE source_scheduling SET last_reported=%s WHERE tenant=%s AND inbox=%s',(now,row['tenant'],row['inbox']))
            return {**dict(row),'report_token':token}

    def finish_report(self,job,status=None,error=None):
        with self.store.transaction() as db:
            if status:
                return db.execute('''UPDATE source_jobs SET reported_state=%s,state=%s,report_token=NULL,
                  report_until=NULL,report_error='' WHERE tenant=%s AND inbox=%s AND message_id=%s AND report_token=%s
                  RETURNING message_id''',(status,'confirmed' if status=='delivered' else 'source_closed' if status=='discarded' else 'failed',
                  job['tenant'],job['inbox'],job['message_id'],job['report_token'])).fetchone() is not None
            db.execute('''UPDATE source_jobs SET report_token=NULL,report_until=NULL,report_next_at=%s,report_error=%s
              WHERE tenant=%s AND inbox=%s AND message_id=%s AND report_token=%s''',
              (self.store.clock()+5,type(error).__name__ if error else 'ReceiptNotConfirmed',
               job['tenant'],job['inbox'],job['message_id'],job['report_token']))
            return False

    def retry(self,job,error,infrastructure=False):
        now=self.store.clock();s=self.settings
        with self.store.transaction() as db:
            row=db.execute("""SELECT * FROM source_jobs WHERE tenant=%s AND inbox=%s AND message_id=%s
              AND task IS NULL AND state='preparing' AND lease_token=%s AND lease_until>%s FOR UPDATE""",
              (job['tenant'],job['inbox'],job['message_id'],job['lease_token'],now)).fetchone()
            if row is None:
                return False
            now=self.store.clock()
            if row['lease_until']<=now:return False
            failed=now>=row['deadline'] or (row['attempts']>=s.max_attempts and not infrastructure)
            db.execute("""UPDATE source_jobs SET state=%s,error_type=%s,next_at=%s,
              attempts=GREATEST(0,attempts-%s),lease_token=NULL,lease_until=NULL
              WHERE tenant=%s AND inbox=%s AND message_id=%s""",
              ('failed' if failed else 'pending',type(error).__name__,now+min(30,2**row['attempts']),
               1 if infrastructure else 0,job['tenant'],job['inbox'],job['message_id']))
            return True

    def finalize(self,job,body):
        with self.store.transaction() as db:
            # Same lock order as other execution mutations: device before task/job.
            self.store.lock_device(db,job['tenant'],job['device'])
            route=db.execute('SELECT * FROM source_routes WHERE tenant=%s AND inbox=%s FOR SHARE',
                             (job['tenant'],job['inbox'])).fetchone()
            current=db.execute('SELECT * FROM source_jobs WHERE tenant=%s AND inbox=%s AND message_id=%s FOR UPDATE',
                               (job['tenant'],job['inbox'],job['message_id'])).fetchone()
            now=self.store.clock()
            if (not current or current['task'] is not None or current['state']!='preparing'
                or current['lease_token']!=job['lease_token'] or current['lease_until']<=now
                or current['attempt_until']<=now):
                raise LeaseLost('Preparation ownership expired; no task published')
            if (not route or route['mode']!='gateway' or not route['client_ready_at']
                or route['generation']!=job['route_generation'] or route['device']!=job['device']
                or job['message_id']<=route['floor']):
                db.execute("UPDATE source_jobs SET state='source_closed',reported_state='source_closed',error_type='RouteChanged',lease_token=NULL,lease_until=NULL WHERE tenant=%s AND inbox=%s AND message_id=%s",
                           (job['tenant'],job['inbox'],job['message_id']))
                return None
            old=db.execute("""SELECT * FROM tasks WHERE tenant=%s AND payload::jsonb->>'inbox_id'=%s
              AND payload::jsonb->'context'->>'chatwoot_message_id'=%s LIMIT 1""",
              (job['tenant'],job['inbox'],str(job['message_id']))).fetchone()
            if old is None:
                ready=db.execute("SELECT count(*) n FROM tasks WHERE tenant=%s AND device=%s AND state IN ('queued','leased','submitting','submitted','unknown')",
                                 (job['tenant'],job['device'])).fetchone()['n']
                if ready>=self.settings.ready_per_device:
                    db.execute("UPDATE source_jobs SET state='pending',next_at=%s,attempts=GREATEST(0,attempts-1),lease_token=NULL,lease_until=NULL WHERE tenant=%s AND inbox=%s AND message_id=%s",
                               (now+.5,job['tenant'],job['inbox'],job['message_id']))
                    return None
            task=self.store.present(old) if old else self.store.create_task(job['tenant'],body,transaction=db)
            db.execute("""UPDATE source_jobs SET task=%s,state='queued',error_type='',lease_token=NULL,lease_until=NULL
              WHERE tenant=%s AND inbox=%s AND message_id=%s""",(task['id'],job['tenant'],job['inbox'],job['message_id']))
            return task


class PreparationLease:
    def __init__(self,queue,job,cancellation=None):
        self.queue,self.job=queue,job
        self.cancellation=cancellation
        self.stopped=threading.Event();self.lost=threading.Event();self.thread=None

    def check(self):
        if self.queue.store.clock()>=self.job['attempt_until']:
            raise PreparationTimeout('Preparation execution deadline expired')
        if self.lost.is_set() or self.stopped.is_set() or (self.cancellation and self.cancellation.is_set()):
            raise LeaseLost('Preparation no longer owns this work')

    def remaining(self):
        self.check()
        return self.job['attempt_until']-self.queue.store.clock()

    def __enter__(self):
        def renew():
            while not self.stopped.wait(min(1,self.queue.settings.lease_seconds/3)):
                try:owned=self.queue.renew(self.job)
                except Exception:owned=False
                if not owned:self.lost.set();return
        self.thread=threading.Thread(target=renew,name='source-lease-renew',daemon=True);self.thread.start()
        return self

    def __exit__(self,*args):
        self.stopped.set()
        if self.thread:self.thread.join(timeout=10)
