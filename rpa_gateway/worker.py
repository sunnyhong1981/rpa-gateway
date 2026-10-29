"""Bounded outbox relay, expiry, image validation and retention. No desktop operations."""
import asyncio
import hashlib
import json
import time
import uuid
from .objects import verify_image


class Worker:
    def __init__(self,store,metrics=None,media_days=30,event_days=365,alerts=None):
        if media_days<1 or event_days<30:raise ValueError('Retention below safety minimum')
        self.store,self.metrics=store,metrics
        self.alerts=alerts
        self.media_days,self.event_days=media_days,event_days

    def relay(self):
        store=self.store
        if not store.presence:return 0
        with store.transaction() as db:
            rows=db.execute('SELECT * FROM outbox WHERE delivered_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED').fetchall()
            # A Redis failure rolls this transaction back. Publish-before-commit can duplicate hints; hints never execute a task.
            for row in rows:
                store.presence.publish({key:row[key] for key in ('tenant','device','revision','kind')})
                db.execute('UPDATE outbox SET delivered_at=%s WHERE id=%s',(store.clock(),row['id']))
            return len(rows)

    def sweep(self):
        with self.store.transaction() as db:self.store.sweep(db)

    def validate_one(self):
        store=self.store
        if not store.objects:return False
        now=store.clock()
        with store.transaction() as db:
            row=db.execute("SELECT * FROM media_uploads WHERE state='queued' OR (state='validating' AND lease_until<%s) ORDER BY updated LIMIT 1 FOR UPDATE SKIP LOCKED",(now,)).fetchone()
            if row is None:return False
            if row['attempts']>=3:
                db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(row['staging_key'],now))
                db.execute("UPDATE media_uploads SET state='rejected',reason='validation_retry_exhausted',updated=%s WHERE id=%s",(now,row['id']))
                return True
            db.execute("UPDATE media_uploads SET state='validating',attempts=attempts+1,lease_until=%s,updated=%s WHERE id=%s",(now+120,now,row['id']))
        began=time.monotonic()
        try:
            data=store.objects.get(row['staging_key'])
            meta=verify_image(data)
            if meta['sha256']!=row['expected_sha256'] or meta['md5']!=row['expected_md5'] or meta['size']!=row['expected_size']:
                raise ValueError('Media manifest mismatch')
            media=store.put_media(row['tenant'],data,meta['mime'],_verified=meta)
        except ValueError:
            with store.transaction() as db:
                db.execute("UPDATE media_uploads SET state='rejected',reason='invalid_image_or_hash',updated=%s WHERE id=%s",(store.clock(),row['id']))
                db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(row['staging_key'],store.clock()))
            return True
        except Exception:
            # Lease will expire, with bounded attempts. Do not claim corrupted bytes are ready.
            return True
        with store.transaction() as db:
            db.execute("UPDATE media_uploads SET state='ready',media_id=%s,updated=%s,validation_seconds=%s WHERE id=%s",(media['id'],store.clock(),time.monotonic()-began,row['id']))
            db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(row['staging_key'],store.clock()))
        return True

    def externalize_one(self):
        store=self.store
        if not store.objects:return False
        # Old BLOB -> original object, verification -> NULL BLOB in a single locked-row transaction.
        with store.transaction() as db:
            row=db.execute('SELECT * FROM media WHERE data IS NOT NULL AND deleting=false LIMIT 1 FOR UPDATE SKIP LOCKED').fetchone()
            if row is None:return False
            key=store.objects.key(row['tenant'],row['sha256'])+'-'+uuid.uuid4().hex
            store.objects.put(key,row['data'],row['mime'])
            actual=store.objects.get(key)
            if hashlib.sha256(actual).hexdigest()!=row['sha256'] or len(actual)!=row['size']:
                raise ValueError('Object migration integrity failure')
            db.execute('UPDATE media SET object_key=%s,data=NULL WHERE tenant=%s AND id=%s',(key,row['tenant'],row['id']))
            return True

    def collect_garbage(self):
        store=self.store
        if not store.objects:return 0
        with store.transaction() as db:
            rows=db.execute('SELECT * FROM media_gc ORDER BY created LIMIT 20 FOR UPDATE SKIP LOCKED').fetchall()
            for row in rows:
                store.objects.delete(row['object_key'])
                db.execute('DELETE FROM media_gc WHERE object_key=%s',(row['object_key'],))
            return len(rows)

    def retain(self):
        store=self.store;now=store.clock()
        with store.transaction() as db:
            if not db.execute("SELECT pg_try_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':retention')) locked").fetchone()['locked']:
                return 0
            rows=db.execute("""SELECT m.* FROM media m WHERE m.created<%s AND NOT EXISTS (
                SELECT 1 FROM tasks t WHERE t.tenant=m.tenant AND (t.payload::jsonb->'media_ids') ? m.id
                AND (t.state IN ('queued','leased','submitting','submitted','unknown') OR t.updated>=%s))
                AND NOT EXISTS (SELECT 1 FROM media_uploads u WHERE u.media_id=m.id AND u.updated>=%s)
                ORDER BY m.created LIMIT 50 FOR UPDATE OF m SKIP LOCKED""",
                (now-self.media_days*86400,now-self.media_days*86400,now-self.media_days*86400)).fetchall()
            for row in rows:
                if row['object_key']:
                    db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(row['object_key'],now))
                db.execute('DELETE FROM media WHERE tenant=%s AND id=%s',(row['tenant'],row['id']))
            # Keep task identities, receipt event IDs/state/hash and archive uniqueness forever; prune old sensitive detail only.
            redacted=db.execute("""WITH old AS (
                SELECT e.seq FROM events e JOIN tasks t ON e.task=t.id
                WHERE e.at<%s AND t.state IN ('confirmed','failed','expired','cancelled','closed_unconfirmed')
                AND e.details NOT LIKE '%%retention_redacted%%' ORDER BY e.seq LIMIT 500 FOR UPDATE OF e SKIP LOCKED)
                UPDATE events SET details=json_build_object('retention_redacted',true,'original_md5',md5(details))::text
                WHERE seq IN (SELECT seq FROM old) RETURNING seq""",(now-self.event_days*86400,)).fetchall()
            db.execute('DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE delivered_at<%s ORDER BY id LIMIT 1000)',(now-7*86400,))
            expired=db.execute("UPDATE media_uploads SET state='rejected',reason='upload_expired',updated=%s WHERE state='pending' AND created<%s RETURNING staging_key",(now,now-3600)).fetchall()
            for row in expired:
                db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(row['staging_key'],now))
            if rows or redacted:
                db.execute('INSERT INTO retention_audit(at,kind,affected,details) VALUES(%s,%s,%s,%s)',
                    (now,'media_delete_and_event_redact',len(rows)+len(redacted),json.dumps({'media':len(rows),'event_details':len(redacted),'media_days':self.media_days,'event_days':self.event_days})))
            return len(rows)

    def reconcile_orphans(self):
        store=self.store
        if not store.objects:return 0
        now=store.clock()
        with store.transaction() as db:
            if not db.execute("SELECT pg_try_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':object-orphans')) locked").fetchone()['locked']:
                return 0
            row=db.execute("SELECT value FROM maintenance_cursors WHERE name='object-orphans'").fetchone()
            arguments={'Bucket':store.objects.bucket,'Prefix':store.objects.prefix,'MaxKeys':100}
            if row and row['value']:arguments['ContinuationToken']=row['value']
            page=store.objects.client.list_objects_v2(**arguments)
            count=0
            for item in page.get('Contents',[]):
                key=item['Key']
                if now-item['LastModified'].timestamp()<86400:continue
                referenced=db.execute('SELECT 1 FROM media WHERE object_key=%s UNION ALL SELECT 1 FROM media_uploads WHERE staging_key=%s AND state IN (%s,%s,%s) LIMIT 1',
                                      (key,key,'pending','queued','validating')).fetchone()
                if not referenced:
                    db.execute('INSERT INTO media_gc VALUES(%s,%s) ON CONFLICT DO NOTHING',(key,now));count+=1
            db.execute("INSERT INTO maintenance_cursors(name,value) VALUES('object-orphans',%s) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",
                       (page.get('NextContinuationToken'),))
            return count

    def observe(self,error=''):
        store=self.store
        with store.transaction() as db:
            db.execute("INSERT INTO worker_health(name,updated,error_type) VALUES('maintenance',%s,%s) ON CONFLICT(name) DO UPDATE SET updated=EXCLUDED.updated,error_type=EXCLUDED.error_type",(store.clock(),error))

    async def run(self):
        from starlette.concurrency import run_in_threadpool
        tick=0
        while True:
            error=''
            operations=(self.relay,self.validate_one,self.externalize_one,self.collect_garbage)
            if self.alerts:operations=operations+(self.alerts.send_one,)
            for operation in operations:
                try:await run_in_threadpool(operation)
                except Exception as exc:error=type(exc).__name__
            if tick%10==0:
                try:await run_in_threadpool(self.sweep)
                except Exception as exc:error=type(exc).__name__
            if tick%600==0:
                try:
                    await run_in_threadpool(self.retain)
                    await run_in_threadpool(self.reconcile_orphans)
                except Exception as exc:error=type(exc).__name__
            if tick%20==0:
                try:await run_in_threadpool(self.observe,error)
                except Exception:pass
            tick+=1
            await asyncio.sleep(.1)


def main():
    from .app import Settings,make_store
    settings=Settings.from_env()
    store=make_store(settings,pool_size=2)
    from .alerting import Alerts
    try:asyncio.run(Worker(store,media_days=settings.media_days,event_days=settings.event_days,alerts=Alerts(store,settings.wecom_webhook)).run())
    finally:store.close()

if __name__=='__main__':main()
