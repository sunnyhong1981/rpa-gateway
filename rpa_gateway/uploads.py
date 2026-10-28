import hashlib
import uuid
from .store import fail


class Uploads:
    def __init__(self,store):
        self.store=store

    def create(self,tenant,body):
        store=self.store
        if not store.objects:fail(503,'Object storage not configured')
        upload_id=uuid.uuid4().hex
        key=store.objects.staging_key(tenant,upload_id)
        signed=store.objects.upload(key,body.size)
        with store.transaction() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':uploads:' || %s))",(tenant,))
            pending=db.execute("SELECT count(*) n FROM media_uploads WHERE tenant=%s AND state IN ('pending','queued','validating')",(tenant,)).fetchone()['n']
            if pending>=200:fail(429,'Too many unverified media uploads')
            db.execute('INSERT INTO media_uploads(tenant,id,staging_key,expected_sha256,expected_md5,expected_size,state,created,updated) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (tenant,upload_id,key,body.sha256,body.md5,body.size,'pending',store.clock(),store.clock()))
        return {'upload_id':upload_id,'upload':signed,'expires_in':300,'max_bytes':body.size,
                'complete_path':f'/v1/backend/{tenant}/media/uploads/{upload_id}/complete'}

    def complete(self,tenant,upload_id):
        store=self.store
        with store.transaction() as db:
            row=db.execute('SELECT * FROM media_uploads WHERE tenant=%s AND id=%s FOR UPDATE',(tenant,upload_id)).fetchone()
            if row is None:fail(404,'Upload not found')
            if row['state']=='pending':
                if store.clock()-row['created']>360:fail(410,'Upload authorization expired')
                db.execute("UPDATE media_uploads SET state='queued',updated=%s WHERE id=%s",(store.clock(),upload_id))
        return self.status(tenant,upload_id)

    def status(self,tenant,upload_id):
        store=self.store
        with store.transaction() as db:
            row=db.execute('SELECT id,state,media_id,reason FROM media_uploads WHERE tenant=%s AND id=%s',(tenant,upload_id)).fetchone()
            if row is None:fail(404,'Upload not found')
            return dict(row)
