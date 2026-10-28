import asyncio
import concurrent.futures
import hashlib
import io
import json
import time
from unittest.mock import patch
import httpx
import pytest
import redis
from fastapi import HTTPException
from PIL import Image
from rpa_gateway.models import Claim,Receipt
from rpa_gateway.presence import Presence
from rpa_gateway.worker import Worker
from rpa_gateway.objects import verify_image
from rpa_gateway.uploads import Uploads


def original(color='red',kind='PNG'):
    stream=io.BytesIO();Image.new('RGB',(40,40),color).save(stream,format=kind)
    return stream.getvalue()


def staged(env,data=None,**overrides):
    data=data or original()
    manifest={'sha256':hashlib.sha256(data).hexdigest(),'md5':hashlib.md5(data).hexdigest(),'size':len(data)}
    manifest.update(overrides)
    response=env.client.post('/v1/backend/test/media/uploads',headers=env.backend,json=manifest)
    assert response.status_code==200,response.text
    upload=response.json()
    with httpx.Client(trust_env=False) as client:
        response=client.post(upload['upload']['url'],data=upload['upload']['fields'],files={'file':('original',data)})
    assert response.status_code in (200,201,204),response.text
    return upload,data


def test_heartbeat_uses_redis_without_pg_writes(env):
    d=env.register()
    store=env.app.state.store
    with store.transaction() as db:
        before=db.execute('SELECT heartbeat,seen FROM devices').fetchone()
    for _ in range(5):assert env.heartbeat(d).status_code==200
    with store.transaction() as db:assert db.execute('SELECT heartbeat,seen FROM devices').fetchone()==before
    assert store.presence.ready(store.device(d['headers']['Authorization'][7:]))


def test_task_transition_and_outbox_atomic_commit_and_rollback(env):
    d=env.register();env.enqueue(d)
    store=env.app.state.store
    with store.transaction() as db:
        task=db.execute('SELECT * FROM tasks').fetchone()
        count=db.execute('SELECT count(*) n FROM outbox').fetchone()['n']
    with pytest.raises(RuntimeError):
        with store.transaction() as db:
            store.lock_device(db,'test','desktop')
            store.transition(db,task,'cancelled')
            raise RuntimeError('rollback')
    with store.transaction() as db:
        assert db.execute('SELECT state FROM tasks').fetchone()['state']=='queued'
        assert db.execute('SELECT count(*) n FROM outbox').fetchone()['n']==count


def test_outbox_survives_redis_failure_and_relays_later(env):
    env.register();env.enqueue({'device':'desktop','tenant':'test'})
    store=env.app.state.store
    worker=Worker(store)
    with patch.object(store.presence,'publish',side_effect=redis.ConnectionError()):
        with pytest.raises(redis.ConnectionError):worker.relay()
    # Stop relying on background timing; a newly written event must stay pending after a failed relay.
        with store.transaction() as db:
            store.notify(db,'test','desktop')
        with pytest.raises(redis.ConnectionError):worker.relay()
        with store.transaction() as db:assert db.execute('SELECT count(*) n FROM outbox WHERE delivered_at IS NULL').fetchone()['n']>0
    worker.relay()
    with store.transaction() as db:assert db.execute('SELECT count(*) n FROM outbox WHERE delivered_at IS NULL').fetchone()['n']==0


def test_redis_failure_pauses_new_send_but_submitted_receipt_still_persists(env):
    d=env.register();env.enqueue(d)
    task=env.claim(d).json()['task']
    assert env.receipt(d,task,'submitting').status_code==200
    with patch.object(env.app.state.store.presence.redis,'get',side_effect=redis.ConnectionError()):
        assert env.receipt(d,task,'submitted').status_code==200
        # A new device needing dispatch is rejected by readiness, even with a healthy PostgreSQL.
        store=env.app.state.store
        credential=store.device(d['headers']['Authorization'][7:])
        with pytest.raises(HTTPException) as exc:store.claim(credential,Claim(session_id=d['session_id']))
        assert exc.value.status_code==503


def test_rate_limit_shared_across_presence_instances(env):
    store=env.app.state.store
    other=Presence(env.settings.redis_url,env.settings.redis_namespace,env.clock)
    try:
        store.presence.check_rate('test','device',.01,1)
        with pytest.raises(HTTPException) as exc:other.check_rate('test','device',.01,1)
        assert exc.value.status_code==429 and int(exc.value.headers['Retry-After'])>=1
    finally:other.close()


def test_control_body_limit_and_metrics_private_without_id_labels(env):
    assert env.client.get('/metrics').status_code==401
    assert env.client.post('/v1/device/hello',content=b'x'*(257*1024)).status_code==413
    text=env.client.get('/metrics',headers=env.admin).text
    assert 'rpa_gateway_http_seconds' in text and 'Authorization' not in text
    assert 'rpa_gateway_loop' not in text or 'event_loop' in text


def test_original_bytes_not_stored_in_postgres_when_objects_enabled(env):
    meta,data=env.image(kind='JPEG')
    with env.app.state.store.transaction() as db:
        row=db.execute('SELECT * FROM media WHERE id=%s',(meta['id'],)).fetchone()
    assert row['data'] is None and row['object_key']
    assert env.app.state.store.objects.get(row['object_key'])==data


def test_signed_download_requires_task_and_device_authentication(env):
    d=env.register();meta,data=env.image();task=env.enqueue(d,media_ids=[meta['id']]).json()
    route=f"/v1/device/tasks/{task['id']}/media/{meta['id']}/download"
    assert env.client.get(route,headers=d['headers']).status_code==409
    claimed=env.claim(d).json()['task']
    result=env.client.get(route,headers=d['headers']).json()
    assert result['expires_in']==120 and result['sha256']==meta['sha256']
    assert httpx.get(result['url'],trust_env=False).content==data
    other=env.register(device='other')
    assert env.client.get(route,headers=other['headers']).status_code==404
    env.receipt(d,claimed,'failed_pre_submit')
    assert env.client.get(route,headers=d['headers']).status_code==409


def test_direct_upload_is_not_usable_until_validation(env):
    d=env.register()
    upload,data=staged(env)
    path=f"/v1/backend/test/media/uploads/{upload['upload_id']}"
    pending=env.client.get(path,headers=env.backend).json()
    assert pending['state']=='pending' and pending['media_id'] is None
    assert env.enqueue(d,media_ids=[upload['upload_id']]).status_code==422
    assert env.client.post(path+'/complete',headers=env.backend).status_code==200
    worker=Worker(env.app.state.store)
    worker.validate_one()
    for _ in range(30):
        result=env.client.get(path,headers=env.backend).json()
        if result['state']=='ready':break
        time.sleep(.05)
    assert result['state']=='ready'
    assert env.enqueue(d,media_ids=[result['media_id']]).status_code==200


@pytest.mark.parametrize('damage',['hash','invalid'])
def test_bad_direct_upload_rejected_never_attached(env,damage):
    if damage=='invalid':upload,_=staged(env,data=b'<html>bad-image</html>')
    else:upload,_=staged(env,sha256='0'*64)
    path=f"/v1/backend/test/media/uploads/{upload['upload_id']}"
    env.client.post(path+'/complete',headers=env.backend)
    Worker(env.app.state.store).validate_one()
    for _ in range(20):
        result=env.client.get(path,headers=env.backend).json()
        if result['state']=='rejected':break
        time.sleep(.05)
    assert result['state']=='rejected' and result['media_id'] is None


def test_retention_preserves_unknown_task_originals_and_idempotency(env):
    d=env.register();meta,_=env.image();env.enqueue(d,media_ids=[meta['id']])
    task=env.claim(d).json()['task'];env.receipt(d,task,'submitting');env.receipt(d,task,'unknown')
    store=env.app.state.store
    old=env.clock()-400*86400
    with store.transaction() as db:
        db.execute('UPDATE media SET created=%s',(old,));db.execute('UPDATE tasks SET updated=%s',(old,))
    assert Worker(store,media_days=1).retain()==0
    assert env.inspect(d,task).json()['state']=='unknown'
    with store.transaction() as db:assert db.execute('SELECT count(*) n FROM media').fetchone()['n']==1


def test_retention_deletes_only_old_terminal_media_and_audits(env):
    d=env.register();meta,_=env.image();env.enqueue(d,media_ids=[meta['id']])
    task=env.claim(d).json()['task'];env.receipt(d,task,'failed_pre_submit')
    store=env.app.state.store
    old=env.clock()-400*86400
    with store.transaction() as db:
        db.execute('UPDATE media SET created=%s',(old,));db.execute('UPDATE tasks SET updated=%s',(old,));db.execute('UPDATE events SET at=%s',(old,))
        key=db.execute('SELECT object_key FROM media').fetchone()['object_key']
    assert Worker(store,media_days=1).retain()==1
    Worker(store).collect_garbage()
    with store.transaction() as db:
        assert db.execute('SELECT count(*) n FROM tasks').fetchone()['n']==1
        assert db.execute('SELECT count(*) n FROM events').fetchone()['n']>0
        assert db.execute('SELECT count(*) n FROM retention_audit').fetchone()['n']>0
    with pytest.raises(Exception):store.objects.get(key)


def test_worker_externalizes_legacy_blob_without_changing_bytes(env):
    store=env.app.state.store
    data=original();meta=verify_image(data)
    with store.transaction() as db:
        db.execute('INSERT INTO media(tenant,id,sha256,md5,size,mime,data) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                   ('legacy','legacy-id',meta['sha256'],meta['md5'],meta['size'],meta['mime'],data))
    Worker(store).externalize_one()
    with store.transaction() as db:row=db.execute("SELECT * FROM media WHERE id='legacy-id'").fetchone()
    assert row['data'] is None and store.objects.get(row['object_key'])==data


def test_no_per_connection_second_database_poll(env):
    d=env.register()
    store=env.app.state.store
    Worker(store).relay()
    with env.client.websocket_connect('/v1/device/notifications') as ws:
        ws.send_json({'token':d['headers']['Authorization'][7:],'session_id':d['session_id']})
        assert ws.receive_json()['type']=='tasks_changed'
        time.sleep(.2)
        with patch.object(store,'snapshot',wraps=store.snapshot) as snapshot:
            time.sleep(1.2)
            ws.send_text('ping')
            assert ws.receive_json()['type']=='pong'
            assert snapshot.call_count==0


def test_event_notice_contains_monotonic_revision_and_sync_recovers(env):
    d=env.register()
    before=env.client.get('/v1/device/sync',headers=d['headers'],params={'session_id':d['session_id']}).json()
    task=env.enqueue(d).json()
    after=env.client.get('/v1/device/sync',headers=d['headers'],params={'session_id':d['session_id']}).json()
    assert after['cursor']>before['cursor'] and after['tasks'][0]['id']==task['id']


def test_device_socket_limit_shared_across_instances_and_release(env):
    d=env.register();presence=env.app.state.store.presence
    row=env.app.state.store.device(d['headers']['Authorization'][7:])
    peer=Presence(env.settings.redis_url,env.settings.redis_namespace,env.clock)
    try:
        assert presence.acquire_connection(row,'instance-a:one')
        assert peer.acquire_connection(row,'instance-b:two')
        assert not peer.acquire_connection(row,'instance-b:three')
        presence.release_connection(row,'instance-a:one')
        assert peer.acquire_connection(row,'instance-b:three')
        assert presence.renew_connection(row,'instance-b:three')
    finally:peer.close()
