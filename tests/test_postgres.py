import concurrent.futures
import json
import threading
import time
import psycopg
import pytest
from fastapi import HTTPException
from rpa_gateway.database import Database
from rpa_gateway.store import Store
from rpa_gateway.models import Claim, Receipt


def test_runtime_rejects_sqlite_and_chatwoot_database_paths():
    for value in ['/tmp/db.sqlite3','postgresql://user@host/chatwoot','dbname=postgres']:
        with pytest.raises((ValueError,psycopg.ProgrammingError)):
            Database(value)


def test_two_independent_pools_cannot_claim_same_device_twice(env):
    d=env.register()
    env.enqueue(d);env.enqueue(d)
    a=env.app.state.store
    b=Store(env.settings.database,clock=env.clock,pool_size=4,presence=a.presence,objects=a.objects)
    try:
        credential=a.device(d['headers']['Authorization'][7:])
        def claim(index):return (a if index%2 else b).claim(credential,Claim(session_id=d['session_id']))
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results=list(pool.map(claim,range(12)))
        assert sum(row['task'] is not None for row in results)==1
    finally:b.close()


def test_other_device_can_claim_while_one_device_is_locked(env):
    a=env.register(device='a');b=env.register(device='b')
    env.enqueue(a);env.enqueue(b)
    store=env.app.state.store
    with store.transaction() as db:
        store.lock_device(db,'test','a')
        # Same device is intentionally locked. Another device must not wait for it.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            claimed=executor.submit(env.claim,b).result(timeout=1.5)
        assert claimed.status_code==200 and claimed.json()['task'] is not None


def test_read_only_authentication_does_not_wait_for_device_row_lock(env):
    d=env.register()
    store=env.app.state.store
    with store.transaction() as db:
        store.lock_device(db,'test','desktop')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            result=executor.submit(store.device,d['headers']['Authorization'][7:]).result(timeout=1.5)
        assert result['id']=='desktop'


def test_concurrent_duplicate_receipts_return_one_fresh_fence(env):
    d=env.register();env.enqueue(d)
    task=env.claim(d).json()['task']
    store=env.app.state.store
    credential=store.device(d['headers']['Authorization'][7:])
    receipt=Receipt(session_id=d['session_id'],lease_token=task['lease_token'],event_id='same-concurrent',state='submitting')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results=list(executor.map(lambda _:store.receipt(credential,task['id'],receipt),range(8)))
    assert sum(not result['duplicate'] for result in results)==1
    assert all(result['state']=='submitting' for result in results)


def test_concurrent_duplicate_task_creation_stays_idempotent(env):
    d=env.register()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        responses=list(executor.map(lambda _:env.enqueue(d,source_message_id='racing'),range(8)))
    assert all(r.status_code==200 for r in responses)
    assert len({r.json()['id'] for r in responses})==1


def test_database_unique_constraint_prevents_second_active_task(env):
    d=env.register();env.enqueue(d);second=env.enqueue(d).json()
    claimed=env.claim(d).json()['task']
    if claimed['id']==second['id']:
        with env.app.state.store.transaction() as db:
            second=db.execute("SELECT id FROM tasks WHERE state='queued' LIMIT 1").fetchone()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with env.app.state.store.transaction() as db:
            db.execute("UPDATE tasks SET state='leased' WHERE id=%s",(second['id'],))


def test_sweeper_skips_a_locked_device(env):
    d=env.register();task=env.enqueue(d,ttl_seconds=15).json()
    env.clock.advance(16)
    store=env.app.state.store
    with store.transaction() as holder:
        store.lock_device(holder,'test','desktop')
        def sweep():
            with store.transaction() as db:
                store.sweep(db)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(sweep).result(timeout=1.5)
    assert env.inspect(d,task).json()['state']=='expired'
