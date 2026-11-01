import json,time,threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from unittest.mock import patch
import pytest
from psycopg.errors import ReadOnlySqlTransaction
from rpa_gateway.source_pipeline import SourcePipeline
from rpa_gateway.source_queue import SourceQueue,SourceSettings,LeaseLost
from rpa_gateway.source_resources import SourceReader,SourceInfrastructureUnavailable
from .source_fixture import SourceFixture,photo,eventually


def count(store,state=None):
    with store.transaction() as db:
        return db.execute('SELECT count(*) n FROM tasks'+(' WHERE state=%s' if state else ''),((state,) if state else ())).fetchone()['n']


def test_real_loopback_hmac_and_originals_prepare_and_confirm(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add(images=[photo(),photo('blue'),photo('green')])
        pipeline.start()
        eventually(lambda:count(pipeline.store)==1)
        d=fixture.devices[0];task=env.claim(d).json()['task'];assert len(task['media'])==3
        env.receipt(d,task,'submitting');env.receipt(d,task,'submitted');fixture.archive(task)
        eventually(lambda:env.inspect(d,task).json()['state']=='confirmed')
        eventually(lambda:any(r['status']=='delivered' for r in fixture.receipts))
        assert sum(r['status']=='delivered' for r in fixture.receipts)==1
        assert all(r['message_id']==reply['id'] for r in fixture.receipts)


def test_slow_picture_does_not_block_capture_or_archive(env):
    with closing(SourceFixture(env,groups=2)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        fixture.add(group=0,images=[photo()],file_delay=2)
        fixture.add(group=1)
        pipeline.start()
        assert fixture.file_started.wait(2)
        # A separate group can publish while the first group still waits on a real socket.
        eventually(lambda:count(pipeline.store)==1,timeout=1.5)
        with pipeline.store.transaction() as db:row=db.execute('SELECT id,payload FROM tasks').fetchone()
        d=fixture.devices[0];task=env.claim(d).json()['task']
        assert task['payload']['inbox_id']=='inbox2'
        env.receipt(d,task,'submitting');env.receipt(d,task,'submitted');fixture.archive(task)
        eventually(lambda:env.inspect(d,task).json()['state']=='confirmed',timeout=1.5)
        fixture.add(group=1)
        eventually(lambda:count(pipeline.store)>=2)


def test_source_reader_reuses_a_read_only_pool(env):
    with closing(SourceReader(env.settings.database,size=1)) as reader:
        ids=[reader.read('SELECT pg_backend_pid() pid')[0]['pid'] for _ in range(4)]
        assert len(set(ids))==1
        with pytest.raises(ReadOnlySqlTransaction):reader.read('CREATE TABLE must_not_write(id int)')


def test_queue_wait_does_not_become_180_second_image_failure(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        fixture.add(images=[photo()],age=200)
        pipeline.start()
        eventually(lambda:count(pipeline.store)==1)
        assert not fixture.receipts


def test_global_preparation_cap_and_group_fairness_are_shared(env):
    settings=SourceSettings(global_jobs=4,per_group_jobs=2)
    with closing(SourceFixture(env,groups=3)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        for g in range(3):
            for _ in range(5):fixture.add(group=g)
        pipeline.capture_once()
        other=SourceQueue(pipeline.store,pipeline.queue.routes,settings)
        jobs=[pipeline.queue.claim() for _ in range(3)]
        assert len({j['inbox'] for j in jobs})==3
        with ThreadPoolExecutor(max_workers=12) as pool:
            results=list(pool.map(lambda i:(pipeline.queue if i%2 else other).claim(),range(12)))
        jobs.extend(j for j in results if j)
        assert len(jobs)<=4
        # Contention is nonblocking; finish filling available slots deterministically.
        while len(jobs)<4:
            job=other.claim()
            if job:jobs.append(job)
        assert other.claim() is None
        from collections import Counter
        assert max(Counter(j['inbox'] for j in jobs).values())<=2


def test_expired_worker_cannot_finalize_or_fail_reclaimed_job(env):
    settings=SourceSettings(lease_seconds=1,prepare_seconds=10)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        fixture.add();pipeline.capture_once();old=pipeline.queue.claim()
        env.clock.advance(2);new=pipeline.queue.claim();assert new['lease_token']!=old['lease_token']
        bridge=pipeline.bridges[('test','inbox1')]
        from rpa_gateway.source_queue import PreparationLease
        with PreparationLease(pipeline.queue,old) as guard:
            body=bridge.prepare_body(old,guard)
            with pytest.raises(LeaseLost):pipeline.queue.finalize(old,body)
        assert not pipeline.queue.retry(old,RuntimeError('late worker'))
        bridge.prepare(new)
        assert count(pipeline.store)==1


def test_task_publication_and_job_link_survive_lost_commit_ack(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add();pipeline.capture_once();job=pipeline.queue.claim();real=pipeline.queue.finalize
        def lost(*args,**kwargs):
            real(*args,**kwargs);raise ConnectionError('commit ack lost')
        with patch.object(pipeline.queue,'finalize',side_effect=lost):pipeline.bridges[('test','inbox1')].prepare(job)
        with pipeline.store.transaction() as db:
            linked=db.execute('SELECT task,state FROM source_jobs WHERE message_id=%s',(reply['id'],)).fetchone()
        assert linked['task'] and linked['state']=='queued' and count(pipeline.store)==1
        assert pipeline.queue.claim() is None
        assert pipeline.queue.claim_report() is None
        assert not fixture.receipts


def test_route_pause_during_io_prevents_task_publication(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        fixture.add(images=[photo()],file_delay=.3);pipeline.start()
        assert fixture.file_started.wait(2)
        with pipeline.store.transaction() as db:db.execute("UPDATE source_routes SET mode='paused',generation=generation+1 WHERE inbox='inbox1'")
        eventually(lambda:not pipeline.preparing)
        assert count(pipeline.store)==0 and not fixture.receipts


def test_bad_image_retries_are_bounded_and_only_then_fall_back(env):
    settings=SourceSettings(max_attempts=2)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        reply=fixture.add(images=[b'invalid image'])
        pipeline.capture_once()
        for _ in range(2):
            job=pipeline.queue.claim();assert job
            pipeline.bridges[('test','inbox1')].prepare(job);env.clock.advance(5)
        assert pipeline.queue.claim() is None and count(pipeline.store)==0
        report=pipeline.queue.claim_report();assert report and report['state']=='failed'
        pipeline.bridges[('test','inbox1')].publish_report(report)
        assert pipeline.queue.claim_report() is None
        assert len(fixture.receipts)==1 and fixture.receipts[0]['status']=='image_failed'
        assert fixture.receipts[0]['send_not_attempted'] is True


def test_queue_deadline_is_distinct_from_execution_deadline(env):
    settings=SourceSettings(queue_seconds=10,prepare_seconds=2,lease_seconds=1)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        reply=fixture.add(images=[photo()]);pipeline.capture_once();env.clock.advance(11)
        assert pipeline.queue.claim() is None
        with pipeline.store.transaction() as db:r=db.execute('SELECT state,error_type,started_at FROM source_jobs WHERE message_id=%s',(reply['id'],)).fetchone()
        assert r['state']=='failed' and r['error_type']=='QueueDeadlineExceeded' and r['started_at'] is None
        assert not fixture.receipts


def test_duplicate_original_is_not_uploaded_again(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        fixture.add(images=[photo()]);fixture.add(images=[photo()]);pipeline.capture_once()
        with patch.object(pipeline.store.objects,'put',wraps=pipeline.store.objects.put) as upload:
            for _ in range(2):
                job=pipeline.queue.claim();pipeline.bridges[('test',job['inbox'])].prepare(job)
        assert count(pipeline.store)==2 and upload.call_count==1


def test_two_preparers_share_network_and_decode_caps(env):
    from rpa_gateway.objects import verify_image as real_verify
    settings=SourceSettings(workers=4,network_slots=3,decode_slots=1)
    lock=threading.Lock();active=0;maximum=0
    def decode(data):
        nonlocal active,maximum
        with lock:active+=1;maximum=max(maximum,active)
        try:time.sleep(.025);return real_verify(data)
        finally:
            with lock:active-=1
    with closing(SourceFixture(env,groups=8)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as first,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as second:
        for group in range(8):fixture.add(group=group,images=[photo('#%06x'%(group+1))],file_delay=.06)
        first.capture_once()
        with patch('rpa_gateway.source_resources.verify_image',side_effect=decode):
            first.start('prepare');second.start('prepare')
            eventually(lambda:count(first.store)==8,timeout=5)
        assert fixture.max_files<=3 and maximum==1
        assert first.resources.redis.zcard(first.resources.prefix+'network')==0
        assert first.resources.redis.zcard(first.resources.prefix+'decode')==0
        with first.store.transaction() as db:
            assert db.execute('SELECT count(DISTINCT task) n FROM source_jobs').fetchone()['n']==8


def test_shared_limit_outage_keeps_unsubmitted_work_pending(env):
    import redis
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add(images=[photo()]);pipeline.capture_once();job=pipeline.queue.claim()
        with patch.object(pipeline.resources.redis,'eval',side_effect=redis.ConnectionError('local injected outage')):
            pipeline.bridges[('test','inbox1')].prepare(job)
        with pipeline.store.transaction() as db:r=db.execute('SELECT state,attempts FROM source_jobs WHERE message_id=%s',(reply['id'],)).fetchone()
        assert r['state']=='pending' and r['attempts']==0 and count(pipeline.store)==0
        assert not fixture.receipts


def test_reader_never_changes_another_inbox_queue(env):
    settings=SourceSettings(max_attempts=1)
    with closing(SourceFixture(env,groups=2)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        fixture.add(group=0);other=fixture.add(group=1);pipeline.capture_once()
        with pipeline.store.transaction() as db:db.execute('UPDATE source_jobs SET attempts=1 WHERE message_id=%s',(other['id'],))
        scoped=SourceQueue(pipeline.store,[('test','inbox1')],settings);assert scoped.claim()
        with pipeline.store.transaction() as db:r=db.execute('SELECT state FROM source_jobs WHERE message_id=%s',(other['id'],)).fetchone()
        assert r['state']=='pending'


def test_source_edit_before_fence_still_prevents_native_submission(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add();pipeline.capture_once();assert pipeline.queue.claim()  # A lease here has not published anything.
        # Recover the deliberately abandoned lease, then prepare the authoritative source.
        env.clock.advance(16);job=pipeline.queue.claim();pipeline.bridges[('test','inbox1')].prepare(job)
        d=fixture.devices[0];task=env.claim(d).json()['task']
        with pipeline.store.transaction() as db:db.execute("UPDATE source_messages SET content='edited' WHERE id=%s",(reply['id'],))
        assert env.receipt(d,task,'submitting').status_code==409
        assert env.inspect(d,task).json()['state']=='leased'


def test_reporter_does_not_fallback_an_unlinked_unknown_task(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add();pipeline.capture_once();job=pipeline.queue.claim();pipeline.bridges[('test','inbox1')].prepare(job)
        d=fixture.devices[0];task=env.claim(d).json()['task'];env.receipt(d,task,'submitting');env.receipt(d,task,'unknown')
        with pipeline.store.transaction() as db:db.execute("UPDATE source_jobs SET task=NULL,state='failed',reported_state=NULL WHERE message_id=%s",(reply['id'],))
        assert pipeline.queue.claim_report() is None
        assert pipeline.queue.claim_report() is None and not fixture.receipts
        with pipeline.store.transaction() as db:r=db.execute('SELECT task FROM source_jobs WHERE message_id=%s',(reply['id'],)).fetchone()
        assert r['task']==task['id']


def test_utc_source_timestamp_does_not_use_macos_local_timezone(env):
    from datetime import datetime,timezone
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add()
        naive=datetime.fromtimestamp(env.clock(),timezone.utc).replace(tzinfo=None)
        with pipeline.store.transaction() as db:pipeline.queue.enqueue(db,'test','inbox1',reply['id'],naive)
        with pipeline.store.transaction() as db:r=db.execute('SELECT source_created_at,queue_until FROM source_jobs WHERE message_id=%s',(reply['id'],)).fetchone()
        assert r['source_created_at']==env.clock() and r['queue_until']==env.clock()+300


def test_dead_preparer_process_is_reclaimed_without_republishing(env,tmp_path):
    import subprocess,sys,os
    settings=SourceSettings(lease_seconds=1,prepare_seconds=10)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        reply=fixture.add();pipeline.capture_once()
        config=tmp_path/'worker-private.json';config.write_text(json.dumps({'dsn':env.settings.database,'clock':env.clock()}));config.chmod(0o600)
        script="""
import json,sys,os
from pathlib import Path
from rpa_gateway.store import Store
from rpa_gateway.source_queue import SourceQueue,SourceSettings
c=json.loads(Path(sys.argv[1]).read_text());s=Store(c['dsn'],clock=lambda:c['clock'],pool_size=1)
j=SourceQueue(s,[('test','inbox1')],SourceSettings(lease_seconds=1,prepare_seconds=10)).claim()
print(json.dumps({'claimed':j is not None,'message_id':j['message_id'] if j else None}),flush=True)
os._exit(0)
"""
        result=subprocess.run([sys.executable,'-c',script,str(config)],capture_output=True,text=True,timeout=10,check=True)
        assert json.loads(result.stdout)['claimed']
        env.clock.advance(2);job=pipeline.queue.claim();assert job['message_id']==reply['id'] and job['attempts']==2
        pipeline.bridges[('test','inbox1')].prepare(job)
        assert count(pipeline.store)==1


def test_deadline_is_rechecked_after_waiting_for_database_locks(env):
    settings=SourceSettings(lease_seconds=1,prepare_seconds=10)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        fixture.add();pipeline.capture_once();job=pipeline.queue.claim()
        from rpa_gateway.source_queue import PreparationLease
        with PreparationLease(pipeline.queue,job) as budget:body=pipeline.bridges[('test','inbox1')].prepare_body(job,budget)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pipeline.store.transaction() as db:
                pipeline.store.lock_device(db,'test',job['device'])
                future=pool.submit(pipeline.queue.finalize,job,body)
                time.sleep(.05);assert not future.done();env.clock.advance(2)
            with pytest.raises(LeaseLost):future.result(timeout=2)
        assert count(pipeline.store)==0


def test_conflicting_worker_limits_fail_loudly(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        with pytest.raises(ValueError,match='shared limit'):
            SourceQueue(pipeline.store,pipeline.queue.routes,SourceSettings(global_jobs=16))


def test_ready_backpressure_does_not_block_another_device(env):
    settings=SourceSettings(ready_per_device=2)
    with closing(SourceFixture(env,groups=2,devices=2)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        for _ in range(5):fixture.add(group=0)
        fixture.add(group=1);pipeline.start()
        eventually(lambda:count(pipeline.store)==3 and not pipeline.preparing)
        # In-flight preparations can overlap a completion; admission and finalization
        # jointly enforce the bound rather than dropping the durable source message.
        with pipeline.store.transaction() as db:
            ready=db.execute("SELECT device,count(*) n FROM tasks GROUP BY device").fetchall()
        assert len(ready)==2 and {r['device']:r['n'] for r in ready}=={'source_device_0':2,'source_device_1':1}


def test_current_schema_startup_does_not_repeat_ddl_during_live_reads(env):
    from rpa_gateway.database import Database
    with env.app.state.store.transaction() as db:
        db.execute('SELECT * FROM tasks').fetchall()  # Hold a normal reader lock.
        def startup():
            database=Database(env.settings.database,pool_size=1)
            try:database.initialize()
            finally:database.close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(startup).result(timeout=1.5)


def test_one_broken_route_does_not_starve_other_capture_or_archive(env):
    with closing(SourceFixture(env,groups=2)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        fixture.add(group=1)
        bad=pipeline.bridges[('test','inbox1')]
        with patch.object(bad,'capture',side_effect=RuntimeError('injected bad route')):
            with pytest.raises(RuntimeError):pipeline.capture_once()
        job=pipeline.queue.claim();assert job['inbox']=='inbox2'
        pipeline.bridges[('test','inbox2')].prepare(job)
        d=fixture.devices[0];task=env.claim(d).json()['task']
        assert env.receipt(d,task,'submitting').status_code==200
        assert env.receipt(d,task,'submitted').status_code==200
        fixture.archive(task)
        with patch.object(bad,'reconcile',side_effect=RuntimeError('injected bad route')):
            with pytest.raises(RuntimeError):pipeline.archive_once()
        assert env.inspect(d,task).json()['state']=='confirmed'


def test_queued_original_is_not_replaced_by_a_link_fallback(env):
    from psycopg.types.json import Jsonb
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        original=fixture.add(images=[photo()]);fallback=fixture.add()
        fallback['attributes']['in_reply_to']=original['attributes']['in_reply_to']
        fallback['attributes']['rpa_pod_link_fallback']={'image_message_id':original['id']}
        with pipeline.store.transaction() as db:db.execute('UPDATE source_messages SET attributes=%s WHERE id=%s',(Jsonb(fallback['attributes']),fallback['id']))
        pipeline.capture_once();job=pipeline.queue.claim();pipeline.bridges[('test','inbox1')].prepare(job)
        job=pipeline.queue.claim();assert job['message_id']==fallback['id'];pipeline.bridges[('test','inbox1')].prepare(job)
        assert count(pipeline.store)==1 and not fixture.receipts
        with pipeline.store.transaction() as db:r=db.execute('SELECT state,attempts FROM source_jobs WHERE message_id=%s',(fallback['id'],)).fetchone()
        assert r['state']=='pending' and r['attempts']==0
        d=fixture.devices[0];task=env.claim(d).json()['task'];env.receipt(d,task,'submitting');env.receipt(d,task,'submitted');fixture.archive(task)
        pipeline.archive_once();env.clock.advance(3)
        job=pipeline.queue.claim();assert job['message_id']==fallback['id'];pipeline.bridges[('test','inbox1')].prepare(job)
        assert count(pipeline.store)==1
        # Reconcile the legitimate delivery, then cancel its held link without a new desktop task.
        for _ in range(2):
            report=pipeline.queue.claim_report();assert report
            pipeline.bridges[('test','inbox1')].publish_report(report)
        assert [r['status'] for r in fixture.receipts]==['delivered','discarded']
        assert pipeline.queue.claim_report() is None


def test_execution_deadline_cannot_publish_a_late_image_result(env):
    settings=SourceSettings(lease_seconds=1,prepare_seconds=1)
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
        fixture.add(images=[photo()],file_delay=.25);pipeline.capture_once();job=pipeline.queue.claim()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(pipeline.bridges[('test','inbox1')].prepare,job)
            assert fixture.file_started.wait(1)
            env.clock.advance(2)
            assert future.result(timeout=2) is None
        assert count(pipeline.store)==0 and not fixture.receipts
        eventually(lambda:pipeline.resources.redis.zcard(pipeline.resources.prefix+'network')==0,timeout=2)
        next_job=pipeline.queue.claim();assert next_job and next_job['lease_token']!=job['lease_token']


def test_source_container_health_check_reuses_the_existing_database_pool(env,tmp_path,monkeypatch):
    import json
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        marker=tmp_path/'health.json';monkeypatch.setenv('RPA_GATEWAY_SOURCE_HEALTH_FILE',str(marker))
        pipeline.heartbeat(('prepare',))
        value=json.loads(marker.read_text())
        assert value['roles']==['prepare'] and value['instance']==pipeline.instance
        assert time.time()-value['at']<1


def test_legacy_closed_source_without_report_marker_is_not_replayed(env):
    with closing(SourceFixture(env)) as fixture,closing(SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)) as pipeline:
        reply=fixture.add();pipeline.capture_once()
        with pipeline.store.transaction() as db:
            db.execute("UPDATE source_jobs SET state='source_closed',reported_state=NULL,error_type='' WHERE message_id=%s",(reply['id'],))
        assert pipeline.queue.claim_report() is None
        assert not fixture.receipts and count(pipeline.store)==0
