"""Deterministic worker failures: no production connections or desktop activity."""
from concurrent.futures import Future
from contextlib import contextmanager
from types import SimpleNamespace
import threading
from unittest.mock import Mock
import pytest
from psycopg_pool import PoolTimeout
from rpa_gateway.source_pipeline import SourcePipeline


def pipeline():
    p=SourcePipeline.__new__(SourcePipeline)
    p.store=SimpleNamespace(clock=lambda:100.0)
    p.settings=SimpleNamespace(workers=2,report_workers=2)
    p.stop=threading.Event();p.preparing={};p.reporting={};p.errors={};p.progress={}
    p.queue=Mock();p.queue.claim.return_value=None;p.queue.claim_report.return_value=None
    p.preparers=Mock();p.reporters=Mock();p.bridges={};p.instance='local-test'
    @contextmanager
    def transaction():yield Mock()
    p.store.transaction=transaction
    return p


@pytest.mark.parametrize('role',['receipts','prepare'])
def test_pool_timeout_future_is_removed_and_new_work_is_scheduled(role):
    p=pipeline();pending=p.reporting if role=='receipts'else p.preparing
    failed=Future();failed.set_exception(PoolTimeout('temporary database outage'))
    sibling=Future();sibling.set_result(True)
    pending.update({failed:{'message_id':1},sibling:{'message_id':2}})
    job={'tenant':'test','inbox':'group','message_id':3};bridge=Mock();p.bridges[('test','group')]=bridge
    claim=p.queue.claim_report if role=='receipts'else p.queue.claim
    claim.side_effect=[job,None,None,None]
    executor=p.reporters if role=='receipts'else p.preparers
    replacement=Future();executor.submit.return_value=replacement
    callback=p.receipt_once if role=='receipts'else p.prepare_once
    with pytest.raises(PoolTimeout):callback()
    assert failed not in pending and sibling not in pending
    assert list(pending)==[replacement]
    executor.submit.assert_called_once()
    # No second exception from the completed failed job, and no resubmission.
    callback();executor.submit.assert_called_once()
    replacement.set_result(True);callback()
    assert not pending and p.progress[role]==100


def test_pool_failure_while_refilling_does_not_restore_failed_future():
    p=pipeline();failed=Future();failed.set_exception(PoolTimeout('report failed'))
    p.reporting[failed]={};p.queue.claim_report.side_effect=PoolTimeout('database still down')
    with pytest.raises(PoolTimeout):p.receipt_once()
    assert not p.reporting
    p.queue.claim_report.side_effect=None;p.receipt_once()
    assert not p.reporting


def depth(exc):
    count=0;tb=exc.__traceback__
    while tb:count+=1;tb=tb.tb_next
    return count


def test_reaped_exception_traceback_does_not_grow_on_later_ticks():
    p=pipeline();error=PoolTimeout('outage');future=Future();future.set_exception(error);p.reporting[future]={}
    with pytest.raises(PoolTimeout):p.receipt_once()
    before=depth(error)
    for _ in range(500):p.receipt_once()
    assert depth(error)==before


def test_inflight_future_is_retained_and_not_duplicated():
    p=pipeline();future=Future();p.reporting[future]={}
    for _ in range(10):p.receipt_once()
    assert future in p.reporting;p.reporters.submit.assert_not_called()


@pytest.mark.parametrize('mode',['error','stalled','missing'])
def test_health_marker_cannot_mask_failed_stalled_or_unstarted_role(tmp_path,monkeypatch,mode):
    p=pipeline();path=tmp_path/'health.json';monkeypatch.setenv('RPA_GATEWAY_SOURCE_HEALTH_FILE',str(path))
    path.write_text('previous healthy marker');p.progress['receipts']=100
    if mode=='error':p.errors['receipts']='PoolTimeout'
    elif mode=='stalled':p.progress['receipts']=60
    else:p.progress.clear()
    p.heartbeat(('receipts',));assert not path.exists()
    p.errors.clear();p.progress['receipts']=100;p.heartbeat(('receipts',));assert path.exists()


def test_idle_receipt_worker_is_healthy_after_successful_queue_scan(tmp_path,monkeypatch):
    p=pipeline();path=tmp_path/'health.json';monkeypatch.setenv('RPA_GATEWAY_SOURCE_HEALTH_FILE',str(path))
    p.receipt_once();p.heartbeat(('receipts',));assert path.exists()


def test_real_queue_recovers_reports_after_pool_outage_without_new_native_tasks(env):
    from contextlib import closing
    from unittest.mock import patch
    from .source_fixture import SourceFixture,eventually
    with closing(SourceFixture(env,groups=2)) as fixture, closing(SourcePipeline(
            env.app.state.store,env.settings.database,fixture.configs)) as p:
        replies=[fixture.add(group=0),fixture.add(group=1)]
        p.capture_once();p.prepare_once()
        def prepared():
            with p.store.transaction() as db:return db.execute('SELECT count(*) n FROM tasks').fetchone()['n']==2
        eventually(prepared)
        device=fixture.devices[0]
        for _ in range(2):
            task=env.claim(device).json()['task'];assert task
            assert env.receipt(device,task,'submitting').status_code==200
            assert env.receipt(device,task,'submitted').status_code==200
            fixture.archive(task);p.archive_once()
            assert env.inspect(device,task).json()['state']=='confirmed'
        original=p.queue.finish_report;calls={}
        def transient_pool_failure(job,*args,**kwargs):
            mid=job['message_id'];calls[mid]=calls.get(mid,0)+1
            # Failure recording also cannot acquire the pool during the outage.
            if calls[mid]<=2:raise PoolTimeout('injected connection pool outage')
            return original(job,*args,**kwargs)
        def confirmed():
            with p.store.transaction() as db:
                return db.execute("SELECT count(*) n FROM source_jobs WHERE reported_state='delivered'").fetchone()['n']==2
        with patch.object(p.queue,'finish_report',side_effect=transient_pool_failure):
            p.start('receipts')
            eventually(lambda:len(calls)==2 and all(n>=2 for n in calls.values()))
            env.clock.advance(31)  # Expired report leases become eligible again.
            eventually(confirmed,timeout=8)
        with p.store.transaction() as db:
            assert db.execute('SELECT count(*) n FROM tasks').fetchone()['n']==2
            assert db.execute("SELECT count(*) n FROM tasks WHERE state='confirmed'").fetchone()['n']==2
        assert {x['message_id'] for x in fixture.receipts}=={x['id'] for x in replies}
        assert all(x['status']=='delivered' for x in fixture.receipts)
