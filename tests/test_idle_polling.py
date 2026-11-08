"""Compare idle polling on real local PostgreSQL and preserve bounded fresh-work latency."""
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import threading
import time
from unittest.mock import Mock

import pytest
from rpa_gateway.polling import IdleBackoff
from rpa_gateway.worker import Worker
from rpa_gateway.source_pipeline import SourcePipeline
from .source_fixture import SourceFixture, eventually


def test_idle_backoff_is_bounded_and_work_resets_it():
    delay=IdleBackoff(.05,.5)
    assert [delay.next_delay(False) for _ in range(6)]==[.1,.2,.4,.5,.5,.5]
    assert delay.next_delay(True)==.05
    assert delay.next_delay(False)==.1


@pytest.mark.parametrize('active,maximum',[(0,1),(-1,1),(1,.5)])
def test_polling_bounds_are_validated(active,maximum):
    with pytest.raises(ValueError):IdleBackoff(active,maximum)


def test_idle_dispatch_backs_off_and_keeps_health_independent(env):
    worker=Worker(env.app.state.store);worker.relay=Mock(return_value=0);worker.sweep=Mock(return_value=0)
    worker.observe=Mock()
    async def run():
        task=asyncio.create_task(worker.run('dispatch'))
        try:await asyncio.sleep(1.8)
        finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
    asyncio.run(run())
    assert 3<=worker.relay.call_count<=6
    assert worker.sweep.call_count==worker.relay.call_count
    assert worker.observe.call_count>=2


def test_idle_source_reduces_transactions_and_handles_new_jobs(env,monkeypatch):
    results={}
    original_transaction=env.app.state.store.transaction
    class FixedPolling:
        def __init__(self,active,maximum):self.active=active
        def next_delay(self,worked):return self.active
    with closing(SourceFixture(env,groups=2)) as fixture:
        for profile in ['fixed','adaptive']:
            pipeline=SourcePipeline(env.app.state.store,env.settings.database,fixture.configs,instance='idle_'+profile)
            counted={};lock=threading.Lock()
            def transaction(*args,**kwargs):
                name=threading.current_thread().name
                if name.startswith('source-'):
                    with lock:counted[name]=counted.get(name,0)+1
                return original_transaction(*args,**kwargs)
            with monkeypatch.context() as patch:
                patch.setattr(env.app.state.store,'transaction',transaction)
                if profile=='fixed':patch.setattr('rpa_gateway.source_pipeline.IdleBackoff',FixedPolling)
                try:
                    pipeline.start()
                    time.sleep(1.2)  # Warm to the capped idle interval before sampling.
                    with lock:counted.clear()
                    time.sleep(2)
                    with lock:counts=dict(counted)
                    result={'seconds':2,'gateway_transactions':counts,'total_gateway_transactions':sum(counts.values())}
                    with original_transaction() as db:
                        previous=db.execute("SELECT count(*) n FROM tasks WHERE state='queued'").fetchone()['n']
                    began=time.monotonic()
                    fixture.add(group=0);fixture.add(group=1)
                    def ready():
                        with original_transaction() as db:
                            return db.execute("SELECT count(*) n FROM tasks WHERE state='queued'").fetchone()['n']>=previous+2
                    eventually(ready,timeout=2)
                    result['new_jobs_ready_seconds']=round(time.monotonic()-began,4)
                    results[profile]=result
                finally:pipeline.close()
    path=os.environ.get('RPA_GATEWAY_POLLING_REPORT')
    if path:Path(path).write_text(json.dumps(results,indent=2))
    # Capture and health retain their existing cadence; only idle work scans back off.
    assert results['adaptive']['total_gateway_transactions']<results['fixed']['total_gateway_transactions']*.6
    assert results['adaptive']['new_jobs_ready_seconds']<1.5


def test_empty_capture_never_acquires_or_releases_a_write_lease(env):
    with closing(SourceFixture(env,groups=1)) as fixture:
        pipeline=SourcePipeline(env.app.state.store,env.settings.database,fixture.configs)
        try:
            bridge=next(iter(pipeline.bridges.values()))
            bridge.queue.capture_lease=Mock(side_effect=AssertionError('No rows, no capture writes'))
            bridge.queue.release_capture=Mock(side_effect=AssertionError('No lease exists to release'))
            assert bridge.capture()==0
        finally:pipeline.close()
