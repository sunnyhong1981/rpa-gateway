import asyncio,threading,time
from unittest.mock import Mock
from rpa_gateway.worker import Worker


def test_dispatch_worker_never_runs_media_or_housekeeping(env):
    store=env.app.state.store;worker=Worker(store)
    worker.validate_one=Mock(side_effect=AssertionError('Media must be isolated'))
    worker.retain=Mock(side_effect=AssertionError('Retention must be isolated'))
    worker.collect_garbage=Mock(side_effect=AssertionError('GC must be isolated'))
    async def run():
        task=asyncio.create_task(worker.run('dispatch'))
        await asyncio.sleep(.15)
        task.cancel();await asyncio.gather(task,return_exceptions=True)
    asyncio.run(run())
    worker.validate_one.assert_not_called();worker.retain.assert_not_called();worker.collect_garbage.assert_not_called()


def test_slow_validation_does_not_stop_outbox_or_worker_health(env):
    store=env.app.state.store;d=env.register();env.enqueue(d)
    worker=Worker(store);started=threading.Event();released=threading.Event();relayed=threading.Event()
    real_relay=worker.relay
    def slow():started.set();released.wait(2)
    def relay():
        result=real_relay();relayed.set();return result
    worker.validate_one=slow;worker.relay=relay
    async def run():
        task=asyncio.create_task(worker.run('all'))
        try:
            assert await asyncio.to_thread(started.wait,1)
            assert await asyncio.to_thread(relayed.wait,1)
            with store.transaction() as db:
                row=db.execute("SELECT updated FROM worker_health WHERE name='dispatch'").fetchone()
                assert row is not None
        finally:
            released.set();task.cancel();await asyncio.gather(task,return_exceptions=True)
    asyncio.run(run())


def test_worker_container_health_marker_needs_no_extra_sql_connection(env,tmp_path,monkeypatch):
    import json
    path=tmp_path/'health.json';monkeypatch.setenv('RPA_GATEWAY_WORKER_HEALTH_FILE',str(path))
    Worker(env.app.state.store).observe(name='dispatch')
    health=json.loads(path.read_text());assert health['role']=='dispatch' and time.time()-health['at']<1
