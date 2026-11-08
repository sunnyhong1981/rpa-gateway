from contextlib import closing

from rpa_gateway.source_pipeline import SourcePipeline
from rpa_gateway.source_queue import SourceSettings
from .source_fixture import SourceFixture


def test_reversed_preparation_completion_keeps_original_reply_order(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs
    )) as pipeline:
        first = fixture.add()
        second = fixture.add()
        pipeline.capture_once()
        earlier = pipeline.queue.claim()
        later = pipeline.queue.claim()
        assert [earlier['message_id'], later['message_id']] == [first['id'], second['id']]
        bridge = pipeline.bridges[('test', 'inbox1')]
        bridge.prepare(later)
        env.clock.advance(.1)
        bridge.prepare(earlier)

        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == first['id']


def test_later_ready_reply_waits_for_earlier_preparation(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs
    )) as pipeline:
        fixture.add()
        fixture.add()
        pipeline.capture_once()
        earlier = pipeline.queue.claim()
        later = pipeline.queue.claim()
        pipeline.bridges[('test', 'inbox1')].prepare(later)

        assert env.claim(fixture.devices[0]).json()['task'] is None
        pipeline.bridges[('test', 'inbox1')].prepare(earlier)
        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == earlier['message_id']


def test_other_group_on_same_device_can_send_while_first_group_waits(env):
    with closing(SourceFixture(env, groups=2)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs
    )) as pipeline:
        fixture.add()
        fixture.add()
        other = fixture.add(group=1)
        pipeline.capture_once()
        jobs = {job['message_id']: job for job in (pipeline.queue.claim() for _ in range(3))}
        for mid in sorted(jobs)[1:]:
            job = jobs[mid]
            pipeline.bridges[('test', job['inbox'])].prepare(job)

        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == other['id']


def test_preparation_failure_unblocks_later_reply(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs,
        settings=SourceSettings(max_attempts=1)
    )) as pipeline:
        fixture.add()
        second = fixture.add()
        pipeline.capture_once()
        earlier = pipeline.queue.claim()
        later = pipeline.queue.claim()
        pipeline.bridges[('test', 'inbox1')].prepare(later)
        assert env.claim(fixture.devices[0]).json()['task'] is None
        pipeline.queue.retry(earlier, ValueError('Invalid original image'))

        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == second['id']


def test_expired_preparation_budget_unblocks_later_reply(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs,
        settings=SourceSettings(queue_seconds=5, prepare_seconds=2, lease_seconds=1)
    )) as pipeline:
        fixture.add()
        second = fixture.add()
        pipeline.capture_once()
        earlier = pipeline.queue.claim()
        later = pipeline.queue.claim()
        pipeline.bridges[('test', 'inbox1')].prepare(later)
        # A transient preparation failure returns the first reply to its bounded queue.
        pipeline.queue.retry(earlier, ConnectionError('Unavailable identity endpoint'), infrastructure=True)
        assert env.claim(fixture.devices[0]).json()['task'] is None
        env.clock.advance(8)
        assert pipeline.queue.claim() is None

        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == second['id']


def test_submitted_reply_does_not_hold_next_reply_until_archive_confirmation(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs
    )) as pipeline:
        fixture.add()
        second = fixture.add()
        pipeline.capture_once()
        for _ in range(2):
            pipeline.bridges[('test', 'inbox1')].prepare(pipeline.queue.claim())
        device = fixture.devices[0]
        first = env.claim(device).json()['task']
        assert env.receipt(device, first, 'submitting').status_code == 200
        assert env.receipt(device, first, 'submitted').status_code == 200
        assert env.claim(device).json()['task'] is None
        assert env.receipt(device, first, 'desktop_released', native_submit_complete=True,
                           input_cleared=True, group_verified=True).status_code == 200

        task = env.claim(device).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == second['id']
        assert env.inspect(device, first).json()['state'] == 'submitted'


def test_unlinked_backend_tasks_keep_existing_claim_order(env):
    device = env.register()
    first = env.enqueue(device).json()
    env.clock.advance(.1)
    env.enqueue(device)
    assert env.claim(device).json()['task']['id'] == first['id']


def test_later_preparations_cannot_fill_slot_needed_by_first_reply(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs,
        settings=SourceSettings(ready_per_device=2, per_group_jobs=3)
    )) as pipeline:
        first = fixture.add()
        fixture.add()
        third = fixture.add()
        pipeline.capture_once()
        earlier, later = [pipeline.queue.claim() for _ in range(2)]
        bridge = pipeline.bridges[('test', 'inbox1')]
        bridge.prepare(later)
        pipeline.queue.retry(earlier, ConnectionError('Temporary identity outage'), infrastructure=True)
        last = pipeline.queue.claim()
        assert last['message_id'] == third['id']
        bridge.prepare(last)
        with pipeline.store.transaction() as db:
            assert db.execute('SELECT count(*) n FROM tasks').fetchone()['n'] == 1
            waiting = db.execute('SELECT state,task FROM source_jobs WHERE message_id=%s', (third['id'],)).fetchone()
            assert waiting['state'] == 'pending' and waiting['task'] is None

        env.clock.advance(3)
        retried = pipeline.queue.claim()
        assert retried['message_id'] == first['id']
        bridge.prepare(retried)
        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == first['id']
        with pipeline.store.transaction() as db:
            assert db.execute('SELECT count(*) n FROM tasks').fetchone()['n'] == 2


def test_unprepared_history_below_route_floor_does_not_block_new_replies(env):
    with closing(SourceFixture(env)) as fixture, closing(SourcePipeline(
        env.app.state.store, env.settings.database, fixture.configs
    )) as pipeline:
        first = fixture.add()
        second = fixture.add()
        pipeline.capture_once()
        with pipeline.store.transaction() as db:
            db.execute("UPDATE source_routes SET floor=%s WHERE tenant='test' AND inbox='inbox1'", (first['id'],))
        job = pipeline.queue.claim()
        assert job['message_id'] == second['id']
        pipeline.bridges[('test', 'inbox1')].prepare(job)

        task = env.claim(fixture.devices[0]).json()['task']
        assert task['payload']['context']['chatwoot_message_id'] == second['id']
