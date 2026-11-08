"""Desktop execution remains exclusive while released submissions await archive."""
from concurrent.futures import ThreadPoolExecutor
import pytest
from rpa_gateway.database import Database


def release(env,d,task,event_id='desktop_release',**proof):
    values={'native_submit_complete':True,'input_cleared':True,'group_verified':True,**proof}
    return env.receipt(d,task,'desktop_released',event_id=event_id,**values)


def submitted(env,d):
    env.enqueue(d)
    task=env.claim(d).json()['task']
    assert env.receipt(d,task,'submitting').status_code==200
    assert env.receipt(d,task,'submitted').status_code==200
    return task


def test_old_clients_still_hold_device_until_archive(env):
    d=env.register();first=submitted(env,d);env.enqueue(d)
    assert env.claim(d).json()['task'] is None
    assert env.confirm(d,first,env.evidence(d,first)).status_code==200
    assert env.claim(d).json()['task'] is not None


@pytest.mark.parametrize('flag',['native_submit_complete','input_cleared','group_verified'])
def test_release_requires_every_native_proof(env,flag):
    d=env.register();first=submitted(env,d);env.enqueue(d)
    assert release(env,d,first,**{flag:False}).status_code==409
    assert env.claim(d).json()['task'] is None
    assert env.inspect(d,first).json()['desktop_released_at'] is None


def test_cannot_release_intent_or_lease_as_completed(env):
    d=env.register();env.enqueue(d);task=env.claim(d).json()['task']
    assert release(env,d,task).status_code==409
    env.receipt(d,task,'submitting')
    assert release(env,d,task).status_code==409


def test_release_unlocks_one_next_task_without_marking_delivery(env):
    d=env.register();first=submitted(env,d)
    for _ in range(12):env.enqueue(d)
    assert release(env,d,first).json()['state']=='submitted'
    assert release(env,d,first).json()['duplicate'] is True
    assert release(env,d,first,event_id='another_release').status_code==409
    with ThreadPoolExecutor(max_workers=12) as pool:
        results=list(pool.map(lambda _:env.claim(d),range(12)))
    assert sum(r.json()['task'] is not None for r in results)==1
    assert env.inspect(d,first).json()['state']=='submitted'
    assert env.inspect(d,first).json()['desktop_released_at'] is not None
    assert env.receipt(d,first,'submitting').status_code==409
    assert env.confirm(d,first,env.evidence(d,first)).json()['state']=='confirmed'


def test_archive_backlog_is_bounded_and_confirmation_resumes_claim(env):
    d=env.register();tasks=[]
    for i in range(3):
        task=submitted(env,d);assert release(env,d,task,event_id='release'+str(i)).status_code==200;tasks.append(task)
    env.enqueue(d)
    assert env.claim(d).json()=={'task':None,'reason':'archive_confirmation_backlog'}
    db=Database(env.settings.database)
    try:db.initialize()  # Startup must not recreate the obsolete all-active unique index.
    finally:db.close()
    assert env.confirm(d,tasks[0],env.evidence(d,tasks[0],prefix='first')).status_code==200
    assert env.claim(d).json()['task'] is not None


def test_late_unknown_blocks_new_enter_even_with_an_already_leased_task(env):
    d=env.register();first=submitted(env,d);release(env,d,first)
    env.clock.advance(80);env.heartbeat(d);env.enqueue(d);second=env.claim(d).json()['task']
    env.clock.advance(11);env.heartbeat(d)
    assert env.receipt(d,second,'submitting').status_code==409
    assert env.inspect(d,first).json()['state']=='unknown'
    assert env.claim(d).json()['task'] is None
    assert env.confirm(d,first,env.evidence(d,first)).status_code==200
    assert env.receipt(d,second,'submitting').status_code==200
