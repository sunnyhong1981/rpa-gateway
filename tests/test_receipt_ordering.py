"""A faster official archive may confirm before the native submitted HTTP receipt."""
import pytest


def test_official_confirmation_before_submitted_preserves_late_native_audit(env):
    device=env.register();env.enqueue(device);task=env.claim(device).json()['task']
    assert env.receipt(device,task,'submitting').status_code==200
    assert env.confirm(device,task,env.evidence(device,task)).status_code==200
    confirmed_at=env.inspect(device,task).json()['updated']
    env.clock.advance(2)
    response=env.receipt(device,task,'submitted',event_id='native-submitted-once')
    assert response.status_code==200 and response.json()['state']=='confirmed'
    assert env.receipt(device,task,'submitted',event_id='native-submitted-once').json()['duplicate']
    assert env.receipt(device,task,'desktop_released',event_id='release-once',
                       native_submit_complete=True,input_cleared=True,group_verified=True).json()['state']=='confirmed'
    with env.app.state.store.transaction() as db:
        row=db.execute('SELECT state,submitted_at,desktop_released_at FROM tasks WHERE id=%s',(task['id'],)).fetchone()
    assert row=={'state':'confirmed','submitted_at':confirmed_at+2,'desktop_released_at':confirmed_at+2}
    events=env.inspect(device,task).json()['events']
    assert sum(e['details'].get('state')=='submitted' for e in events)==1
    assert env.receipt(device,task,'submitting').status_code==409


def test_new_submitted_event_cannot_overwrite_an_existing_submission_time(env):
    device=env.register();env.enqueue(device);task=env.claim(device).json()['task']
    env.receipt(device,task,'submitting');env.receipt(device,task,'submitted')
    env.confirm(device,task,env.evidence(device,task))
    env.clock.advance(5)
    assert env.receipt(device,task,'submitted',event_id='different-submit-event').status_code==409


def test_late_receipt_never_changes_confirmation_to_unknown_or_authorizes_a_new_enter(env):
    device=env.register();env.enqueue(device);task=env.claim(device).json()['task']
    env.receipt(device,task,'submitting');env.confirm(device,task,env.evidence(device,task))
    assert env.receipt(device,task,'desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True).status_code==409
    assert env.receipt(device,task,'unknown').status_code==409
    assert env.inspect(device,task).json()['state']=='confirmed'
