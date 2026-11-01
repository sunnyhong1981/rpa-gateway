import pytest


def modern(env):
    d=env.register(ready=False)
    env.clock.advance(46)
    d['hello']['worker_instance_id']='runtime_A'
    result=env.client.post('/v1/device/hello',headers=d['headers'],json=d['hello'])
    assert result.status_code==200,result.text
    d['session_id']=result.json()['session_id']
    env.heartbeat(d)
    return d


def takeover(env,d,worker='runtime_B'):
    return env.client.post('/v1/device/hello',headers=d['headers'],json={**d['hello'],'worker_instance_id':worker})


def unknown(env,d):
    env.enqueue(d)
    t=env.claim(d).json()['task']
    assert env.receipt(d,t,'submitting').status_code==200
    env.clock.advance(91)
    assert env.inspect(d,t).json()['state']=='unknown'
    env.heartbeat(d)
    return t


def release(env,d,t,**flags):
    return env.receipt(d,t,'quarantine_released',event_id='release_a',desktop_idle_verified=True,
                       input_cleared=flags.get('input_cleared',True),group_verified=flags.get('group_verified',True))


def test_live_worker_owner_rejects_duplicate_same_installation(env):
    d=modern(env)
    assert takeover(env,d).status_code==409
    assert env.client.post('/v1/device/hello',headers=d['headers'],json=d['hello']).json()['session_id']==d['session_id']
    assert env.heartbeat(d).status_code==200


def test_hello_itself_reserves_startup_window_without_heartbeat(env):
    d=modern(env)
    env.clock.advance(46)
    new=takeover(env,d)
    assert new.status_code==200
    assert takeover(env,d,worker='runtime_C').status_code==409


def test_stale_owner_can_transfer_but_old_heartbeats_and_enter_are_fenced(env):
    d=modern(env)
    env.enqueue(d);old=env.claim(d).json()['task']
    env.clock.advance(46)
    r=takeover(env,d);assert r.status_code==200
    assert r.json()['session_id']!=d['session_id']
    assert env.heartbeat(d).status_code==409
    assert env.claim(d).status_code==409
    assert env.receipt(d,old,'submitting').status_code==409
    new={**d,'session_id':r.json()['session_id']}
    assert env.heartbeat(new).status_code==200
    reclaimed=env.claim(new).json()['task']
    assert reclaimed['id']==old['id'] and reclaimed['lease_token']!=old['lease_token']


def test_login_unknown_does_not_release_live_owner(env):
    d=modern(env)
    env.heartbeat(d,wecom_logged_in=False,desktop_unlocked=False)
    assert takeover(env,d).status_code==409
    assert env.claim(d).status_code==409


def test_legacy_worker_cannot_join_live_modern_session(env):
    d=modern(env)
    body={k:v for k,v in d['hello'].items() if k!='worker_instance_id'}
    assert env.client.post('/v1/device/hello',headers=d['headers'],json=body).status_code==409


def test_unknown_requires_delay_then_idle_proof_retains_audit_and_allows_new_task(env):
    d=modern(env);t=unknown(env,d)
    assert release(env,d,t).status_code==409
    env.clock.advance(300);env.heartbeat(d)
    assert release(env,d,t,input_cleared=False).status_code==409
    assert release(env,d,t,group_verified=False).status_code==409
    assert env.claim(d).json()['task'] is None
    result=release(env,d,t);assert result.status_code==200,result.text
    assert result.json()['state']=='unknown'
    assert release(env,d,t).json()['duplicate'] is True
    audit=env.inspect(d,t).json()
    assert audit['state']=='unknown' and audit['desktop_released_at'] is not None
    assert audit['events'][-1]['details']['state']=='quarantine_released'
    env.enqueue(d)
    next_task=env.claim(d).json()['task'];assert next_task['id']!=t['id']
    assert env.receipt(d,t,'submitting').status_code==409
    assert env.receipt(d,next_task,'submitting').status_code==200


def test_new_owner_releases_old_quarantine_but_cannot_report_old_submit(env):
    d=modern(env);t=unknown(env,d)
    env.clock.advance(300)
    r=takeover(env,d);assert r.status_code==200
    newer={**d,'session_id':r.json()['session_id']};env.heartbeat(newer)
    assert env.receipt(newer,t,'submitted').status_code==409
    assert release(env,newer,t).status_code==200


def test_quarantine_release_requires_ready_health_and_original_lease(env):
    d=modern(env);t=unknown(env,d);env.clock.advance(300)
    env.heartbeat(d,wecom_logged_in=False)
    assert release(env,d,t).status_code==409
    env.heartbeat(d)
    assert release(env,d,{**t,'lease_token':'wrong'*8}).status_code==409
    assert release(env,d,t).status_code==200


def test_confirmed_and_queued_tasks_cannot_fake_quarantine_release(env):
    d=modern(env);env.enqueue(d);t=env.claim(d).json()['task']
    assert release(env,d,t).status_code==409


def test_late_real_evidence_can_confirm_released_unknown_without_resend(env):
    d=modern(env);env.enqueue(d);t=env.claim(d).json()['task']
    env.receipt(d,t,'submitting');proof=env.evidence(d,t)
    env.clock.advance(391);env.heartbeat(d)
    assert env.inspect(d,t).json()['state']=='unknown'
    assert release(env,d,t).status_code==200
    assert env.confirm(d,t,proof).json()['state']=='confirmed'


def test_normal_desktop_release_then_unknown_still_needs_fresh_quarantine(env):
    d=modern(env);env.enqueue(d);t=env.claim(d).json()['task']
    env.receipt(d,t,'submitting');env.receipt(d,t,'submitted')
    result=env.receipt(d,t,'desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True)
    assert result.status_code==200
    env.clock.advance(391);env.heartbeat(d)
    assert env.inspect(d,t).json()['state']=='unknown'
    env.enqueue(d)
    assert env.claim(d).json()['task'] is None
    assert release(env,d,t).status_code==200
    next_task=env.claim(d).json()['task']
    assert next_task['id']!=t['id']
    assert env.receipt(d,next_task,'submitting').status_code==200


def test_release_audit_is_visible_after_ack_loss_and_owner_restart(env):
    d=modern(env);t=unknown(env,d);env.clock.advance(300);env.heartbeat(d)
    before=env.client.get('/v1/device/tasks/'+t['id'],headers=d['headers']).json()
    assert before['quarantine_released_at'] is None
    assert release(env,d,t).status_code==200
    # Ignore the release response, transfer ownership, then recover from the
    # durable audit marker without a second release or another submission.
    env.clock.advance(46)
    next_owner=takeover(env,d)
    assert next_owner.status_code==200
    snapshot=env.client.get('/v1/device/tasks/'+t['id'],headers=d['headers']).json()
    assert snapshot['state']=='unknown' and snapshot['quarantine_released_at'] is not None
    audit=env.inspect(d,t).json()
    assert sum(e['details'].get('state')=='quarantine_released' for e in audit['events'])==1
    assert sum(e['details'].get('state')=='submitting' for e in audit['events'])==1


def test_regular_release_has_no_quarantine_audit_marker(env):
    d=modern(env);env.enqueue(d);t=env.claim(d).json()['task']
    env.receipt(d,t,'submitting');env.receipt(d,t,'submitted')
    assert env.receipt(d,t,'desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True).status_code==200
    snapshot=env.client.get('/v1/device/tasks/'+t['id'],headers=d['headers']).json()
    assert snapshot['desktop_released_at'] is not None
    assert snapshot['quarantine_released_at'] is None


def test_upgrade_stale_legacy_owner_with_unknown_preserves_no_resend(env):
    d=env.register();env.enqueue(d);t=env.claim(d).json()['task']
    env.receipt(d,t,'submitting');env.clock.advance(391)
    assert env.inspect(d,t).json()['state']=='unknown'
    r=env.client.post('/v1/device/hello',headers=d['headers'],json={**d['hello'],'worker_instance_id':'upgraded_runtime'})
    assert r.status_code==200
    newer={**d,'session_id':r.json()['session_id']};env.heartbeat(newer)
    assert env.heartbeat(d).status_code==409
    assert env.claim(newer).json()['task'] is None
    assert release(env,newer,t).status_code==200
    assert env.inspect(newer,t).json()['state']=='unknown'


def test_later_identical_reply_cannot_confirm_quarantined_unknown(env):
    d=modern(env);env.enqueue(d);t=env.claim(d).json()['task']
    env.receipt(d,t,'submitting');env.clock.advance(391);env.heartbeat(d)
    assert env.inspect(d,t).json()['state']=='unknown'
    assert release(env,d,t).status_code==200
    env.clock.advance(1)
    proof=env.evidence(d,t)
    assert env.confirm(d,t,proof).status_code==422
    assert env.inspect(d,t).json()['state']=='unknown'
