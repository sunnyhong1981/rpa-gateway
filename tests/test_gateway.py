import concurrent.futures
import hashlib
import json
import pytest
from fastapi.testclient import TestClient
from rpa_gateway.app import Settings, create_app


def leased(env,images=0):
    d=env.register()
    media=[env.image(color=color)[0] for color in ['red','blue','green'][:images]]
    created=env.enqueue(d,media_ids=[m['id'] for m in media])
    assert created.status_code==200
    task=env.claim(d).json()['task']
    return d,task,media


def test_end_to_end_batch_original_bytes_exact_archive(env):
    d,task,media=leased(env,3)
    assert len(task['payload']['media_ids'])==3
    assert env.claim(d).json()['task'] is None
    for m in media:
        response=env.client.get(f"/v1/device/tasks/{task['id']}/media/{m['id']}",headers=d['headers'])
        assert response.status_code==200
        assert hashlib.sha256(response.content).hexdigest()==m['sha256']
    assert env.receipt(d,task,'submitting').json()['state']=='submitting'
    assert env.receipt(d,task,'submitted').json()['state']=='submitted'
    proof=env.evidence(d,task,media)
    assert env.confirm(d,task,proof).json()['state']=='confirmed'
    assert env.confirm(d,task,proof).json()['duplicate'] is True
    assert env.claim(d).json()['task'] is None
    assert env.inspect(d,task).json()['state']=='confirmed'


@pytest.mark.parametrize('flag',['worker_alive','desktop_unlocked','wecom_logged_in','auto_send'])
def test_unready_never_dispatches(env,flag):
    d=env.register()
    env.enqueue(d)
    env.heartbeat(d,**{flag:False})
    assert env.claim(d).status_code==409


def test_hello_is_not_heartbeat_and_stale_heartbeat_blocks(env):
    d=env.register(ready=False)
    env.enqueue(d)
    assert env.claim(d).status_code==409
    env.heartbeat(d)
    env.clock.advance(46)
    assert env.claim(d).status_code==409


@pytest.mark.parametrize('caps', [['text_v1'],['official_identity_v1'],['caption_images_v1']])
def test_required_capabilities_independent_of_heartbeat(env,caps):
    d=env.register(caps=caps)
    media,_=env.image()
    env.enqueue(d,media_ids=[media['id']])
    assert env.claim(d).json()['reason']=='capability_missing'


def test_one_physical_device_serializes_groups_even_concurrent_claims(env):
    d=env.register()
    for i in range(12):env.enqueue(d,room_id='room2' if i%2 else 'room1',inbox_id='inbox2' if i%2 else 'inbox1')
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        responses=list(pool.map(lambda _:env.claim(d),range(12)))
    assert all(r.status_code==200 for r in responses)
    assert sum(r.json()['task'] is not None for r in responses)==1


def test_idempotency_does_not_extend_deadline_or_replace_payload(env):
    d=env.register()
    a=env.enqueue(d,source_message_id='fixed')
    env.clock.advance(10)
    b=env.enqueue(d,source_message_id='fixed')
    assert a.json()==b.json()
    conflict=env.enqueue(d,source_message_id='fixed',caption='different')
    assert conflict.status_code==409


def test_50_waybills_distinct_one_source_and_duplicates_dedup(env):
    d=env.register()
    ids=[]
    for i in range(50):
        args={'source_message_id':'batch50','waybill':f'JSE_{i}'}
        first=env.enqueue(d,**args).json()
        duplicate=env.enqueue(d,**args).json()
        assert first['id']==duplicate['id']
        ids.append(first['id'])
    assert len(set(ids))==50


def test_pre_submit_lease_expiry_requeues_and_fences_old_worker(env):
    d,old,_=leased(env)
    env.clock.advance(31)
    env.heartbeat(d)
    new=env.claim(d).json()['task']
    assert new['id']==old['id'] and new['lease_token']!=old['lease_token']
    assert env.receipt(d,old,'submitting').status_code==409
    assert env.receipt(d,new,'submitting').status_code==200


@pytest.mark.parametrize('end_state',['submitting','submitted'])
def test_uncertain_submission_never_requeued_after_restart(env,end_state):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    if end_state=='submitted':env.receipt(d,task,'submitted')
    env.clock.advance(91)
    restarted=create_app(env.settings,env.clock)
    with TestClient(restarted) as client:
        response=client.get(f"/v1/admin/test/tasks/{task['id']}",headers=env.admin)
        assert response.json()['state']=='unknown'
        env.enqueue(d)
        env.heartbeat(d)
        claimed=client.post('/v1/device/tasks/claim',headers=d['headers'],json={'session_id':d['session_id']})
        assert claimed.json()['task'] is None


def test_receipt_replay_and_payload_conflict(env):
    d,task,_=leased(env)
    first=env.receipt(d,task,'submitting',event_id='same')
    assert first.status_code==200
    assert env.receipt(d,task,'submitting',event_id='same').json()['duplicate'] is True
    assert env.receipt(d,task,'submitted',event_id='same').status_code==409
    assert env.receipt(d,task,'failed_pre_submit').status_code==409


def test_submit_without_intent_forbidden(env):
    d,task,_=leased(env)
    assert env.receipt(d,task,'submitted').status_code==409
    assert env.receipt(d,task,'failed_pre_submit').json()['state']=='failed'


def test_deadline_not_extended_by_renewal(env):
    d=env.register()
    env.enqueue(d,ttl_seconds=15)
    task=env.claim(d).json()['task']
    env.clock.advance(10)
    assert env.receipt(d,task,'renew').status_code==200
    env.clock.advance(6)
    assert env.receipt(d,task,'submitting').status_code==409
    assert env.inspect(d,task).json()['state']=='expired'


@pytest.mark.parametrize('damage',['missing','size','md5','caption','room','actor','before','future','duplicate'])
def test_archive_must_match_all_originals_scope_caption_time(env,damage):
    d,task,media=leased(env,3)
    env.receipt(d,task,'submitting')
    proof=env.evidence(d,task,media)
    if damage=='missing':proof['events'].pop()
    if damage=='size':proof['events'][-1]['size']+=1
    if damage=='md5':proof['events'][-1]['md5']='0'*32
    if damage=='caption':proof['events'][0]['text']='different'
    if damage=='room':proof['events'][0]['room_id']='wrong'
    if damage=='actor':proof['events'][0]['actor_id']='wrong'
    if damage=='before':proof['events'][0]['occurred_at']-=10
    if damage=='future':proof['events'][0]['occurred_at']+=10
    if damage=='duplicate':proof['events'][-1]['event_id']=proof['events'][0]['event_id']
    assert env.confirm(d,task,proof).status_code==422
    assert env.inspect(d,task).json()['state']=='submitting'


def test_archive_events_cannot_be_reused_for_another_task(env):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    evidence=env.evidence(d,task)
    assert env.confirm(d,task,evidence).status_code==200
    env.enqueue(d)
    second=env.claim(d).json()['task']
    env.receipt(d,second,'submitting')
    assert env.confirm(d,second,evidence).status_code==409


def test_unknown_manual_close_is_audited_not_a_success_or_retry(env):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    env.receipt(d,task,'unknown')
    body={'action':'close_unknown_without_retry','reason':'人工核对后关闭，保持未确认，不自动重发'}
    response=env.client.post(f"/v1/admin/test/tasks/{task['id']}/review",headers=env.admin,json=body)
    assert response.json()=={'state':'closed_unconfirmed','automatically_retried':False}
    assert env.inspect(d,task).json()['events'][-1]['details']['reason']==body['reason']


def test_late_archive_can_confirm_unknown_without_resend(env):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    proof=env.evidence(d,task)
    env.clock.advance(91)
    assert env.inspect(d,task).json()['state']=='unknown'
    assert env.confirm(d,task,proof).json()['state']=='confirmed'


@pytest.mark.parametrize('action',['rotate','revoke'])
def test_credential_rotation_revocation_blocks_old_device(env,action):
    d,task,_=leased(env)
    response=env.client.post(f'/v1/admin/test/devices/desktop/{action}',headers=env.admin)
    assert response.status_code==200
    assert env.claim(d).status_code==401
    assert env.receipt(d,task,'submitting').status_code==401


def test_cross_device_and_tenant_access_denied(env):
    d,task,media=leased(env,1)
    other=env.register(device='other')
    alien=env.register(tenant='alien',device='alien')
    for peer in [other,alien]:
        assert env.receipt(peer,task,'submitting').status_code==404
        assert env.client.get(f"/v1/device/tasks/{task['id']}/media/{media[0]['id']}",headers=peer['headers']).status_code==404
    assert env.enqueue(alien,media_ids=[media[0]['id']]).status_code==422
    assert env.client.get(f"/v1/admin/alien/tasks/{task['id']}",headers=env.admin).status_code==404


def test_device_scope_and_media_validation(env):
    d=env.register()
    assert env.enqueue(d,room_id='foreign').status_code==403
    assert env.enqueue(d,inbox_id='foreign').status_code==403
    assert env.enqueue(d,media_ids=['missing']).status_code==422
    m,_=env.image()
    assert env.enqueue(d,media_ids=[m['id'],m['id']]).status_code==422
    assert env.enqueue(d,media_ids=['missing']*10).status_code==422


def test_jpeg_mislabeled_png_preserves_original_mime_and_bytes(env):
    d=env.register()
    meta,data=env.image(kind='JPEG')
    assert meta['mime']=='image/jpeg'
    env.enqueue(d,media_ids=[meta['id']])
    task=env.claim(d).json()['task']
    response=env.client.get(f"/v1/device/tasks/{task['id']}/media/{meta['id']}",headers=d['headers'])
    assert response.content==data and response.headers['content-type']=='image/jpeg'


@pytest.mark.parametrize('payload',[b'<html>error</html>',b'\x89PNG\r\n\x1a\n',b'not-an-image'])
def test_invalid_images_rejected(env,payload):
    assert env.client.post('/v1/backend/test/media',headers=env.backend,content=payload).status_code==415


@pytest.mark.parametrize('role',['admin','backend','verifier'])
def test_roles_separated_and_device_cannot_confirm_itself(env,role):
    d,task,_=leased(env)
    headers=getattr(env,role)
    if role!='admin':assert env.client.get('/v1/admin/test/status',headers=headers).status_code==401
    if role!='backend':assert env.client.post('/v1/backend/test/tasks',headers=headers,json={}).status_code==401
    if role!='verifier':assert env.client.post(f"/v1/verifier/test/tasks/{task['id']}/confirm",headers=headers,json={'events':[]}).status_code==401
    assert env.confirm(d,task,env.evidence(d,task)).status_code==409
    assert env.client.post(f"/v1/verifier/test/tasks/{task['id']}/confirm",headers=d['headers'],json=env.evidence(d,task)).status_code==401


def test_no_legacy_connections_or_credential_leak(env):
    assert env.client.get('/healthz').json()['legacy_connected'] is False
    assert env.client.get('/v1/admin/test/status').status_code==401
    d=env.register()
    status=env.client.get('/v1/admin/test/status',headers=env.admin)
    assert d['headers']['Authorization'][7:] not in status.text
    assert 'token_hash' not in status.text
    assert env.client.get('/openapi.json',headers=env.admin).status_code==200
    assert env.client.get('/openapi.json').status_code==401


def test_installation_takeover_blocked_during_active_work_and_protocol_rejected(env):
    d,task,_=leased(env)
    body={**d['hello'],'installation_id':'different'}
    assert env.client.post('/v1/device/hello',headers=d['headers'],json=body).status_code==409
    assert env.client.post('/v1/device/hello',headers=d['headers'],json={**body,'protocol':2}).status_code==422
    assert env.client.post('/v1/device/hello',headers=d['headers'],json=d['hello']).json()['session_id']==d['session_id']


def test_websocket_is_notification_only(env):
    d=env.register()
    with env.client.websocket_connect('/v1/device/notifications') as ws:
        ws.send_json({'token':d['headers']['Authorization'][7:],'session_id':d['session_id']})
        event=ws.receive_json()
        assert event['type']=='tasks_changed' and event['tasks']==[]


def test_active_resume_does_not_claim_or_resend(env):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    response=env.client.get('/v1/device/active',headers=d['headers'],params={'session_id':d['session_id']})
    assert response.json()['tasks'][0]['state']=='submitting'
    assert env.claim(d).json()['task'] is None


def test_persisted_db_contains_hash_not_device_credential(env):
    d=env.register()
    with env.app.state.store.transaction() as db:
        value=db.execute('SELECT token_hash FROM devices').fetchone()['token_hash']
    assert value!=d['headers']['Authorization'][7:]
    assert len(value)==64


def test_short_or_shared_control_credentials_rejected():
    with pytest.raises(ValueError):Settings('x','x','y','z')
    with pytest.raises(ValueError):Settings('x','a'*48,'a'*48,'v'*48)


def test_inbox_room_bindings_are_pairs_not_independent_allowlists(env):
    d=env.register()
    assert env.enqueue(d,inbox_id='inbox1',room_id='room2').status_code==403


def test_same_physical_device_id_cannot_be_registered_to_second_tenant(env):
    env.register()
    response=env.client.post('/v1/admin/alien/devices',headers=env.admin,json={
        'device_id':'desktop','bindings':[{'inbox_id':'inbox1','room_id':'room1'}]})
    assert response.status_code==409


def test_claim_contains_expected_hashes_before_download(env):
    d,task,media=leased(env,3)
    assert task['media']==media


def test_actual_streamed_body_limit_without_content_length(env):
    def chunks():
        for _ in range(17):yield b'x'*(1024*1024)
    response=env.client.post('/v1/backend/test/media',headers=env.backend,content=chunks())
    assert response.status_code==413


def test_bad_websocket_token_rejected(env):
    from starlette.websockets import WebSocketDisconnect
    with env.client.websocket_connect('/v1/device/notifications') as ws:
        ws.send_json({'token':'bad','session_id':'bad'})
        with pytest.raises(WebSocketDisconnect):ws.receive_json()


def test_media_cannot_download_before_claim_or_after_completion(env):
    d=env.register()
    meta,_=env.image()
    task=env.enqueue(d,media_ids=[meta['id']]).json()
    url=f"/v1/device/tasks/{task['id']}/media/{meta['id']}"
    assert env.client.get(url,headers=d['headers']).status_code==409
    claimed=env.claim(d).json()['task']
    assert env.client.get(url,headers=d['headers']).status_code==200
    env.receipt(d,claimed,'failed_pre_submit')
    assert env.client.get(url,headers=d['headers']).status_code==409


def test_nonfinite_archive_timestamp_rejected(env):
    d,task,_=leased(env)
    env.receipt(d,task,'submitting')
    proof=env.evidence(d,task)
    proof['events'][0]['occurred_at']=float('nan')
    response=env.client.post(f"/v1/verifier/test/tasks/{task['id']}/confirm",headers={**env.verifier,'Content-Type':'application/json'},content=json.dumps(proof))
    assert response.status_code==422


def test_boolean_is_not_a_protocol_version(env):
    d=env.register()
    assert env.client.post('/v1/device/hello',headers=d['headers'],json={**d['hello'],'protocol':True}).status_code==422
