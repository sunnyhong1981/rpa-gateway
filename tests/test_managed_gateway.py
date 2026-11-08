import hashlib
import uuid
from unittest.mock import Mock
import pytest


@pytest.fixture
def managed(env):
    token='managed-test-token-'+'m'*48
    body=dict(device_id='managed-test',token_hash=hashlib.sha256(token.encode()).hexdigest(),installation_id=str(uuid.uuid4()),
              bindings=[dict(inbox_id='1',room_id='room1',floor=0),dict(inbox_id='2',room_id='room2',floor=0)])
    response=env.client.post('/v1/admin/test/managed-devices/ensure',headers=env.admin,json=body)
    assert response.status_code==200,response.text
    headers={'Authorization':'Bearer '+token}
    hello=dict(protocol=1,installation_id=body['installation_id'],runtime_version='managed-test',worker_instance_id='owner-test',
               capabilities=['text_v1','caption_images_v1','official_identity_v1','managed_delivery_v1'])
    response=env.client.post('/v1/device/hello',headers=headers,json=hello);assert response.status_code==200,response.text
    d=dict(tenant='test',device=body['device_id'],headers=headers,session_id=response.json()['session_id'],hello=hello)
    assert env.heartbeat(d).status_code==200
    backend=Mock();backend.verify.return_value=True;env.app.state.store.managed_backend=backend
    return d,body,backend


def task(env,d,message_id,inbox='1'):
    body=dict(device_id=d['device'],inbox_id=inbox,room_id='room'+inbox,source_message_id=str(message_id),waybill='message-'+str(message_id),
              recipient_id='group',robot_id='robot',caption='QA '+str(message_id),media_ids=[],ttl_seconds=3600,
              context=dict(provider='chatwoot_managed',chatwoot_message_id=message_id,source_fingerprint='a'*64,route_generation=1,native_messages=[]))
    response=env.client.post('/v1/backend/test/tasks',headers=env.backend,json=body)
    assert response.status_code==200,response.text
    return response.json(),body


def claim(env,d,inboxes):
    return env.client.post('/v1/device/tasks/claim',headers=d['headers'],json=dict(session_id=d['session_id'],inbox_ids=inboxes))


def test_enrollment_is_idempotent_and_installation_is_pinned(env,managed):
    d,body,_=managed
    repeated=env.client.post('/v1/admin/test/managed-devices/ensure',headers=env.admin,json=body)
    assert repeated.status_code==200 and repeated.json()['changed']is False
    same=env.client.post('/v1/device/hello',headers=d['headers'],json=d['hello'])
    assert same.json()['session_id']==d['session_id']
    assert len(same.json()['bindings'])==2
    wrong=env.client.post('/v1/device/hello',headers=d['headers'],json=dict(d['hello'],installation_id=str(uuid.uuid4())))
    assert wrong.status_code==409


def test_one_device_routes_claims_and_preserves_message_order(env,managed):
    d,_,_=managed
    task(env,d,10);task(env,d,9);last,_=task(env,d,11,'2')
    first=claim(env,d,['2','1']);assert first.status_code==200;leased=first.json()['task'];assert leased['id']==last['id']
    assert claim(env,d,['1','2']).json()['task']is None
    assert env.receipt(d,leased,'failed_pre_submit').status_code==200
    leased=claim(env,d,['1']).json()['task'];assert leased['payload']['source_message_id']=='9'
    assert env.receipt(d,leased,'failed_pre_submit').status_code==200
    leased=claim(env,d,['1']).json()['task'];assert leased['payload']['source_message_id']=='10'


def test_source_is_rechecked_at_final_fence_and_creation_retries_are_idempotent(env,managed):
    d,_,backend=managed
    original,body=task(env,d,12)
    backend.verify.return_value=False
    repeated=env.client.post('/v1/backend/test/tasks',headers=env.backend,json=body)
    assert repeated.status_code==200 and repeated.json()['id']==original['id']
    leased=claim(env,d,['1']).json()['task']
    response=env.receipt(d,leased,'submitting');assert response.status_code==409
    assert env.inspect(d,leased).json()['state']=='leased'


def test_delayed_old_revocation_cannot_revoke_rotated_credentials(env,managed):
    d,body,_=managed
    newer=dict(body,token_hash=hashlib.sha256(b'new-managed-token').hexdigest())
    assert env.client.post('/v1/admin/test/managed-devices/ensure',headers=env.admin,json=newer).status_code==200
    response=env.client.post('/v1/admin/test/managed-devices/managed-test/revoke',headers=env.admin,json={'token_hash':body['token_hash']})
    assert response.status_code==200 and response.json()['changed']is False
    assert env.client.get('/v1/device/sync',headers=d['headers'],params={'session_id':d['session_id']}).status_code==401


def test_official_confirmation_can_precede_submitted_ack_without_blocking_device(env,managed):
    d,_,backend=managed
    task(env,d,15)
    leased=claim(env,d,['1']).json()['task']
    assert env.receipt(d,leased,'submitting').status_code==200
    backend.request.return_value=dict(verified=True,source_fingerprint='a'*64,evidence=dict(event_ids=['official-managed-15'],room_id='room1',sender_id='robot',source_fingerprint='a'*64))
    assert env.app.state.store.reconcile_managed_once()
    assert env.inspect(d,leased).json()['state']=='confirmed'
    assert env.receipt(d,leased,'submitted').status_code==200
    response=env.client.post('/v1/device/tasks/'+leased['id']+'/receipts',headers=d['headers'],json=dict(session_id=d['session_id'],lease_token=leased['lease_token'],event_id='managed-release',state='desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True))
    assert response.status_code==200,response.text
    with env.app.state.store.transaction()as db:
        assert db.execute('SELECT count(*) n FROM archive_claims').fetchone()['n']==1


def test_empty_enrollment_and_claim_scope_are_explicit(env,managed):
    d,body,_=managed
    response=claim(env,d,['outside'])
    assert response.status_code==403
    assert claim(env,d,[]).json()['task']is None
    response=env.client.post('/v1/admin/test/managed-devices/ensure',headers=env.admin,json=dict(body,bindings=[]))
    assert response.status_code==200
    assert response.json()['bindings']==[]
