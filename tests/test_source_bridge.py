import json
from datetime import datetime,timezone
from unittest.mock import Mock
from rpa_gateway.source_bridge import SourceBridge
from rpa_gateway.source_policy import eligible_source,eligible_reply
from rpa_gateway.models import TaskCreate


def test_source_policy_excludes_files_private_notifications_and_unscoped_replies():
    base={'message_type':1,'private':False,'attributes':{'in_reply_to':10,'rpa_recipient_id':'member'},'attachments':[]}
    assert eligible_source(base)
    for attrs in [{'rpa_group_push_id':1},{'rpa_notification_recipient':{'sender_id':'staff'}}]:
        assert not eligible_source({**base,'attributes':{**base['attributes'],**attrs}})
    assert not eligible_source({**base,'private':True})
    assert not eligible_source({**base,'attachments':[{'file_type':'file'}]})
    assert not eligible_reply({'reply_target':{'sender_id':'m','source_message_id':1,'require_native_mention':True}})
    assert eligible_reply({'reply_target':{'sender_id':'m','source_message_id':1}})


def test_route_source_historical_floor_never_replays(env):
    store=env.app.state.store;d=env.register()
    with store.transaction() as db:
        db.execute("INSERT INTO source_routes VALUES(%s,%s,%s,'shadow',100,100,1,%s,NULL)",('test','inbox1','desktop',env.clock()))
    bridge=SourceBridge(store,'unused',{'tenant':'test','inbox_id':'inbox1','base_url':'https://test','inbox_identifier':'x',
        'contact_source':'source','public_conversation_id':1,'hmac_token':'h','delivery_client_id':'client','group_name':'test','archive_sender_id':'robot_test'})
    bridge.read=Mock(return_value=[{'id':101,'message_type':0,'private':False,'attributes':{},'attachments':[]}])
    bridge.capture()
    assert bridge.read.call_args.args[1]==(100,)
    with store.transaction() as db:
        assert db.execute('SELECT cursor FROM source_routes').fetchone()['cursor']==101
        assert db.execute('SELECT count(*) n FROM source_jobs').fetchone()['n']==0
    bridge.materialize()
    assert bridge.read.call_count==1


def test_official_caption_prefix_allowed_only_when_backend_context_authorizes(env):
    d=env.register();caption='运单号：JSE_TEST'
    response=env.enqueue(d,caption=caption,context={'provider':'chatwoot_official','reply_target':{'sender_name':'Sunny Hong'}})
    task=env.claim(d).json()['task'];env.receipt(d,task,'submitting')
    proof=env.evidence(d,task)
    proof['events'][0]['text']='@Sunny Hong '+caption
    assert env.confirm(d,task,proof).status_code==200


def test_wrong_mention_prefix_cannot_confirm_official_source(env):
    d=env.register()
    env.enqueue(d,context={'provider':'chatwoot_official','reply_target':{'sender_name':'Sunny Hong'}})
    task=env.claim(d).json()['task'];env.receipt(d,task,'submitting')
    proof=env.evidence(d,task);proof['events'][0]['text']='@Other '+task['payload']['caption']
    assert env.confirm(d,task,proof).status_code==422


def test_scoped_source_materializes_then_confirms_from_real_ledger(env):
    store=env.app.state.store;d=env.register()
    config={'tenant':'test','inbox_id':'inbox1','base_url':'https://test','inbox_identifier':'x','contact_source':'s',
        'public_conversation_id':1,'hmac_token':'h','delivery_client_id':'c','group_name':'test','archive_sender_id':'robot_test','archive_group_id':'room1'}
    with store.transaction() as db:
        db.execute("INSERT INTO source_routes VALUES('test','inbox1','desktop','gateway',100,100,1,%s,%s)",(env.clock(),env.clock()))
        db.execute("INSERT INTO source_jobs(tenant,inbox,message_id) VALUES('test','inbox1',101)")
    bridge=SourceBridge(store,'unused',config)
    message={'id':101,'message_type':1,'private':False,'attributes':{'in_reply_to':90,'rpa_recipient_id':'official_member'},
        'attachments':[],'content':'运单号：JSE_TEST 测试','created_at':datetime.fromtimestamp(env.clock()-1,timezone.utc)}
    source={'id':90,'content':'查 JSE_TEST','attributes':{'rpa_sender_id':'official_member','rpa_archive_room_id':'room1'}}
    archive={'msgid':'official-event','room_id':'room1','sender_id':'robot_test','occurred_at':env.clock(),
        'payload':{'msgtype':'text','text':{'content':'@Sunny Hong '+message['content']}}}
    bridge.read=Mock(side_effect=lambda query,params: [archive] if 'test_archive' in query else [message if params[0]==101 else source])
    bridge.http=Mock()
    bridge.http.identity.return_value={'sender_id':'official_member','sender_name':'Sunny Hong','room_id':'room1','origin':'official_directory','verification_version':2}
    bridge.http.receipt.return_value={'ok':True,'status':'delivered'}
    bridge.materialize()
    store.source_validator=lambda payload:None  # This fixture supplies a fake scoped Chatwoot view.
    task=env.claim(d).json()['task'];assert task['payload']['context']['chatwoot_message_id']==101
    env.receipt(d,task,'submitting');env.receipt(d,task,'submitted')
    bridge.reconcile()
    assert env.inspect(d,task).json()['state']=='confirmed'
    with store.transaction() as db:assert db.execute('SELECT reported_state FROM source_jobs').fetchone()['reported_state']=='delivered'


def test_pipeline_archive_does_not_borrow_photos_from_next_caption(env):
    store=env.app.state.store;d=env.register();media,_=env.image()
    config={'tenant':'test','inbox_id':'inbox1','base_url':'https://test','inbox_identifier':'x','contact_source':'s',
        'public_conversation_id':1,'hmac_token':'h','delivery_client_id':'c','group_name':'test','archive_sender_id':'robot_test','archive_group_id':'room1'}
    tasks=[]
    for i in range(2):
        env.enqueue(d,caption='相同单号的相同回复',media_ids=[media['id']],context={'reply_target':{'sender_name':'Sunny'}})
        task=env.claim(d).json()['task'];tasks.append(task)
        env.receipt(d,task,'submitting');env.receipt(d,task,'submitted')
        env.receipt(d,task,'desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True)
        with store.transaction() as db:db.execute("INSERT INTO source_jobs(tenant,inbox,message_id,task,state) VALUES('test','inbox1',%s,%s,'queued')",(101+i,task['id']))
    def event(identity,kind):
        payload={'msgtype':kind}
        if kind=='text':payload['text']={'content':'相同单号的相同回复'}
        else:payload['image']={'md5sum':media['md5'],'filesize':media['size']}
        return {'msgid':identity,'room_id':'room1','sender_id':'robot_test','occurred_at':env.clock(),'payload':payload}
    bridge=SourceBridge(store,'unused',config);bridge.http=Mock();bridge.http.receipt.return_value={'status':'delivered'}
    events=[event('caption1','text'),event('caption2','text'),event('photo2','image')]
    bridge.read=Mock(side_effect=lambda *args:events)
    bridge.reconcile()
    assert all(env.inspect(d,t).json()['state']=='submitted' for t in tasks)
    # Once each original native batch is complete, each consumes only its own evidence.
    events.insert(1,event('photo1','image'))
    bridge.reconcile()
    assert all(env.inspect(d,t).json()['state']=='confirmed' for t in tasks)
    with store.transaction() as db:
        claims={r['event_id']:r['task'] for r in db.execute('SELECT event_id,task FROM archive_claims')}
    assert claims=={'caption1':tasks[0]['id'],'photo1':tasks[0]['id'],'caption2':tasks[1]['id'],'photo2':tasks[1]['id']}
