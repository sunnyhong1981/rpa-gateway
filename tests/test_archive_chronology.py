"""Official archive sequence is ingestion order, not native text/image occurrence order."""
import json
from unittest.mock import MagicMock, Mock
import pytest
from rpa_gateway.source_bridge import SourceBridge


def build(env, *, count=3, captions=('测试运单A',), repeated=False):
    store=env.app.state.store;device=env.register()
    media=[env.image(color=color)[0] for color in ('red','blue','green')[:count]]
    if repeated:media=[media[0],media[0]]
    tasks=[]
    for index,caption in enumerate(captions):
        env.enqueue(device,caption=caption,media_ids=[m['id'] for m in media],
                    context={'provider':'chatwoot_official','reply_target':{'sender_name':'洪波'}})
        task=env.claim(device).json()['task'];tasks.append(task)
        env.receipt(device,task,'submitting');env.receipt(device,task,'submitted')
        env.receipt(device,task,'desktop_released',native_submit_complete=True,input_cleared=True,group_verified=True)
        with store.transaction() as db:
            task['intent_at']=db.execute('SELECT intent_at FROM tasks WHERE id=%s',(task['id'],)).fetchone()['intent_at']
            db.execute("INSERT INTO source_jobs(tenant,inbox,message_id,task,state) VALUES('test','inbox1',%s,%s,'queued')",(100+index,task['id']))
        env.clock.advance(10)
    config={'tenant':'test','inbox_id':'inbox1','base_url':'https://test','inbox_identifier':'x',
            'contact_source':'s','public_conversation_id':1,'hmac_token':'h','delivery_client_id':'c',
            'group_name':'test','archive_sender_id':'robot_test','archive_group_id':'room1'}
    bridge=SourceBridge(store,'unused',config);bridge.http=MagicMock();bridge.http.receipt.return_value={'status':'delivered'}
    return device,media,tasks,bridge


def caption(task,seq,offset=1):
    return {'seq':seq,'msgid':'text-'+task['id'],'room_id':'room1','sender_id':'robot_test',
            'occurred_at':task['intent_at']+offset,
            'payload':{'msgtype':'text','text':{'content':'@洪波 '+task['payload']['caption']}}}


def photo(task,medium,seq,index=1,offset=2):
    return {'seq':seq,'msgid':'photo-'+task['id']+'-'+str(index),'room_id':'room1','sender_id':'robot_test',
            'occurred_at':task['intent_at']+offset,
            'payload':{'msgtype':'image','image':{'md5sum':medium['md5'],'filesize':medium['size']}}}


def status(env,d,task):return env.inspect(d,task).json()['state']


def test_first_image_ingested_before_caption_but_occurred_after_it(env):
    d,media,(task,),bridge=build(env)
    events=[photo(task,media[0],3134,offset=1.63),caption(task,3135,offset=1),
            photo(task,media[1],3136,index=2,offset=2.2),photo(task,media[2],3137,index=3,offset=2.8)]
    bridge.read=Mock(return_value=events)
    bridge.reconcile()
    assert status(env,d,task)=='confirmed'
    with env.app.state.store.transaction() as db:
        assert db.execute('SELECT count(*) n FROM archive_claims WHERE task=%s',(task['id'],)).fetchone()['n']==4


def test_late_ingested_caption_after_all_originals_can_confirm(env):
    d,media,(task,),bridge=build(env)
    events=[photo(task,m,i,index=i,offset=1+i/10) for i,m in enumerate(media,1)]+[caption(task,8)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert status(env,d,task)=='confirmed'


def test_unknown_task_reconciles_existing_official_proof_without_new_submit(env):
    d,media,(task,),bridge=build(env)
    env.receipt(d,task,'unknown')
    events=[photo(task,media[0],1),caption(task,2),photo(task,media[1],3,index=2),photo(task,media[2],4,index=3)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert status(env,d,task)=='confirmed'
    states=[x['state'] for x in env.inspect(d,task).json()['events']]
    assert states.count('submitting')==1 and states.count('submitted')==2  # submitted + desktop release
    assert states[-1]=='confirmed'


def test_reordered_batches_with_shared_originals_use_disjoint_evidence(env):
    d,media,tasks,bridge=build(env,captions=('第一票','第二票'))
    first,second=tasks
    events=[caption(first,1),photo(second,media[0],2),caption(second,3),
            photo(first,media[0],4),photo(first,media[1],5,index=2),photo(first,media[2],6,index=3),
            photo(second,media[1],7,index=2),photo(second,media[2],8,index=3)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert all(status(env,d,t)=='confirmed' for t in tasks)
    with env.app.state.store.transaction() as db:
        claims={r['event_id']:r['task'] for r in db.execute('SELECT event_id,task FROM archive_claims')}
    assert len(claims)==8
    for t in tasks:
        assert claims['text-'+t['id']]==t['id']
        for i in (1,2,3):assert claims['photo-'+t['id']+'-'+str(i)]==t['id']


def test_does_not_borrow_next_batch_original_after_sorting(env):
    d,media,tasks,bridge=build(env,captions=('相同回复','相同回复'))
    first,second=tasks
    events=[caption(first,1),photo(first,media[0],2),photo(first,media[1],3,index=2),
            photo(second,media[0],4),caption(second,5),photo(second,media[1],6,index=2),photo(second,media[2],7,index=3)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert all(status(env,d,t)=='submitted' for t in tasks)
    with env.app.state.store.transaction() as db:assert db.execute('SELECT count(*) n FROM archive_claims').fetchone()['n']==0


def test_same_timestamp_uses_sequence_as_stable_boundary_not_digest_search(env):
    d,media,(task,),bridge=build(env)
    events=[photo(task,media[0],1,offset=1),caption(task,2,offset=1),
            photo(task,media[1],3,index=2,offset=1),photo(task,media[2],4,index=3,offset=1)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert status(env,d,task)=='submitted'


@pytest.mark.parametrize('change',['md5','size','caption','room','sender','time'])
def test_reordering_does_not_weaken_exact_proof(env,change):
    from fastapi import HTTPException
    d,media,(task,),bridge=build(env)
    events=[photo(task,media[0],1),caption(task,2),photo(task,media[1],3,index=2),photo(task,media[2],4,index=3)]
    if change=='md5':events[0]['payload']['image']['md5sum']='0'*32
    elif change=='size':events[0]['payload']['image']['filesize']+=1
    elif change=='caption':events[1]['payload']['text']['content']='@其他人 '+task['payload']['caption']
    elif change=='room':events[0]['room_id']='wrong-room'
    elif change=='sender':events[0]['sender_id']='wrong-actor'
    elif change=='time':events[0]['occurred_at']=task['intent_at']-3
    bridge.read=Mock(return_value=events)
    try:bridge.reconcile()
    except HTTPException as exc:assert exc.status_code==422
    assert status(env,d,task)=='submitted'
    with env.app.state.store.transaction() as db:assert db.execute('SELECT count(*) n FROM archive_claims').fetchone()['n']==0


def test_already_claimed_original_cannot_be_reused_by_reordering(env):
    d,media,tasks,bridge=build(env,captions=('票A','票B'));first,second=tasks
    events=[photo(first,media[0],1),caption(first,2),photo(first,media[1],3,index=2),photo(first,media[2],4,index=3)]
    bridge.read=Mock(return_value=events);bridge.reconcile()
    assert status(env,d,first)=='confirmed' and status(env,d,second)=='submitted'
    bridge.reconcile()
    assert status(env,d,second)=='submitted'
