"""Synthetic restart test. Device token in private temporary state is removed after verify."""
import argparse
import json
import os
import time
import uuid
from pathlib import Path
from urllib.request import Request,urlopen
from rpa_gateway.client import GatewayClient


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('phase',choices=['prepare','verify'])
    parser.add_argument('--state',default='/data/restart-probe.json')
    args=parser.parse_args()
    roles={r:Path('/run/secrets/'+r+'_token').read_text().strip() for r in ('admin','backend','verifier')}
    def call(role,path,body=None):
        req=Request('http://127.0.0.1:8080'+path,data=json.dumps(body).encode() if body is not None else None,
                    headers={'Authorization':'Bearer '+roles[role],'Content-Type':'application/json'})
        with urlopen(req,timeout=10) as response:return json.load(response)
    path=Path(args.state)
    if args.phase=='prepare':
        if path.exists():raise RuntimeError('Existing probe state: refuse to replace')
        tenant='restart_'+uuid.uuid4().hex[:12]
        device='restart_sim_'+uuid.uuid4().hex
        registered=call('admin',f'/v1/admin/{tenant}/devices',{'device_id':device,
            'bindings':[{'inbox_id':'test','room_id':'test'}]})
        client=GatewayClient('http://127.0.0.1:8080',registered['token'],'restart_install','synthetic',
                             ['text_v1','official_identity_v1'])
        client.hello();client.heartbeat(worker_alive=True,desktop_unlocked=True,wecom_logged_in=True,auto_send=True)
        created=call('backend',f'/v1/backend/{tenant}/tasks',{'device_id':device,'inbox_id':'test','room_id':'test',
            'source_message_id':'restart_test','waybill':'SYNTHETIC_RESTART','recipient_id':'sim_member','robot_id':'sim_robot',
            'caption':'SYNTHETIC_RESTART: no real send'})
        task=client.claim();assert task['id']==created['id']
        client.begin_submit(task,'persisted_intent')
        client.receipt(task,'submitted',event_id='persisted_submitted')
        state={'tenant':tenant,'device':device,'token':registered['token'],'session':client.session_id,'task':task,'occurred_at':time.time()}
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as file:json.dump(state,file)
        print(json.dumps({'prepared':True,'task_id':task['id'],'state':'submitted','scope':'synthetic_restart_only'}))
    else:
        state=json.loads(path.read_text())
        client=GatewayClient('http://127.0.0.1:8080',state['token'],'restart_install','synthetic',
                             ['text_v1','official_identity_v1'])
        assert client.hello()['session_id']==state['session']
        client.heartbeat(worker_alive=True,desktop_unlocked=True,wecom_logged_in=True,auto_send=True)
        task=state['task']
        current=call('admin',f"/v1/admin/{state['tenant']}/tasks/{task['id']}")
        assert current['state'] in ('submitted','unknown')
        assert client.claim() is None
        events=[{'event_id':'restart_archive_'+uuid.uuid4().hex,'room_id':'test','actor_id':'sim_robot',
                 'occurred_at':state['occurred_at'],'kind':'text','text':task['payload']['caption']}]
        assert call('verifier',f"/v1/verifier/{state['tenant']}/tasks/{task['id']}/confirm",{'events':events})['state']=='confirmed'
        call('admin',f"/v1/admin/{state['tenant']}/devices/{state['device']}/revoke",{})
        path.unlink()
        print(json.dumps({'passed':True,'persisted_after_restart':True,'no_requeue':True,
            'session_preserved':True,'temporary_device_revoked':True,'scope':'synthetic_restart_only'}))

if __name__=='__main__':main()
