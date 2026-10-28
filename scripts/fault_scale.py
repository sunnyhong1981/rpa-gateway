"""Owned loopback-only fault tests. Never executes against shared/production dependencies."""
import asyncio
import json
import os
import signal
import ssl
import subprocess
import time
import uuid
from pathlib import Path
import httpx
import websockets

root=Path(__file__).resolve().parents[1]
state=root/'runtime/scale-local'
ssl_context=ssl.create_default_context(cafile=str(state/'cert.pem'))
base='https://127.0.0.1:38443'
admin={'Authorization':'Bearer '+(state/'admin_token').read_text().strip()}
backend={'Authorization':'Bearer '+(state/'backend_token').read_text().strip()}
verifier={'Authorization':'Bearer '+(state/'verifier_token').read_text().strip()}
result={}


def command(pid):return subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True).strip()


def restart_node(name):
    processes=json.loads((state/'processes.json').read_text())
    item=next(p for p in processes if p['name']==name)
    if str(root/'.venv/bin') not in command(item['pid']) or 'rpa_gateway' not in command(item['pid']):
        raise RuntimeError('Unexpected process ownership')
    os.kill(item['pid'],signal.SIGTERM)
    time.sleep(1)
    settings={'RPA_GATEWAY_DATABASE_URL':(state/'test_database_url').read_text().strip(),
        'RPA_GATEWAY_REDIS_URL':'redis://127.0.0.1:36379/0','RPA_GATEWAY_REDIS_NAMESPACE':'local_scale',
        'RPA_GATEWAY_OBJECT_CONFIG':(state/'objects.json').read_text(),
        'RPA_GATEWAY_MEDIA_PROXY':'0','RPA_GATEWAY_INSTANCE_ID':name}
    for role in ('ADMIN','BACKEND','VERIFIER'):settings['RPA_GATEWAY_'+role+'_TOKEN']=(state/(role.lower()+'_token')).read_text().strip()
    with (state/(name+'.log')).open('a') as log:
        process=subprocess.Popen(item['command'],cwd=root,env={**os.environ,**settings},stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    item['pid']=process.pid
    (state/'processes.json').write_text(json.dumps(processes,indent=2))


def redis_stop():
    pid=int((state/'redis.pid').read_text())
    if 'redis-server' not in command(pid) or '36379' not in command(pid):raise RuntimeError('Unexpected Redis ownership')
    os.kill(pid,signal.SIGTERM);time.sleep(.5)


def redis_start():
    subprocess.run(['redis-server','--bind','127.0.0.1','--port','36379','--save','','--appendonly','no','--maxmemory','64mb',
        '--maxmemory-policy','noeviction','--dir',str(state),'--daemonize','yes','--pidfile',str(state/'redis.pid'),'--logfile',str(state/'redis.log')],check=True)
    time.sleep(.5)


async def main():
    tenant='fault_'+uuid.uuid4().hex[:12];device='fault_sim_'+uuid.uuid4().hex
    async with httpx.AsyncClient(verify=ssl_context,timeout=15) as http:
        async def req(method,path,headers,body=None,origin=base,allowed=(200,)):
            response=await http.request(method,origin+path,headers=headers,json=body)
            if response.status_code not in allowed:raise RuntimeError('Unexpected HTTP '+str(response.status_code)+' during fault probe')
            return response
        reg=(await req('POST',f'/v1/admin/{tenant}/devices',admin,{'device_id':device,'bindings':[{'inbox_id':'test','room_id':'test'}]})).json()
        headers={'Authorization':'Bearer '+reg['token']}
        hello={'protocol':1,'installation_id':'fault_install','runtime_version':'synthetic','capabilities':['text_v1','official_identity_v1']}
        sid=(await req('POST','/v1/device/hello',headers,hello)).json()['session_id']
        async def beat():return await req('POST','/v1/device/heartbeat',headers,{'session_id':sid,'worker_alive':True,'desktop_unlocked':True,'wecom_logged_in':True,'auto_send':True})
        await beat()
        # Connect directly to B, enqueue through A. This proves cross-instance outbox -> Redis -> WS delivery.
        async with websockets.connect('ws://127.0.0.1:38082/v1/device/notifications',max_size=65536) as ws:
            await ws.send(json.dumps({'token':reg['token'],'session_id':sid}));await ws.recv()
            payload={'device_id':device,'inbox_id':'test','room_id':'test','source_message_id':'fault_source','waybill':'SYNTHETIC_FAULT',
                'recipient_id':'sim_member','robot_id':'sim_robot','caption':'模拟故障测试'}
            created=(await req('POST',f'/v1/backend/{tenant}/tasks',backend,payload,origin='http://127.0.0.1:38081')).json()
            until=time.monotonic()+5;notified=False
            while time.monotonic()<until:
                notice=json.loads(await asyncio.wait_for(ws.recv(),5))
                if any(t['id']==created['id'] for t in notice.get('tasks',[])):notified=True;break
            assert notified;result['cross_instance_event_delivery']=True
        task=(await req('POST','/v1/device/tasks/claim',headers,{'session_id':sid})).json()['task']
        async def receipt(stage,event_id=None):return await req('POST',f"/v1/device/tasks/{task['id']}/receipts",headers,
            {'session_id':sid,'lease_token':task['lease_token'],'event_id':event_id or stage,'state':stage})
        await receipt('submitting');archive_time=time.time()
        await receipt('submitted')
        restart_node('gateway-a');await asyncio.sleep(2)
        assert (await req('POST','/v1/device/tasks/claim',headers,{'session_id':sid})).json()['task'] is None
        assert (await req('POST','/v1/device/hello',headers,hello)).json()['session_id']==sid
        result['node_restart_session_preserved_no_requeue']=True
        try:
            redis_stop()
            denied=await req('POST','/v1/device/tasks/claim',headers,{'session_id':sid},allowed=(503,))
            duplicate=await receipt('submitted')
            assert duplicate.json()['duplicate'] is True
            result['redis_down_new_sends_paused_receipts_persist']=True
        finally:redis_start()
        await beat()
        assert (await req('POST','/v1/device/tasks/claim',headers,{'session_id':sid})).json()['task'] is None
        result['redis_restart_does_not_requeue_submitted']=True
        # Stop the owned local test PostgreSQL; no other database or service is targeted.
        assert (state/'pg/postmaster.pid').is_file()
        pg=['/opt/homebrew/opt/postgresql@17/bin/pg_ctl','-D',str(state/'pg')]
        try:
            subprocess.run(pg+['stop','-m','fast'],check=True,stdout=subprocess.DEVNULL)
            await req('GET','/healthz',{},allowed=(500,503))
            result['postgres_failure_reported_no_fake_success']=True
        finally:
            subprocess.run(pg+['-l',str(state/'postgres.log'),'-o','-h 127.0.0.1 -p 35432 -k /tmp -c max_connections=64 -c shared_buffers=128MB','start'],check=True,stdout=subprocess.DEVNULL)
        await asyncio.sleep(2)
        await beat()
        assert (await req('POST','/v1/device/tasks/claim',headers,{'session_id':sid})).json()['task'] is None
        result['postgres_restart_preserves_submitted']=True
        events=[{'event_id':'fault_archive_'+uuid.uuid4().hex,'kind':'text','text':task['payload']['caption'],
                 'room_id':'test','actor_id':'sim_robot','occurred_at':archive_time}]
        assert (await req('POST',f"/v1/verifier/{tenant}/tasks/{task['id']}/confirm",verifier,{'events':events})).json()['state']=='confirmed'
        await req('POST',f'/v1/admin/{tenant}/devices/{device}/revoke',admin,{})
    result.update(passed=True,scope='owned_local_dependency_faults_only',real_wecom_messages_sent=0)
    output=Path('/Users/sunny/Downloads/facebook-chat/outputs/rpa-gateway-scale-20261029/faults.json')
    output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))

if __name__=='__main__':asyncio.run(main())
