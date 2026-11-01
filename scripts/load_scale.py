"""Real TLS/WSS synthetic load. Refuses production DB and records no device secrets."""
import argparse
import asyncio
import hashlib
import io
import json
import random
import ssl
import statistics
import sys
import time
import uuid
from collections import Counter,deque
from pathlib import Path
import httpx
import websockets
from PIL import Image


def percentile(values,p):
    if not values:return None
    ordered=sorted(values)
    return round(ordered[min(len(ordered)-1,int((len(ordered)-1)*p))]*1000,3)


async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--clients',type=int,default=1000)
    parser.add_argument('--seconds',type=int,default=90)
    parser.add_argument('--tasks-per-second',type=int,default=20)
    parser.add_argument('--task-seconds',type=int,default=30)
    parser.add_argument('--output',required=True)
    parser.add_argument('--base',default='https://127.0.0.1:38443')
    parser.add_argument('--state-dir',default='')
    parser.add_argument('--ws-bases',default='')
    args=parser.parse_args()
    ws_bases=args.ws_bases.split(',') if args.ws_bases else [args.base]
    if any(url not in ('https://127.0.0.1:38443','https://127.0.0.1:39443','https://127.0.0.1:39445','https://127.0.0.1:39447','https://127.0.0.1:39449') for url in ws_bases):raise ValueError('Unknown sandbox websocket origin')
    if args.base not in ('https://127.0.0.1:38443','https://127.0.0.1:39443'):raise ValueError('Use only the owned local lab or the user-selected UAT sandbox tunnel')
    root=Path(__file__).resolve().parents[1]
    state=Path(args.state_dir) if args.state_dir else root/'runtime/scale-local'
    context=ssl.create_default_context(cafile=str(state/'cert.pem'))
    admin={'Authorization':'Bearer '+(state/'admin_token').read_text().strip()}
    backend={'Authorization':'Bearer '+(state/'backend_token').read_text().strip()}
    verifier={'Authorization':'Bearer '+(state/'verifier_token').read_text().strip()}
    tenant='load_'+uuid.uuid4().hex[:12]
    prefix='sim_'+uuid.uuid4().hex[:12]
    latencies=deque(maxlen=100000);request_count=0;errors=Counter();notification_times={};dispatch=[];devices=[];sockets=[]
    task_ids=set();confirmed=set();receivers=[];send_locks=[];start=time.monotonic();finished=asyncio.Event()
    transport=httpx.AsyncHTTPTransport(verify=context,retries=0,limits=httpx.Limits(max_connections=48,max_keepalive_connections=48))
    # Bound callers before httpcore's connection pool: a synchronized heartbeat burst
    # must not make every queued request repeatedly scan all persistent sockets.
    http_slots=asyncio.Semaphore(48)
    async with httpx.AsyncClient(base_url=args.base,transport=transport,timeout=15) as http:
        async def request(method,path,headers,body=None):
            nonlocal request_count
            began=time.monotonic()
            async with http_slots:
                response=await http.request(method,path,headers=headers,json=body)
            latencies.append(time.monotonic()-began);request_count+=1
            if response.status_code!=200:
                errors[str(response.status_code)]+=1
                raise RuntimeError('Unexpected HTTP '+str(response.status_code)+' at '+path.split('/')[2])
            return response.json()
        await request('GET','/healthz',{})
        sem=asyncio.Semaphore(16)
        async def register(i):
            async with sem:
                reg=await request('POST',f'/v1/admin/{tenant}/devices',admin,{'device_id':prefix+'_'+str(i),
                    'bindings':[{'inbox_id':'load_inbox','room_id':'load_room'}]})
                d={'id':reg['device_id'],'headers':{'Authorization':'Bearer '+reg['token']},'token':reg['token']}
                hello=await request('POST','/v1/device/hello',d['headers'],{'protocol':1,'installation_id':'load_install',
                    'runtime_version':'synthetic_load','capabilities':['text_v1','caption_images_v1','official_identity_v1']})
                d['session']=hello['session_id']
                await beat(d)
                return d
        async def beat(d):
            await request('POST','/v1/device/heartbeat',d['headers'],{'session_id':d['session'],'worker_alive':True,
                'desktop_unlocked':True,'wecom_logged_in':True,'auto_send':True})
        try:
            devices.extend(await asyncio.gather(*(register(i) for i in range(args.clients))))
            async def receive(ws):
                try:
                    async for text in ws:
                        message=json.loads(text)
                        if message.get('type')=='tasks_changed':
                            for task in message['tasks']:
                                notification_times.setdefault(task['id'],time.monotonic())
                except Exception:
                    if not finished.is_set():errors['ws_disconnect']+=1
            async def connect(index,d):
                async with sem:
                    ws=await websockets.connect(ws_bases[index%len(ws_bases)].replace('https://','wss://')+'/v1/device/notifications',ssl=context,
                                                open_timeout=15,ping_interval=20,ping_timeout=20,max_size=64*1024)
                    await ws.send(json.dumps({'token':d['token'],'session_id':d['session']}))
                    initial=json.loads(await asyncio.wait_for(ws.recv(),15))
                    assert initial['type']=='tasks_changed'
                    receivers.append(asyncio.create_task(receive(ws)))
                    sockets.append(ws);send_locks.append(asyncio.Lock())
            await asyncio.gather(*(connect(i,d) for i,d in enumerate(devices)))
            connected_at=time.monotonic()
            print(json.dumps({'phase':'connected','clients':len(sockets),'seconds':round(connected_at-start,2)}),flush=True)
            async def keepalive(index,d):
                await asyncio.sleep(random.uniform(0,10))
                while not finished.is_set():
                    try:
                        await beat(d)
                        await sockets[index].send('ping')
                    except Exception:errors['keepalive']+=1
                    try:await asyncio.wait_for(finished.wait(),15)
                    except TimeoutError:pass
            heartbeats=[asyncio.create_task(keepalive(i,d)) for i,d in enumerate(devices)]
            # A real original fixture ~2 MB. Upload once; tasks each reference three distinct originals.
            media=[]
            rng=random.Random(42)
            for index in range(3):
                image=Image.frombytes('RGB',(1000,700),rng.randbytes(1000*700*3))
                buffer=io.BytesIO();image.save(buffer,format='PNG');raw=buffer.getvalue()
                response=await http.post(f'/v1/backend/{tenant}/media',headers=backend,content=raw,timeout=30)
                if response.status_code!=200:raise RuntimeError('Image ingress failed '+str(response.status_code))
                media.append(response.json())
            sem_tasks=asyncio.Semaphore(32)
            async def job(index):
                async with sem_tasks:
                    d=devices[index%len(devices)]
                    created=await request('POST',f'/v1/backend/{tenant}/tasks',backend,{
                        'device_id':d['id'],'inbox_id':'load_inbox','room_id':'load_room','source_message_id':'source_'+str(index),
                        'waybill':'SYNTHETIC_'+str(index),'recipient_id':'sim_recipient','robot_id':'sim_robot',
                        'caption':'模拟容量验收 '+str(index),'media_ids':[m['id'] for m in media]})
                    ready=time.monotonic();task_id=created['id'];task_ids.add(task_id)
                    deadline=ready+10
                    while task_id not in notification_times and time.monotonic()<deadline:await asyncio.sleep(.005)
                    if task_id not in notification_times:errors['notification_timeout']+=1
                    else:dispatch.append(max(0,notification_times[task_id]-ready))
                    claimed=await request('POST','/v1/device/tasks/claim',d['headers'],{'session_id':d['session']})
                    task=claimed['task']
                    if task is None or task['id']!=task_id:raise RuntimeError('Claim identity mismatch')
                    async def receipt(stage):
                        return await request('POST',f'/v1/device/tasks/{task_id}/receipts',d['headers'],{
                            'session_id':d['session'],'lease_token':task['lease_token'],'event_id':str(index)+'_'+stage,'state':stage})
                    assert (await receipt('submitting'))['state']=='submitting'
                    assert (await receipt('submitted'))['state']=='submitted'
                    common={'room_id':'load_room','actor_id':'sim_robot','occurred_at':time.time()}
                    events=[dict(common,event_id=task_id+'_text',kind='text',text=task['payload']['caption'])]
                    events.extend(dict(common,event_id=task_id+'_'+str(i),kind='image',md5=m['md5'],size=m['size']) for i,m in enumerate(media))
                    result=await request('POST',f'/v1/verifier/{tenant}/tasks/{task_id}/confirm',verifier,{'events':events})
                    assert result['state']=='confirmed'
                    confirmed.add(task_id)
            jobs=[]
            count=args.tasks_per_second*args.task_seconds
            task_start=time.monotonic()
            for i in range(count):
                jobs.append(asyncio.create_task(job(i)))
                await asyncio.sleep(max(0,task_start+(i+1)/args.tasks_per_second-time.monotonic()))
            results=await asyncio.gather(*jobs,return_exceptions=True)
            task_run_seconds=time.monotonic()-task_start
            for result in results:
                if isinstance(result,Exception):errors[type(result).__name__]+=1
            remain=max(0,args.seconds-(time.monotonic()-connected_at))
            while remain>0:
                await asyncio.sleep(min(10,remain))
                progress={'phase':'hold','clients':len(sockets),'confirmed':len(confirmed),'elapsed_seconds':round(time.monotonic()-connected_at,2),'request_count':request_count,'error_counts':dict(errors),'planned_seconds':args.seconds,'real_wecom_messages_sent':0}
                progress_path=Path(args.output).with_suffix('.progress.json');temp=progress_path.with_suffix('.tmp');temp.write_text(json.dumps(progress,indent=2));temp.replace(progress_path)
                print(json.dumps(progress),flush=True)
                remain=max(0,args.seconds-(time.monotonic()-connected_at))
            status=await request('GET',f'/v1/admin/{tenant}/status',admin)
            online=sum(not d['stale'] for d in status['devices'])
            open_sockets=sum(ws.state.name=='OPEN' for ws in sockets)
            result={'scope':'uat_independent_sandbox_over_ssh_tls' if args.base.endswith(':39443') else 'local_two_instances_tls_wss_synthetic','clients':args.clients,'online_at_end':online,
                'websockets_open_at_end':open_sockets,'task_run_seconds':round(task_run_seconds,3),
                'achieved_confirmed_tasks_per_second':round(len(confirmed)/task_run_seconds,3),
                'connected_hold_seconds':round(time.monotonic()-connected_at,3),'expected_tasks':count,'unique_created':len(task_ids),
                'unique_confirmed':len(confirmed),'request_samples':len(latencies),'request_count':request_count,'latency_window_last_requests':100000,'http_p95_ms':percentile(latencies,.95),
                'http_p99_ms':percentile(latencies,.99),'notification_p95_ms':percentile(dispatch,.95),
                'notification_p99_ms':percentile(dispatch,.99),'errors':dict(errors),'real_wecom_messages_sent':0,
                'load_generator_and_services_same_host':args.base.endswith(':38443'),'long_soak_validated':False,
                'passed':online==args.clients and open_sockets==args.clients and not errors and len(confirmed)==count,
                'latency_targets_met':bool(latencies and dispatch and percentile(latencies,.95)<200 and percentile(dispatch,.95)<500)}
            Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps(result),flush=True)
        finally:
            finished.set()
            for task in receivers:task.cancel()
            await asyncio.gather(*(ws.close() for ws in sockets),return_exceptions=True)
            # Uncertain tasks are never replayed or closed automatically. Retain any failures for inspection.
            async def revoke(d):
                async with sem:
                    try:await request('POST',f"/v1/admin/{tenant}/devices/{d['id']}/revoke",admin,{})
                    except Exception:pass
            await asyncio.gather(*(revoke(d) for d in devices))
    if not result['passed']:raise SystemExit(1)

if __name__=='__main__':asyncio.run(main())
