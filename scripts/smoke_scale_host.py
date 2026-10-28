"""Real isolated v3 ingress, S3 signed transfer, cross-node notification and ledger verification."""
import hashlib
import io
import json
import ssl
import time
import uuid
from pathlib import Path
import httpx
from PIL import Image

root=Path('/opt/ai/rpa-gateway')
secrets_dir=root/'runtime/secrets'
roles={role:{'Authorization':'Bearer '+(secrets_dir/(role+'_token')).read_text().strip()} for role in ('admin','backend','verifier')}
ssl_context=ssl.create_default_context(cafile=str(root/'runtime/tls/cert.pem'))
base='https://127.0.0.1:31443'
tenant='scale_'+uuid.uuid4().hex[:12];device='scale_sim_'+uuid.uuid4().hex
result={'scope':'isolated_scale_synthetic_only','real_wecom_messages_sent':0}
with httpx.Client(verify=ssl_context,timeout=15,trust_env=False) as http:
    def call(method,path,headers,body=None):
        response=http.request(method,base+path,headers=headers,json=body)
        if response.status_code!=200:raise RuntimeError('Unexpected HTTP '+str(response.status_code)+' at '+path.split('/')[2])
        return response.json()
    registration=call('POST',f'/v1/admin/{tenant}/devices',roles['admin'],{'device_id':device,'bindings':[{'inbox_id':'synthetic','room_id':'synthetic'}]})
    headers={'Authorization':'Bearer '+registration['token']}
    try:
        hello=call('POST','/v1/device/hello',headers,{'protocol':1,'installation_id':'synthetic_install','runtime_version':'scale_synthetic',
            'capabilities':['text_v1','caption_images_v1','official_identity_v1']})
        sid=hello['session_id']
        call('POST','/v1/device/heartbeat',headers,{'session_id':sid,'worker_alive':True,'desktop_unlocked':True,'wecom_logged_in':True,'auto_send':True})
        media=[]
        # Exercise the actual S3 service and TLS Host/signature forwarding, not the test emulator.
        for color,kind in [('red','PNG'),('blue','JPEG'),('green','PNG')]:
            stream=io.BytesIO();Image.new('RGB',(64,64),color).save(stream,format=kind);raw=stream.getvalue()
            upload=call('POST',f'/v1/backend/{tenant}/media/uploads',roles['backend'],{
                'sha256':hashlib.sha256(raw).hexdigest(),'md5':hashlib.md5(raw).hexdigest(),'size':len(raw)})
            response=http.post(upload['upload']['url'],data=upload['upload']['fields'],files={'file':('original',raw)})
            if response.status_code not in (200,201,204):raise RuntimeError('Signed S3 upload failed '+str(response.status_code))
            status_path=f"/v1/backend/{tenant}/media/uploads/{upload['upload_id']}"
            call('POST',status_path+'/complete',roles['backend'],{})
            until=time.monotonic()+20
            while True:
                status=call('GET',status_path,roles['backend'])
                if status['state']=='ready':break
                if status['state']=='rejected' or time.monotonic()>until:raise RuntimeError('Original validation did not succeed')
                time.sleep(.2)
            media.append({'id':status['media_id'],'raw':raw,'sha256':hashlib.sha256(raw).hexdigest(),
                'md5':hashlib.md5(raw).hexdigest(),'size':len(raw)})
        payload={'device_id':device,'inbox_id':'synthetic','room_id':'synthetic','source_message_id':'scale_synthetic_source',
            'waybill':'SYNTHETIC_SCALE','recipient_id':'synthetic_recipient','robot_id':'synthetic_robot',
            'caption':'模拟网关优化验收 SYNTHETIC_SCALE 三张原图','media_ids':[m['id'] for m in media]}
        created=call('POST',f'/v1/backend/{tenant}/tasks',roles['backend'],payload)
        assert call('POST',f'/v1/backend/{tenant}/tasks',roles['backend'],payload)['id']==created['id']
        task=call('POST','/v1/device/tasks/claim',headers,{'session_id':sid})['task']
        assert task['id']==created['id'] and len(task['media'])==3
        for original in media:
            signed=call('GET',f"/v1/device/tasks/{task['id']}/media/{original['id']}/download",headers)
            response=http.get(signed['url']) # Explicitly no RPA bearer on the object download.
            assert response.status_code==200 and response.content==original['raw']
            assert hashlib.sha256(response.content).hexdigest()==signed['sha256']
        for stage in ('submitting','submitted'):
            response=call('POST',f"/v1/device/tasks/{task['id']}/receipts",headers,{'session_id':sid,'lease_token':task['lease_token'],'event_id':stage,'state':stage})
            assert response['state']==stage
        events=[{'event_id':'scale_arc_'+uuid.uuid4().hex,'room_id':'synthetic','actor_id':'synthetic_robot',
                 'occurred_at':time.time(),'kind':'text','text':payload['caption']}]
        events.extend({'event_id':'scale_arc_'+uuid.uuid4().hex,'room_id':'synthetic','actor_id':'synthetic_robot','occurred_at':time.time(),
            'kind':'image','md5':m['md5'],'size':m['size']} for m in media)
        assert call('POST',f"/v1/verifier/{tenant}/tasks/{task['id']}/confirm",roles['verifier'],{'events':events})['state']=='confirmed'
        sync=call('GET','/v1/device/sync?session_id='+sid,headers)
        assert sync['tasks']==[]
        result.update(passed=True,task_id=task['id'],original_images=3,tls_verified=True,signed_direct_upload=True,
            signed_direct_download=True,original_bytes_preserved=True,idempotent=True,simulated_archive_confirmed=True)
    finally:
        call('POST',f'/v1/admin/{tenant}/devices/{device}/revoke',roles['admin'],{})
print(json.dumps(result,ensure_ascii=False))
