"""Synthetic gateway-only acceptance. No WeCom, Chatwoot, collector, or native desktop calls."""
import argparse
import hashlib
import io
import json
import sys
import uuid
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from PIL import Image
from rpa_gateway.client import GatewayClient


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--base-url',default='http://127.0.0.1:8080')
    parser.add_argument('--secrets-dir',default='/run/secrets')
    args=parser.parse_args()
    secrets_dir=Path(args.secrets_dir)
    roles={role:(secrets_dir/(role+'_token')).read_text().strip() for role in ('admin','backend','verifier')}
    tenant='synthetic_'+uuid.uuid4().hex[:12]
    def call(role,path,body=None,raw=None):
        data=raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        request=Request(args.base_url+path,data=data,headers={'Authorization':'Bearer '+roles[role],
            'Content-Type':'image/png' if raw is not None else 'application/json'})
        with urlopen(request,timeout=10) as response:return json.load(response)
    device='sim_'+uuid.uuid4().hex
    registered=call('admin',f'/v1/admin/{tenant}/devices',{'device_id':device,
        'bindings':[{'inbox_id':'synthetic_inbox','room_id':'synthetic_room'}]})
    try:
        client=GatewayClient(args.base_url,registered['token'],'test_install','synthetic-0.1',
            ['text_v1','caption_images_v1','official_identity_v1'])
        client.hello()
        client.heartbeat(worker_alive=True,desktop_unlocked=True,wecom_logged_in=True,auto_send=True)
        media=[]
        for color,kind in [('red','PNG'),('blue','JPEG'),('green','PNG')]:
            buffer=io.BytesIO();Image.new('RGB',(16,16),color).save(buffer,format=kind)
            media.append(call('backend',f'/v1/backend/{tenant}/media',raw=buffer.getvalue()))
        payload={'device_id':device,'inbox_id':'synthetic_inbox','room_id':'synthetic_room',
            'source_message_id':'synthetic_source','waybill':'SYNTHETIC_ONLY','recipient_id':'synthetic_recipient',
            'robot_id':'synthetic_robot','caption':'模拟验收 运单号：SYNTHETIC_ONLY，3张原图，不发群',
            'media_ids':[m['id'] for m in media]}
        created=call('backend',f'/v1/backend/{tenant}/tasks',payload)
        assert call('backend',f'/v1/backend/{tenant}/tasks',payload)['id']==created['id']
        task=client.claim();assert task['id']==created['id']
        assert client.claim() is None
        for item in task['media']:
            data=client.download(task,item)
            assert hashlib.md5(data).hexdigest()==item['md5']
        client.begin_submit(task,event_id='synthetic_submit_intent')
        # Simulate UI result only. There is no desktop operation in this script.
        assert client.receipt(task,'submitted',event_id='synthetic_submitted')['state']=='submitted'
        import time
        common={'room_id':'synthetic_room','actor_id':'synthetic_robot','occurred_at':time.time()}
        events=[dict(common,event_id='sim_text_'+uuid.uuid4().hex,kind='text',text=payload['caption'])]
        for item in media:
            events.append(dict(common,event_id='sim_img_'+uuid.uuid4().hex,kind='image',md5=item['md5'],size=item['size']))
        confirmation=call('verifier',f"/v1/verifier/{tenant}/tasks/{task['id']}/confirm",{'events':events})
        assert confirmation['state']=='confirmed'
        result=call('admin',f"/v1/admin/{tenant}/tasks/{task['id']}")
        assert [event['state'] for event in result['events']]==['queued','leased','submitting','submitted','confirmed']
        print(json.dumps({'passed':True,'scope':'gateway_synthetic_only','tenant':tenant,'task_id':task['id'],
            'original_images':3,'mime_detection':True,'dedup':True,'single_claim':True,
            'state_timeline':[event['state'] for event in result['events']],
            'real_wecom_messages_sent':0,'legacy_clients_changed':0},ensure_ascii=False))
    finally:
        call('admin',f'/v1/admin/{tenant}/devices/{device}/revoke',{})

if __name__=='__main__':main()
