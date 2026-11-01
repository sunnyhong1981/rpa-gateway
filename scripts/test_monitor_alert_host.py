"""One explicitly authorized test alert to the existing RPA monitor group."""
import hashlib,json,time,uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import Request,urlopen
root=Path('/opt/ai/rpa-gateway')
key=hashlib.sha256(('monitor-pilot-'+uuid.uuid4().hex).encode()).hexdigest()[:16]
body={'alerts':[{'fingerprint':key,'status':'firing','startsAt':datetime.now(timezone.utc).isoformat(),
 'labels':{'severity':'info','instance':'本机RPA网关试接'},'annotations':{'summary':'RPA网关告警通道接入测试：这是一条验收消息，不代表实际故障。'}}]}
request=Request('http://127.0.0.1:31080/v1/admin/alerts/ingest',data=json.dumps(body,ensure_ascii=False).encode(),
 headers={'Authorization':'Bearer '+(root/'runtime/secrets/admin_token').read_text().strip(),'Content-Type':'application/json'})
with urlopen(request,timeout=15) as response:result=json.load(response)
print(json.dumps({'accepted':result['accepted'],'channel':'RPA漏回监控群','fingerprint':key,'at':time.time()},ensure_ascii=False))
