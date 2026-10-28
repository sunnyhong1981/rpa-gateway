"""Read-only deployment verification; exclude secrets and customer message contents."""
import json
import ssl
import subprocess
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request,urlopen
root=Path('/opt/ai/rpa-gateway')
before_path=root/'scale-before-services.json';before=json.loads(before_path.read_text())
rows=[json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines()]
current={r['Names']:r['ID'] for r in rows}
changed={name:{'before':cid,'after':current.get(name)} for name,cid in before.items()
         if name!='qunl-rpa-gateway-gateway-1' and current.get(name)!=cid}
services={}
for name in ['gateway-a','gateway-b','media','worker','redis','objects','ingress','prometheus']:
    container='qunl-rpa-gateway-scale-'+name+'-1'
    d=json.loads(subprocess.check_output(['docker','inspect',container]))[0]
    services[name]={'id':d['Id'][:12],'image':d['Config']['Image'],'image_id':d['Image'],
                    'state':d['State']['Status'],'health':d['State'].get('Health',{}).get('Status'),
                    'ports':d['HostConfig']['PortBindings']}
context=ssl.create_default_context(cafile=str(root/'runtime/tls/cert.pem'))
with urlopen('https://127.0.0.1:31443/healthz',context=context,timeout=5) as response:health=json.load(response)
request=Request('https://127.0.0.1:31444/rpa-gateway-originals')
try:
    with urlopen(request,context=context,timeout=5) as response:anonymous=response.status
except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as exc:anonymous=exc.code
with urlopen('http://127.0.0.1:31090/api/v1/targets',timeout=5) as response:targets=json.load(response)['data']['activeTargets']
with urlopen('http://127.0.0.1:31090/api/v1/rules',timeout=5) as response:rule_groups=json.load(response)['data']['groups']
proof_script='''
import hashlib,json
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from rpa_gateway.app import Settings
from rpa_gateway.objects import Objects
s=Settings.from_env();objects=Objects(s.object_config)
with psycopg.connect(s.database,row_factory=dict_row) as db:
 db.execute('SET TRANSACTION READ ONLY')
 result=dict(db.execute("SELECT (SELECT count(*) FROM devices WHERE revoked=0) active_credentials, (SELECT count(*) FROM tasks WHERE state IN ('queued','leased','submitting','submitted','unknown')) active_tasks, (SELECT count(*) FROM media WHERE data IS NOT NULL) legacy_blobs, (SELECT count(*) FROM outbox WHERE delivered_at IS NULL) outbox_pending, (SELECT count(*) FROM media_uploads WHERE state IN ('queued','validating')) media_pending, (SELECT updated FROM worker_health WHERE name='maintenance') worker_updated, (SELECT count(*) FROM pg_stat_activity WHERE usename='rpa_gateway') gateway_connections").fetchone())
 states=db.execute('SELECT state,count(*) n FROM tasks GROUP BY state').fetchall();result['states']={r['state']:r['n'] for r in states}
 all_media=db.execute('SELECT sha256,md5,size,object_key FROM media').fetchall()
 for media in all_media:
  data=objects.get(media['object_key'])
  assert hashlib.sha256(data).hexdigest()==media['sha256'] and hashlib.md5(data).hexdigest()==media['md5'] and len(data)==media['size']
 result['all_object_bytes_verified']=len(all_media)
 result['source_sha256']={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path('rpa_gateway').glob('*.py')) if not p.name.startswith('._')}
 print(json.dumps(result))
'''
proof=json.loads(subprocess.check_output(['docker','exec','-i','qunl-rpa-gateway-scale-media-1','python','-'],input=proof_script,text=True))
starts={}
for name in ['chatwoot-postgres-1','chatwoot-rails-1','chatwoot-sidekiq-1']:
    d=json.loads(subprocess.check_output(['docker','inspect',name]))[0]
    starts[name]=datetime.fromisoformat(d['State']['StartedAt'].replace('Z','+00:00')).timestamp()<before_path.stat().st_mtime
result={'version':'0.3.0','health':health,'services':services,'unchanged_original_services':len(before)-1,
        'original_service_id_changes':changed,'original_service_start_times_unchanged':starts,
        'object_store_anonymous_status':anonymous,'prometheus_targets':[{'health':t['health'],'endpoint':t['scrapeUrl'],'error':t['lastError']} for t in targets],
        'alert_rules':sum(len(group['rules']) for group in rule_groups),
        'rule_health':[{'name':r['name'],'health':r['health']} for g in rule_groups for r in g['rules']],
        'ledger':proof,'public_ports_added':False,'checked_at_epoch':time.time()}
print(json.dumps(result,ensure_ascii=False,indent=2))
assert health['version']=='0.3.0' and health['storage']=='postgresql'
assert not changed and all(starts.values())
assert all(s['state']=='running' for s in services.values())
assert all(services[name]['health']=='healthy' for name in ['gateway-a','gateway-b','media','worker'])
assert services['gateway-a']['image_id']==services['gateway-b']['image_id']==services['media']['image_id']==services['worker']['image_id']
assert len(targets)==3 and all(t['health']=='up' and not t['lastError'] for t in targets)
assert result['alert_rules']==11 and all(r['health']=='ok' for r in result['rule_health'])
assert anonymous in (401,403)
assert proof['active_credentials']==0 and proof['active_tasks']==0 and proof['legacy_blobs']==0
assert proof['outbox_pending']==0 and proof['media_pending']==0 and time.time()-proof['worker_updated']<60
assert proof['gateway_connections']<=16 and proof['all_object_bytes_verified']>=12
for service in services.values():
 for bindings in (service['ports'] or {}).values():
  assert all(binding['HostIp']=='127.0.0.1' for binding in bindings)
