"""Install a reviewed source candidate without changing native adapters or replacing state."""
import hashlib,json,os,sqlite3,subprocess,sys,time,zipfile
from pathlib import Path
root=Path('/Users/sunny/AI/QUNL-RPA-mac');old=(root/'current').resolve()
if old.name!='2026.10.29.2':raise RuntimeError('Native baseline changed; refresh candidate before installation')
manifest=json.loads((old/'MANIFEST.json').read_text())
for name,digest in manifest['files'].items():
 if hashlib.sha256((old/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Native baseline hash changed')
sys.path.insert(0,str(old))
from scripts.rpa_service import Service
service=Service(old);settings=service.settings();status=service.inspect()
if not status['running'] or status['ownership']!='verified':raise RuntimeError('Expected verified running native service')
state=Path(settings['state'])
with sqlite3.connect('file:'+str(state)+'?mode=ro',uri=True) as db:
 rows=db.execute("SELECT scope,state,count(*) FROM outgoing WHERE (scope LIKE '%|469|%' OR scope LIKE '%|491|%') AND state IN ('queued','sending','confirming','unknown') GROUP BY scope,state").fetchall()
 if rows:raise RuntimeError('Native queues are not idle')
archive=Path('/Users/sunny/Downloads/facebook-chat/outputs/rpa-gateway-mac-pilot-20261029/QUNL-RPA-mac-2026.10.29.90-gateway.zip')
candidate=root/'versions/2026.10.29.90'
if candidate.exists():raise RuntimeError('Candidate version already exists; never overwrite an installed version')
with zipfile.ZipFile(archive) as z:
 prefix='QUNL-RPA-mac-2026.10.29.90/'
 for name in z.namelist():
  if not name.startswith(prefix) or '..' in Path(name).parts:raise RuntimeError('Unsafe release member')
  relative=name[len(prefix):]
  if not relative:continue
  dest=candidate/relative;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(z.read(name))
package=json.loads((candidate/'MANIFEST.json').read_text())
for name,digest in package['files'].items():
 if hashlib.sha256((candidate/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Candidate manifest differs')
for name in ['mac_desktop.py','mac_image_delivery.py','mac_group_navigation.py','mac_group_messages.py','rpa_member_identity.py','rpa_daemon.py']:
 if hashlib.sha256((candidate/name).read_bytes()).digest()!=hashlib.sha256((old/name).read_bytes()).digest():
  raise RuntimeError('Current native adapter must be preserved')
original_config=Path(settings['config']);groups=json.loads(original_config.read_text())
new_groups=json.loads(json.dumps(groups));pilot=root/'data/gateway-pilot'
for entry in new_groups['groups']:
 p=Path(entry['config']);p=p if p.is_absolute() else original_config.parent/p
 c=json.loads(p.read_text())
 if str(c['inbox_id'])=='469':
  c['queue_transport']='gateway';c['gateway_config_path']=str(pilot/'gateway.json')
  saved=pilot/'rpa-test.gateway.json';saved.write_text(json.dumps(c,ensure_ascii=False,indent=2));saved.chmod(0o600)
  entry['config']=str(saved)
 else:entry['config']=str(p)
new_config=root/'data/groups.gateway-pilot.local.json';new_config.write_text(json.dumps(new_groups,ensure_ascii=False,indent=2));new_config.chmod(0o600)
backup=pilot/('handover-'+str(int(time.time())));backup.mkdir(mode=0o700)
(backup/'original-settings.json').write_text(json.dumps(settings));(backup/'original-settings.json').chmod(0o600)
plan={'root':str(root),'old_root':str(old),'candidate':str(candidate),'state':str(state),'success':str(backup/'ready.json')}
plan_file=backup/'watchdog-plan.json';plan_file.write_text(json.dumps(plan));plan_file.chmod(0o600)
watchdog=Path(__file__).with_name('mac_pilot_watchdog.py')
with (backup/'watchdog.log').open('w') as log:
 guard=subprocess.Popen([settings['python'],str(watchdog),str(plan_file)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
start=time.monotonic()
try:
 service.stop(timeout=30)
 # From this point there is no old desktop owner. Fresh source IDs below the floor remain legacy-owned.
 remote="""import subprocess,json
p=subprocess.run(['docker','exec','chatwoot-postgres-1','psql','-U','postgres','-d','chatwoot','-At','-v','ON_ERROR_STOP=1','-c','SELECT COALESCE(max(id),0) FROM rpa_gateway_source.test_messages'],capture_output=True,text=True,check=True)
floor=int(p.stdout.strip())
sql="UPDATE source_routes SET mode='gateway',floor=%d,cursor=%d,generation=generation+1,client_ready_at=NULL WHERE tenant='chatwoot_871' AND inbox='469'; UPDATE source_jobs SET state='legacy_floor' WHERE tenant='chatwoot_871' AND inbox='469' AND task IS NULL AND message_id<=%d;"%(floor,floor,floor)
subprocess.run(['docker','exec','-i','chatwoot-postgres-1','psql','-U','postgres','-d','rpa_gateway','-At','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True,check=True)
print(json.dumps({'floor':floor,'mode':'gateway'}))
"""
 result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','chatwoot-prod','python3','-'],input=remote,text=True,capture_output=True,timeout=30)
 if result.returncode:raise RuntimeError('Gateway source ownership handover failed')
 route=json.loads(result.stdout)
 temp=root/('current.pilot.'+str(os.getpid()));temp.symlink_to(candidate);os.replace(temp,root/'current')
 new_service=Service(candidate)
 new_service.install(config=new_config,state=state,interval=settings['interval'],python=settings['python'],auto_send=settings['auto_send'])
 started=new_service.start(timeout=20)
 ready={'installed':True,'runtime_version':'2026.10.29.90','pid':started['pid'],'pause_seconds':round(time.monotonic()-start,3),
        'config':str(new_config),'old_config_preserved':True,'state_preserved':True,'sqlite_restored':False,
        'route':route,'backup':str(backup),'watchdog_pid':guard.pid}
 Path(plan['success']).write_text(json.dumps(ready));print(json.dumps(ready,ensure_ascii=False))
except Exception as exc:
 # The independent guard handles recovery; it never restores an old SQLite snapshot or replays uncertain tasks.
 print(json.dumps({'installed':False,'type':type(exc).__name__,'watchdog_plan':str(plan_file)}));raise
