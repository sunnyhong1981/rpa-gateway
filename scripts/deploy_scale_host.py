"""Promote the isolated gateway only. Retain a safe 0.3.0 single-replica recovery mode."""
import json
import os
import shutil
import subprocess
import tarfile
import time
from pathlib import Path
from urllib.request import urlopen
root=Path('/opt/ai/rpa-gateway')
candidate=Path('/opt/ai/rpa-gateway-scale-candidate-20261029')
backup=root/'runtime/releases'/time.strftime('scale-%Y%m%d-%H%M%S',time.gmtime())
backup.mkdir(parents=True,mode=0o700)
old_compose=['docker','compose','-f',str(root/'deploy/compose.yaml')]
new_compose=['docker','compose','-f',str(root/'deploy/scale.compose.yaml')]


def run(command,input=None,timeout=120):
    result=subprocess.run(command,input=input,text=True,capture_output=True,timeout=timeout)
    if result.returncode:
        # Docker output might contain secret configuration; never return it to the chat log.
        raise RuntimeError('Scale deployment command failed; command output withheld')
    return result.stdout


def health():
    with urlopen('http://127.0.0.1:31080/healthz',timeout=3) as response:return json.load(response)

current=health()
if current.get('version')!='0.2.0':raise RuntimeError('Expected isolated gateway 0.2.0')
# Confirm there are no real clients or active gateway tasks before changing its ingress.
sql="SELECT count(*) FROM devices WHERE revoked=0; SELECT count(*) FROM tasks WHERE state IN ('queued','leased','submitting','submitted','unknown');"
counts=run(['docker','exec','chatwoot-postgres-1','psql','-U','postgres','-d','rpa_gateway','-At','-v','ON_ERROR_STOP=1','-c',sql])
if counts.strip().splitlines()!=['0','0']:raise RuntimeError('Gateway is not idle')
tracked=['rpa_gateway','scripts','tests','deploy','Dockerfile','.dockerignore','.gitignore','pyproject.toml','requirements.txt','requirements-dev.txt','README.md','API.md','INTEGRATION.md','CAPACITY-1000.md']
with tarfile.open(backup/'previous-source.tar.gz','w:gz') as archive:
    for name in tracked:
        if (root/name).exists():archive.add(root/name,arcname=name,filter=lambda entry:None if '__pycache__' in entry.name else entry)
# Separate database backup before any 0.3.0 additive schema upgrades.
with (backup/'postgres-before.dump').open('wb') as output:
    subprocess.run(['docker','exec','chatwoot-postgres-1','pg_dump','-U','postgres','-d','rpa_gateway','-Fc'],stdout=output,check=True)
for name in tracked:
    if (candidate/name).is_dir():shutil.copytree(candidate/name,root/name,dirs_exist_ok=True)
    elif (candidate/name).is_file():shutil.copy2(candidate/name,root/name)
run(['python3',str(root/'scripts/prepare_scale_host.py')])
# Pull only new isolated components; existing gateway remains available while this happens.
run(new_compose+['pull','redis','objects','ingress','prometheus'],timeout=180)
run(new_compose+['up','-d','--no-deps','redis','objects'])
bootstrap="""
import json,time
from pathlib import Path
from rpa_gateway.objects import Objects
settings=json.loads(Path('/run/secrets/object_config').read_text())
objects=Objects(settings)
for attempt in range(45):
 try:
  objects.client.list_buckets()
  break
 except Exception:time.sleep(1)
else:raise RuntimeError('Object storage readiness failed')
try:objects.client.head_bucket(Bucket=objects.bucket)
except Exception:objects.client.create_bucket(Bucket=objects.bucket)
# Staging authorizations expire in five minutes; abandonments are additionally removed by lifecycle.
objects.client.put_bucket_lifecycle_configuration(Bucket=objects.bucket,LifecycleConfiguration={'Rules':[
 {'ID':'abandoned-staging','Status':'Enabled','Filter':{'Prefix':objects.prefix+'staging/'},'Expiration':{'Days':1},'AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}}]})
print('Object bucket ready')
"""
run(['docker','run','--rm','-i','--network','qunl-rpa-gateway_isolated','--cpus','0.5','--memory','192m',
 '--mount','type=bind,src='+str(root/'runtime/secrets/object_config')+',dst=/run/secrets/object_config,readonly',
 'qunl-rpa-gateway:0.3.0','python','-'],input=bootstrap)
# Start API/media and worker on internal ports first. They do not touch existing desktop clients.
start=time.monotonic()
try:
    run(new_compose+['up','-d','--no-deps','gateway-a','gateway-b','media','worker'])
    run(old_compose+['stop','gateway'])
    run(new_compose+['up','-d','--no-deps','ingress','prometheus'])
    ready=None
    until=time.monotonic()+45
    while time.monotonic()<until:
        try:
            ready=health()
            if ready.get('version')=='0.3.0':break
        except Exception:pass
        time.sleep(1)
    else:raise RuntimeError('Ingress did not become ready')
    result={'deployed':True,'version':'0.3.0','entry_pause_seconds':round(time.monotonic()-start,3),
            'backup':str(backup),'health':ready,'formal_rpa_clients_changed':0,
            'loopback_ports':[31080,31443,31444,31090],'public_ports_added':False}
    (backup/'result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
except Exception:
    # Never restore the old database/0.2.0 code after media externalization. Keep the new ledger and objects.
    # Remove the ingress port binding and expose a 0.3.0 single-node HTTP recovery configuration.
    try:run(old_compose+['stop','gateway'])
    except Exception:pass
    try:run(new_compose+['stop','ingress'])
    except Exception:pass
    recovery=json.loads(run(['docker','compose','-f',str(root/'deploy/scale.compose.yaml'),'config','--format','json']))
    gateway=recovery['services']['gateway-a']
    gateway['ports']=[{'target':8080,'published':'31080','host_ip':'127.0.0.1','protocol':'tcp'}]
    gateway['environment']['RPA_GATEWAY_MEDIA_PROXY']='1'
    recovery['services']={'gateway-a':gateway}
    path=backup/'recovery.compose.json';path.write_text(json.dumps(recovery));path.chmod(0o600)
    run(['docker','compose','-f',str(path),'up','-d','--no-deps','gateway-a'])
    print(json.dumps({'deployed':False,'single_node_v3_recovery':True,'database_restored':False,'backup':str(backup)}))
    raise SystemExit(1)
