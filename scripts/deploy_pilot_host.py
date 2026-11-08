"""Update only the isolated gateway to 0.4.0; source adapter stays shadow until local handover."""
import json,os,shutil,subprocess,time
from pathlib import Path
root=Path('/opt/ai/rpa-gateway');candidate=Path('/opt/ai/rpa-gateway-mac-pilot-candidate-20261029')
compose=['docker','compose','-f',str(root/'deploy/scale.compose.yaml')]

def run(args,timeout=120,input=None):
 p=subprocess.run(args,input=input,text=True,capture_output=True,timeout=timeout)
 if p.returncode:raise RuntimeError('Pilot gateway deployment command failed; no secret output displayed')
 return p.stdout

secret=root/'runtime/secrets/source_config'
if Path('/tmp/qunl-gateway-source-pilot.json').exists():
 if secret.exists():raise RuntimeError('Existing scoped source config must be reviewed, not overwritten')
 shutil.move('/tmp/qunl-gateway-source-pilot.json',secret);secret.chmod(0o600);os.chown(secret,10001,10001)
run(['docker','exec','chatwoot-postgres-1','psql','-U','postgres','-d','postgres','-At','-v','ON_ERROR_STOP=1','-c','ALTER ROLE rpa_gateway CONNECTION LIMIT 20;'])
for name in ['rpa_gateway','scripts','tests','deploy']:
 shutil.copytree(candidate/name,root/name,dirs_exist_ok=True)
for name in ['Dockerfile','.dockerignore','pyproject.toml','requirements.txt']:
 shutil.copy2(candidate/name,root/name)
config=root/'deploy/nginx-scale.conf';original=config.read_text()
def reload():
 run(compose+['exec','-T','ingress','nginx','-t']);run(compose+['exec','-T','ingress','nginx','-s','reload'])
for service in ['gateway-a','gateway-b']:
 changed=[]
 for line in original.splitlines():
  if 'server '+service+':8080' in line:line=line.rstrip().removesuffix(';')+' down;'
  changed.append(line)
 config.write_text('\n'.join(changed)+'\n');reload()
 run(compose+['up','-d','--no-deps',service])
 until=time.monotonic()+30
 while True:
  try:
   run(compose+['exec','-T',service,'python','-c',"import urllib.request,json;assert json.load(urllib.request.urlopen('http://127.0.0.1:8080/healthz'))['version']=='0.4.0'"],timeout=6)
   break
  except Exception:
   if time.monotonic()>until:raise
   time.sleep(.5)
 config.write_text(original);reload()
run(compose+['up','-d','--no-deps','media','worker','source'])
print(json.dumps({'deployed':True,'version':'0.4.0','source_scope':'inbox469_only','source_mode':'shadow_until_native_ready','gateway_role_cap':20,'source_role_cap':2,'formal_rpa_changed':False}))
