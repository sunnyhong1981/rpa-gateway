"""Update only v3 gateway components, draining one ingress upstream at a time."""
import json
import shutil
import subprocess
import time
from pathlib import Path
root=Path('/opt/ai/rpa-gateway');candidate=Path('/opt/ai/rpa-gateway-scale-candidate-20261029')
compose=['docker','compose','-f',str(root/'deploy/scale.compose.yaml')]

def run(command,timeout=90):
    result=subprocess.run(command,text=True,capture_output=True,timeout=timeout)
    if result.returncode:raise RuntimeError('Owned rolling update command failed')
    return result.stdout

for name in ['rpa_gateway','scripts','tests','deploy']:
    shutil.copytree(candidate/name,root/name,dirs_exist_ok=True)
for name in ['Dockerfile','.dockerignore','pyproject.toml','requirements.txt','README.md','API.md','INTEGRATION.md','CAPACITY-1000.md','SCALE-OPERATIONS.md']:
    if (candidate/name).exists():shutil.copy2(candidate/name,root/name)
config_path=root/'deploy/nginx-scale.conf';original=config_path.read_text()

def reload_nginx():
    run(compose+['exec','-T','ingress','nginx','-t'])
    run(compose+['exec','-T','ingress','nginx','-s','reload'])

reload_nginx()
updated=[]
for service in ['gateway-a','gateway-b']:
    lines=[]
    for line in original.splitlines():
        if 'server '+service+':8080' in line:line=line.rstrip().removesuffix(';')+' down;'
        lines.append(line)
    config_path.write_text('\n'.join(lines)+'\n');reload_nginx()
    run(compose+['up','-d','--no-deps',service])
    end=time.monotonic()+30
    while time.monotonic()<end:
        try:
            run(compose+['exec','-T',service,'python','-c',"import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=3)"],timeout=6)
            break
        except Exception:time.sleep(.5)
    else:raise RuntimeError('Updated instance is not ready; other instance remains active')
    config_path.write_text(original);reload_nginx();updated.append(service)
run(compose+['up','-d','--no-deps','media','worker'])
# Reload new rule files without restarting Prometheus or opening an unauthenticated lifecycle endpoint.
run(['docker','kill','--signal=HUP','qunl-rpa-gateway-scale-prometheus-1'])
print(json.dumps({'updated':updated+['media','worker'],'one_gateway_available_during_update':True,'database_restored':False,'formal_clients_changed':0}))
