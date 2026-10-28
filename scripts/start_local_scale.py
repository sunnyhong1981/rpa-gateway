"""Start two real ASGI instances behind a local Nginx TLS proxy. All state is local test state."""
import json
import os
import secrets
import subprocess
import time
from urllib.request import urlopen
from pathlib import Path

root=Path(__file__).resolve().parents[1]
state=root/'runtime/scale-local'
state.mkdir(exist_ok=True)
config=state/'processes.json'
if config.exists():raise RuntimeError('Local scale process list already exists; stop the owned processes first')
pg_url=(state/'test_database_url').read_text().strip()
settings={'RPA_GATEWAY_DATABASE_URL':pg_url,'RPA_GATEWAY_REDIS_URL':'redis://127.0.0.1:36379/0',
          'RPA_GATEWAY_REDIS_NAMESPACE':'local_scale', 'RPA_GATEWAY_OBJECT_CONFIG':(state/'objects.json').read_text(),
          'RPA_GATEWAY_MEDIA_PROXY':'1'}
for role in ('ADMIN','BACKEND','VERIFIER'):
    path=state/(role.lower()+'_token')
    if not path.exists():path.write_text(secrets.token_urlsafe(48));path.chmod(0o600)
    settings['RPA_GATEWAY_'+role+'_TOKEN']=path.read_text().strip()
if not (state/'cert.pem').exists():
    subprocess.run(['/opt/homebrew/bin/openssl','req','-x509','-newkey','rsa:2048','-nodes',
        '-keyout',str(state/'key.pem'),'-out',str(state/'cert.pem'),'-days','7','-subj','/CN=localhost',
        '-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(state/'key.pem').chmod(0o600)
# Match production routing but use independently started local services.
nginx=(root/'deploy/nginx-scale.conf').read_text().replace('/etc/nginx/mime.types','/opt/homebrew/etc/nginx/mime.types')
nginx=nginx.replace('/run/tls/',str(state)+'/').replace('gateway-a:8080','127.0.0.1:38081').replace('gateway-b:8080','127.0.0.1:38082')
nginx=nginx.replace('media:8080','127.0.0.1:38083').replace('objects:9000','127.0.0.1:39000')
nginx=nginx.replace('listen 8080;','listen 127.0.0.1:38080;').replace('listen 8443 ssl;','listen 127.0.0.1:38443 ssl;').replace('listen 8444 ssl;','listen 127.0.0.1:38444 ssl;')
nginx=nginx.replace('/tmp/nginx.pid',str(state/'nginx.pid')).replace('/tmp/client',str(state/'client-temp')).replace('/tmp/proxy',str(state/'proxy-temp'))
nginx=nginx.replace('worker_processes auto','worker_processes 2')
(state/'nginx.conf').write_text(nginx)
subprocess.run(['/opt/homebrew/opt/nginx/bin/nginx','-t','-c',str(state/'nginx.conf')],check=True)
processes=[]
try:
    for name,port,kind in [('gateway-a',38081,'rpa_gateway.app:create_app'),('gateway-b',38082,'rpa_gateway.app:create_app'),('media',38083,'rpa_gateway.app:create_media_app')]:
        environment={**os.environ,**settings,'RPA_GATEWAY_INSTANCE_ID':name}
        environment['RPA_GATEWAY_MEDIA_PROXY']='1' if name=='media' else '0'
        log=(state/(name+'.log')).open('w')
        command=[str(root/'.venv/bin/uvicorn'),kind,'--factory','--host','127.0.0.1','--port',str(port),'--no-access-log','--limit-concurrency','4096','--ws-max-size','4096','--ws-max-queue','4']
        process=subprocess.Popen(command,cwd=root,env=environment,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        processes.append({'name':name,'pid':process.pid,'command':command})
    log=(state/'worker.log').open('w')
    command=[str(root/'.venv/bin/python'),'-m','rpa_gateway.worker']
    process=subprocess.Popen(command,cwd=root,env={**os.environ,**settings},stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    processes.append({'name':'worker','pid':process.pid,'command':command})
    for port in (38081,38082,38083):
        until=time.monotonic()+15
        while True:
            try:
                with urlopen('http://127.0.0.1:'+str(port)+'/healthz',timeout=1) as response:assert response.status==200
                break
            except Exception:
                if time.monotonic()>until:raise RuntimeError('Local gateway readiness timeout')
                time.sleep(.2)
    subprocess.run(['/opt/homebrew/opt/nginx/bin/nginx','-c',str(state/'nginx.conf')],check=True)
    config.write_text(json.dumps(processes,indent=2));config.chmod(0o600)
    print(json.dumps({'started':[p['name'] for p in processes],'http':'http://127.0.0.1:38080','tls':'https://127.0.0.1:38443','production_connections':0}))
except Exception:
    for item in processes:
        try:os.kill(item['pid'],15)
        except OSError:pass
    raise
