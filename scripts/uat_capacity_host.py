"""Dedicated sandbox on the user-selected uat host; cached dependencies, tmpfs data, CPU budgets."""
import json,os,secrets,subprocess,shutil
from pathlib import Path
root=Path('/opt/ai/rpa-gateway-capacity-20261029')
if root.exists():raise RuntimeError('Existing capacity directory must be reviewed; refusing overwrite')
if shutil.disk_usage('/').free<1500*1024*1024:raise RuntimeError('Insufficient disk reserve; no sandbox started')
root.mkdir(parents=True,mode=0o700);(root/'runtime/secrets').mkdir(parents=True,mode=0o700);(root/'runtime/tls').mkdir(mode=0o700)
(root/'deploy').mkdir()
def private(name,text):
 p=root/'runtime/secrets'/name;p.write_text(text);p.chmod(0o600);os.chown(p,10001,10001)
 return text
roles={name:private(name,secrets.token_urlsafe(48)) for name in ['admin_token','backend_token','verifier_token','pg_password','redis_password','object_user','object_password']}
private('database_url','postgresql://gateway_lab:'+roles['pg_password']+'@postgres:5432/rpa_gateway_test')
private('redis_url','redis://:'+roles['redis_password']+'@redis:6379/0')
private('redis.conf','\n'.join(['bind 0.0.0.0','port 6379','requirepass '+roles['redis_password'],'save ""','appendonly no','maxmemory 32mb','maxmemory-policy noeviction']))
private('object_config',json.dumps({'endpoint':'http://objects:9000','public_endpoint':'https://127.0.0.1:39444','bucket':'capacity-private','prefix':'lab',
 'region':'us-east-1','access_key':roles['object_user'],'secret_key':roles['object_password']}))
subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(root/'runtime/tls/key.pem'),'-out',str(root/'runtime/tls/cert.pem'),
 '-days','7','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(root/'runtime/tls/key.pem').chmod(0o600)
images={}
for name,key in [('backend-postgres-1','postgres'),('backend-minio-1','objects'),('backend-redis-1','redis'),('backend-frontend-1','nginx')]:
 images[key]=json.loads(subprocess.check_output(['docker','inspect',name]))[0]['Image']
role_secrets=['database_url','admin_token','backend_token','verifier_token','redis_url','object_config']
env={ 'RPA_GATEWAY_DATABASE_URL_FILE':'/run/secrets/database_url','RPA_GATEWAY_ADMIN_TOKEN_FILE':'/run/secrets/admin_token',
 'RPA_GATEWAY_BACKEND_TOKEN_FILE':'/run/secrets/backend_token','RPA_GATEWAY_VERIFIER_TOKEN_FILE':'/run/secrets/verifier_token',
 'RPA_GATEWAY_REDIS_URL_FILE':'/run/secrets/redis_url','RPA_GATEWAY_OBJECT_CONFIG_FILE':'/run/secrets/object_config',
 'RPA_GATEWAY_REDIS_NAMESPACE':'capacity-uat','RPA_GATEWAY_MEDIA_PROXY':'0'}
base={'image':'qunl-rpa-gateway:0.4.0','init':True,'read_only':True,'user':'10001:10001','restart':'no',
      'cpus':.25,'mem_limit':'256m','pids_limit':96,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],
      'tmpfs':['/tmp:size=32m,mode=1777'],'networks':['lab'],'secrets':role_secrets,'environment':env,
      'logging':{'driver':'json-file','options':{'max-size':'2m','max-file':'2'}}}
services={}
for name in ['gateway-a','gateway-b','media','worker']:
 value=json.loads(json.dumps(base));value['environment']['RPA_GATEWAY_INSTANCE_ID']=name
 if name=='media':value['command']=['uvicorn','rpa_gateway.app:create_media_app','--factory','--host','0.0.0.0','--port','8080','--no-access-log','--limit-concurrency','8'];value['cpus']=.15
 if name=='worker':value['command']=['python','-m','rpa_gateway.worker'];value['cpus']=.1;value['mem_limit']='192m'
 services[name]=value
services['postgres']={'image':images['postgres'],'restart':'no','cpus':.25,'mem_limit':'768m',
 'environment':{'POSTGRES_USER':'gateway_lab','POSTGRES_DB':'rpa_gateway_test','POSTGRES_PASSWORD_FILE':'/run/secrets/pg_password'},
 'secrets':['pg_password'],'tmpfs':['/var/lib/postgresql/data:size=512m,mode=0700'],'networks':['lab'],
 'command':['postgres','-c','max_connections=32','-c','shared_buffers=64MB','-c','max_wal_size=128MB']}
services['redis']={'image':images['redis'],'user':'10001:10001','command':['redis-server','/run/secrets/redis_conf'],
 'secrets':['redis_conf'],'tmpfs':['/data:size=16m'],'cpus':.05,'mem_limit':'64m','networks':['lab']}
services['objects']={'image':images['objects'],'user':'10001:10001','command':['server','/data','--console-address',':9001','--certs-dir','/tmp/certs'],
 'environment':{'MINIO_ROOT_USER_FILE':'/run/secrets/object_user','MINIO_ROOT_PASSWORD_FILE':'/run/secrets/object_password'},
 'secrets':['object_user','object_password'],'tmpfs':['/data:size=64m,uid=10001,gid=10001','/tmp:size=8m,mode=1777'],'cpus':.1,'mem_limit':'256m','networks':['lab']}
services['ingress']={'image':images['nginx'],'ports':['127.0.0.1:33443:8443','127.0.0.1:33444:8444'],
 'volumes':['./nginx.conf:/etc/nginx/nginx.conf:ro','./runtime/tls:/run/tls:ro'],
 'tmpfs':['/tmp:size=16m','/var/cache/nginx:size=8m','/var/run:size=1m'],'read_only':True,'cpus':.1,'mem_limit':'128m','networks':['lab'],
 'ulimits':{'nofile':{'soft':16384,'hard':16384}}}
config={'name':'qunl-rpa-capacity-lab','services':services,'networks':{'lab':{'internal':False}},
 'secrets':{name:{'file':'./runtime/secrets/'+name} for name in role_secrets+['pg_password','object_user','object_password']}}
config['secrets']['redis_conf']={'file':'./runtime/secrets/redis.conf'}
(root/'compose.json').write_text(json.dumps(config,indent=2))
rows=[json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines()]
(root/'before-services.json').write_text(json.dumps({row['Names']:row['ID'] for row in rows},indent=2))
(root/'cached-images.json').write_text(json.dumps(images,indent=2))
print(json.dumps({'prepared':True,'directory':str(root),'scope':'independent_database_and_tmpfs','existing_services_modified':False,'disk_reserve_bytes':shutil.disk_usage('/').free}))
