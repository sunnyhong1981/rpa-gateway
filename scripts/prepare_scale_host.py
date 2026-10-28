"""Prepare isolated gateway scale secrets/resources; never alter Chatwoot configuration."""
import json
import os
import secrets
import subprocess
from pathlib import Path
root=Path('/opt/ai/rpa-gateway')
secrets_dir=root/'runtime/secrets'

def private(name,value,owner=10001):
    path=secrets_dir/name
    if not path.exists():
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as stream:stream.write(value+'\n')
        os.chown(path,owner,owner)
    return path.read_text().strip()

password=private('redis_password',secrets.token_urlsafe(48))
private('redis_url','redis://:'+password+'@redis:6379/0')
private('redis.conf','\n'.join(['bind 0.0.0.0','port 6379','protected-mode yes','requirepass '+password,
    'save ""','appendonly no','maxmemory 32mb','maxmemory-policy noeviction','loglevel warning']))
user=private('object_user','gateway_'+secrets.token_hex(8))
secret=private('object_password',secrets.token_urlsafe(48))
private('object_identity',json.dumps({'identities':[{'name':'rpa-gateway','credentials':[{'accessKey':user,'secretKey':secret}],'actions':['Admin','Read','Write','List','Tagging']}]}))
private('object_config',json.dumps({'endpoint':'http://objects:9000','public_endpoint':'https://127.0.0.1:31444',
    'bucket':'rpa-gateway-originals','prefix':'gateway-v3','region':'us-east-1','access_key':user,'secret_key':secret}))
for name in ['objects','prometheus']:
    path=root/'runtime'/name
    path.mkdir(exist_ok=True,mode=0o700);os.chown(path,10001,10001)
tls=root/'runtime/tls'
tls.mkdir(exist_ok=True,mode=0o700)
if not (tls/'key.pem').exists():
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(tls/'key.pem'),'-out',str(tls/'cert.pem'),
        '-days','30','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(tls/'key.pem').chmod(0o600)
# Bounded pools: 4+4+4+2 = 14. Existing role cap 16 remains unchanged.
info=json.loads(subprocess.check_output(['docker','inspect','qunl-rpa-gateway-gateway-1']))[0]
rows=[json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines()]
(root/'scale-before-services.json').write_text(json.dumps({row['Names']:row['ID'] for row in rows},indent=2))
print(json.dumps({'prepared':True,'existing_gateway_image':info['Image'],'public_ports_added':False,'role_connection_cap_unchanged':16,'secrets_printed':False}))
