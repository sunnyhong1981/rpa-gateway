"""Root-only provisioning on the Docker host. No Chatwoot data/config changes."""
import json
import os
import secrets
import subprocess
from pathlib import Path

root=Path('/opt/ai/rpa-gateway')
secret_dir=root/'runtime/secrets'
secret_dir.mkdir(exist_ok=True,mode=0o700)

def sql(database,statement):
    result=subprocess.run(['docker','exec','-i','chatwoot-postgres-1','psql','-U','postgres','-d',database,
                           '-At','-v','ON_ERROR_STOP=1'],input=statement,text=True,capture_output=True)
    if result.returncode:
        # Server diagnostics can repeat SQL containing a password. Never print them.
        raise RuntimeError('PostgreSQL provisioning command failed; inspect privately on host')
    return result.stdout.strip()

rows=[json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines()]
(root/'postgres-before-services.json').write_text(json.dumps({r['Names']:r['ID'] for r in rows},indent=2))
for role,database,filename in [('rpa_gateway','rpa_gateway','database_url'),('rpa_gateway_test','rpa_gateway_test','test_database_url')]:
    path=secret_dir/filename
    role_exists=sql('postgres',f"SELECT count(*) FROM pg_roles WHERE rolname='{role}';")=='1'
    if path.exists():
        if not role_exists:
            raise RuntimeError('Existing credential has no matching role; refusing implicit replacement')
    else:
        if role_exists:
            raise RuntimeError('Existing role has no gateway credential; refusing to change its password')
        password=secrets.token_urlsafe(48)
        sql('postgres',f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 16;")
        text='postgresql://'+role+':'+password+'@gateway-postgres:5432/'+database+'?connect_timeout=5'
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:f.write(text+'\n')
        os.chown(path,10001,10001)
    sql('postgres',f'ALTER ROLE {role} SET synchronous_commit TO on;')
    db_exists=sql('postgres',f"SELECT count(*) FROM pg_database WHERE datname='{database}';")=='1'
    if not db_exists:
        sql('postgres',f'CREATE DATABASE {database} OWNER {role} TEMPLATE template0;')
    if sql('postgres',f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='{database}';")!=role:
        raise RuntimeError('Unexpected database owner')
    sql('postgres',f'REVOKE ALL ON DATABASE {database} FROM PUBLIC; GRANT CONNECT ON DATABASE {database} TO {role};')
    sql(database,f'REVOKE ALL ON SCHEMA public FROM PUBLIC; GRANT ALL ON SCHEMA public TO {role};')

network=subprocess.run(['docker','network','inspect','qunl-rpa-gateway_isolated'],capture_output=True,text=True)
if network.returncode:
    subprocess.run(['docker','network','create','--driver','bridge','--label','com.docker.compose.project=qunl-rpa-gateway',
                    '--label','com.docker.compose.network=isolated','qunl-rpa-gateway_isolated'],check=True,stdout=subprocess.DEVNULL)
else:
    owner=json.loads(network.stdout)[0].get('Labels',{}).get('com.docker.compose.project')
    if owner!='qunl-rpa-gateway':
        raise RuntimeError('Gateway network belongs to another project')
inspect=json.loads(subprocess.check_output(['docker','inspect','chatwoot-postgres-1']))[0]
if 'qunl-rpa-gateway_isolated' not in inspect['NetworkSettings']['Networks']:
    subprocess.run(['docker','network','connect','--alias','gateway-postgres','qunl-rpa-gateway_isolated','chatwoot-postgres-1'],check=True)
# Attaching a separate network interface does not restart PostgreSQL or change its existing network.
print(json.dumps({'provisioned':True,'postgres_container':inspect['Id'][:12],
 'databases':['rpa_gateway','rpa_gateway_test'],'accounts':['rpa_gateway','rpa_gateway_test'],
 'role_connection_limit':16,'postgres_restarted':False,'credentials_printed':False}))
