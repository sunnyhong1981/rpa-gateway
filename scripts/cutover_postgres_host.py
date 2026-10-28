"""Replace only the isolated gateway after tests pass; preserve SQLite and old code."""
import json
import shutil
import sqlite3
import subprocess
import tarfile
import time
from pathlib import Path
from urllib.request import urlopen

root=Path('/opt/ai/rpa-gateway')
candidate=Path('/opt/ai/rpa-gateway-postgres-candidate-20261028')
backup=root/'runtime/migrations'/time.strftime('postgres-%Y%m%d-%H%M%S',time.gmtime())
backup.mkdir(parents=True,mode=0o700)
compose=['docker','compose','-f',str(root/'deploy/compose.yaml')]

def run(command,**kwargs):
    result=subprocess.run(command,text=True,capture_output=True,timeout=90,**kwargs)
    if result.returncode:
        raise RuntimeError('Gateway deployment command failed; private command output retained by the host administrator')
    return result.stdout

def health():
    with urlopen('http://127.0.0.1:31080/healthz',timeout=3) as response:return json.load(response)

old=health()
if old['version']!='0.1.0' or old.get('storage')=='postgresql':
    raise RuntimeError('Unexpected active version; refusing an implicit second cutover')
with sqlite3.connect('file:'+str(root/'runtime/data/gateway.sqlite3')+'?mode=ro',uri=True) as db:
    if db.execute('SELECT count(*) FROM devices WHERE revoked=0').fetchone()[0]:
        raise RuntimeError('Live gateway devices are configured; an idle migration is required')
tracked=['rpa_gateway','scripts','tests','deploy','Dockerfile','.dockerignore','.gitignore','pyproject.toml',
         'requirements.txt','requirements-dev.txt','README.md','API.md','INTEGRATION.md','CAPACITY-1000.md']
with tarfile.open(backup/'previous-source.tar.gz','w:gz') as archive:
    for name in tracked:
        path=root/name
        if path.exists():archive.add(path,arcname=name,filter=lambda entry:None if '__pycache__' in entry.name else entry)
start=time.monotonic()
stopped=False
try:
    run(compose+['stop','gateway']);stopped=True
    snapshot=backup/'sqlite-before-postgres.sqlite3'
    with sqlite3.connect('file:'+str(root/'runtime/data/gateway.sqlite3')+'?mode=ro',uri=True) as source:
        with sqlite3.connect(snapshot) as target:
            source.backup(target)
            target.execute('PRAGMA journal_mode=DELETE')
    snapshot.chmod(0o444) # private parent is root-only; container receives only the explicit file mount
    command=['docker','run','--rm','-i','--network','qunl-rpa-gateway_isolated','--cpus','0.5','--memory','384m',
             '--read-only','--tmpfs','/tmp:size=16m,mode=1777']
    for name in ('admin_token','backend_token','verifier_token','database_url'):
        command+=['--mount','type=bind,src='+str(root/'runtime/secrets'/name)+',dst=/run/secrets/'+name+',readonly',
                  '-e','RPA_GATEWAY_'+name.upper()+'_FILE=/run/secrets/'+name]
    command+=['--mount','type=bind,src='+str(snapshot)+',dst=/migration/source.sqlite3,readonly',
              'qunl-rpa-gateway:0.2.0','python','-','--source','/migration/source.sqlite3']
    migration=json.loads(run(command,input=(candidate/'scripts/migrate_sqlite_to_postgres.py').read_text()))
    (backup/'migration-result.json').write_text(json.dumps(migration,indent=2))
    for name in tracked:
        source=candidate/name
        if source.is_dir():shutil.copytree(source,root/name,dirs_exist_ok=True)
        elif source.is_file():shutil.copy2(source,root/name)
    run(compose+['up','-d','--no-deps','gateway'])
    ready=None
    end=time.monotonic()+45
    while time.monotonic()<end:
        try:
            ready=health()
            if ready.get('version')=='0.2.0' and ready.get('storage')=='postgresql':break
        except Exception:pass
        time.sleep(1)
    else:raise RuntimeError('PostgreSQL gateway readiness deadline exceeded')
    result={'switched':True,'gateway_pause_seconds':round(time.monotonic()-start,3),
            'backup_directory':str(backup),'migration':migration,'health':ready,'formal_rpa_clients_changed':0}
    (backup/'cutover-result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))
except Exception as exc:
    if stopped:
        # No real gateway clients are configured. Roll back only this isolated service.
        with tarfile.open(backup/'previous-source.tar.gz') as archive:archive.extractall(root,filter='data')
        try:run(compose+['up','-d','--no-deps','gateway'])
        except Exception:pass
    failure={'switched':False,'rollback_attempted':stopped,'reason_type':type(exc).__name__,'backup_directory':str(backup)}
    (backup/'cutover-failure.json').write_text(json.dumps(failure,indent=2))
    print(json.dumps(failure))
    raise SystemExit(1) from None
