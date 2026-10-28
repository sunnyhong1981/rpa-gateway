"""Verify the real frozen backup under the runtime container UID in a temporary test schema."""
import json
import sqlite3
import subprocess
from pathlib import Path
root=Path('/opt/ai/rpa-gateway')
paths=sorted((root/'runtime/migrations').glob('postgres-*/sqlite-before-postgres.sqlite3'))
if not paths:raise RuntimeError('No frozen migration backup exists')
source=paths[-1]
# Make the offline backup a single-file snapshot, without WAL/SHM dependencies.
with sqlite3.connect(source) as db:
    db.execute('PRAGMA journal_mode=DELETE')
source.chmod(0o444)
script='''
import json,uuid
from pathlib import Path
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo,conninfo_to_dict
from rpa_gateway.database import Database
from rpa_gateway.migration import import_sqlite
secret=Path('/run/secrets/test_database_url').read_text().strip()
assert conninfo_to_dict(secret)['dbname']=='rpa_gateway_test'
schema='preflight_'+uuid.uuid4().hex
with psycopg.connect(secret,autocommit=True) as admin:
 admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
 database=None
 try:
  database=Database(make_conninfo(secret,options='-c search_path='+schema),pool_size=1)
  database.initialize()
  result=import_sqlite(database,'/migration/source.sqlite3')
  assert result['content_verified']
  print(json.dumps({'preflight_passed':True,'real_backup':True,'runtime_uid':10001,'tables':result['tables']}))
 finally:
  if database:database.close()
  admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
'''
command=['docker','run','--rm','-i','--network','qunl-rpa-gateway_isolated','--cpus','0.5','--memory','384m','--read-only',
 '--tmpfs','/tmp:size=16m,mode=1777','--mount','type=bind,src='+str(source)+',dst=/migration/source.sqlite3,readonly',
 '--mount','type=bind,src='+str(root/'runtime/secrets/test_database_url')+',dst=/run/secrets/test_database_url,readonly',
 'qunl-rpa-gateway:0.2.0','python','-']
result=subprocess.run(command,input=script,text=True,capture_output=True,timeout=60)
if result.returncode:
    print(json.dumps({'preflight_passed':False,'reason':'Migration preflight failed'}))
    raise SystemExit(1)
print(result.stdout,end='')
