"""Create narrowly scoped, read-only views for the authorized RPA test inbox only."""
import json,os,secrets,subprocess
from pathlib import Path
root=Path('/opt/ai/rpa-gateway')

def sql(database,statement):
    result=subprocess.run(['docker','exec','-i','chatwoot-postgres-1','psql','-U','postgres','-d',database,'-At','-v','ON_ERROR_STOP=1'],input=statement,text=True,capture_output=True)
    if result.returncode:raise RuntimeError('Scoped source provisioning failed; inspect privately')
    return result.stdout.strip()

role='rpa_gateway_source'
path=root/'runtime/secrets/source_database_url'
if not path.exists():
    if sql('postgres',f"SELECT count(*) FROM pg_roles WHERE rolname='{role}'")!='0':raise RuntimeError('Role already exists without a saved credential')
    password=secrets.token_urlsafe(48)
    sql('postgres',f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 2;")
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as stream:stream.write('postgresql://'+role+':'+password+'@gateway-postgres:5432/chatwoot\n')
    os.chown(path,10001,10001)
views="""
CREATE SCHEMA IF NOT EXISTS rpa_gateway_source;
REVOKE ALL ON SCHEMA rpa_gateway_source FROM PUBLIC;
CREATE OR REPLACE VIEW rpa_gateway_source.test_messages WITH (security_barrier=true) AS
 SELECT m.id,m.account_id,m.inbox_id,m.conversation_id,m.message_type,m.private,m.content,m.created_at,
 CASE WHEN jsonb_typeof(m.content_attributes::jsonb)='string' THEN (m.content_attributes::jsonb #>> '{}')::jsonb ELSE m.content_attributes::jsonb END attributes,
 COALESCE((SELECT jsonb_agg(jsonb_build_object('id',a.id,'file_type',CASE WHEN a.file_type=0 THEN 'image' ELSE 'file' END,'filename',b.filename,'size',b.byte_size,'md5',encode(decode(b.checksum,'base64'),'hex')) ORDER BY a.id)
 FROM attachments a JOIN active_storage_attachments asa ON asa.record_type='Attachment' AND asa.record_id=a.id AND asa.name='file'
 JOIN active_storage_blobs b ON b.id=asa.blob_id WHERE a.message_id=m.id),'[]'::jsonb) attachments
 FROM messages m WHERE m.account_id=871 AND m.inbox_id=469 AND m.conversation_id=1370;
CREATE OR REPLACE VIEW rpa_gateway_source.test_archive WITH (security_barrier=true) AS
 SELECT e.id,e.msgid,e.seq,e.room_id,e.sender_id,EXTRACT(EPOCH FROM e.occurred_at)::double precision occurred_at,e.payload
 FROM rpa_archive_events e JOIN rpa_archive_streams s ON s.id=e.rpa_archive_stream_id
 WHERE s.account_id=871 AND e.room_id='wr93kQSAAARbMiA4ywr4_FJwO4l3jVjg' AND e.sender_id='hongbo';
GRANT CONNECT ON DATABASE chatwoot TO rpa_gateway_source;
GRANT USAGE ON SCHEMA rpa_gateway_source TO rpa_gateway_source;
GRANT SELECT ON rpa_gateway_source.test_messages,rpa_gateway_source.test_archive TO rpa_gateway_source;
ALTER ROLE rpa_gateway_source SET default_transaction_read_only=on;
ALTER ROLE rpa_gateway_source SET statement_timeout='3s';
ALTER ROLE rpa_gateway_source SET lock_timeout='1s';
"""
sql('chatwoot',views)
print(json.dumps({'prepared':True,'read_only_role':role,'inbox':469,'parent_conversation':1370,'views':2,'base_table_access_granted':False,'postgres_restarted':False}))
