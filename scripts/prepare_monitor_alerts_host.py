"""Private existing AgentBot API credential, scoped by code to the existing monitor conversation."""
import json,os,subprocess
from pathlib import Path
root=Path('/opt/ai/rpa-gateway')
def sql(statement):
 p=subprocess.run(['docker','exec','-i','chatwoot-postgres-1','psql','-U','postgres','-d','chatwoot','-At','-v','ON_ERROR_STOP=1'],input=statement,text=True,capture_output=True)
 if p.returncode:raise RuntimeError('Monitor transport provisioning failed; inspect privately')
 return p.stdout.strip()
token=sql("SELECT token FROM access_tokens WHERE owner_type='AgentBot' AND owner_id=31 ORDER BY id LIMIT 1")
if not token:raise RuntimeError('Existing logistics AgentBot API credential unavailable')
path=root/'runtime/secrets/monitor_alert_config'
if not path.exists():
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as stream:json.dump({'base_url':'http://chatwoot-rails-1:3000','host_header':'uat4.qunl.com','api_token':token,
   'account_id':871,'inbox_id':491,'conversation_id':2090,'group_name':'RPA漏回监控群'},stream,ensure_ascii=False)
 os.chown(path,10001,10001)
webhook=root/'runtime/secrets/wecom_webhook'
if not webhook.exists():
 fd=os.open(webhook,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd);os.chown(webhook,10001,10001)
sql("""CREATE OR REPLACE VIEW rpa_gateway_source.monitor_alert_messages WITH (security_barrier=true) AS
 SELECT id,content,created_at,CASE WHEN jsonb_typeof(content_attributes::jsonb)='string' THEN (content_attributes::jsonb #>> '{}')::jsonb ELSE content_attributes::jsonb END attributes
 FROM messages WHERE account_id=871 AND inbox_id=491 AND conversation_id=2090 AND message_type=1 AND private=false;
 GRANT SELECT ON rpa_gateway_source.monitor_alert_messages TO rpa_gateway_source;""")
for name in ['alertmanager']:
 p=root/'runtime'/name;p.mkdir(exist_ok=True,mode=0o700);os.chown(p,10001,10001)
print(json.dumps({'prepared':True,'channel':'existing_rpa_monitor','inbox_id':491,'conversation_id':2090,'group':'RPA漏回监控群','credential_printed':False}))
