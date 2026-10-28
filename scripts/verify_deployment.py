"""Read-only host verification for gateway 0.2.0; never output credentials."""
import hashlib
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

root=Path('/opt/ai/rpa-gateway')
inspect=json.loads(subprocess.check_output(['docker','inspect','qunl-rpa-gateway-gateway-1']))[0]
pg=json.loads(subprocess.check_output(['docker','inspect','chatwoot-postgres-1']))[0]
current={r['Names']:r['ID'] for r in (json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines())}
before_path=root/'postgres-before-services.json'
before=json.loads(before_path.read_text())
rails=json.loads(subprocess.check_output(['docker','inspect','chatwoot-rails-1']))[0]
unchanged_started={name:datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()<before_path.stat().st_mtime
  for name,value in {'postgres':pg['State']['StartedAt'],'chatwoot_rails':rails['State']['StartedAt']}.items()}
code="from pathlib import Path;import hashlib,json;print(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path('rpa_gateway').glob('*.py')) if not p.name.startswith('._')}))"
image_hashes=json.loads(subprocess.check_output(['docker','exec','qunl-rpa-gateway-gateway-1','python','-c',code],text=True))
changed={name:{'before':container,'after':current.get(name)} for name,container in before.items()
         if name!='qunl-rpa-gateway-gateway-1' and current.get(name)!=container}
with urlopen('http://127.0.0.1:31080/healthz',timeout=5) as response:health=json.load(response)

def query(database,statement):
    return json.loads(subprocess.check_output(['docker','exec','chatwoot-postgres-1','psql','-U','postgres','-d',database,
        '-At','-v','ON_ERROR_STOP=1','-c',statement],text=True))
summary=query('rpa_gateway',"""SELECT json_build_object(
 'database',current_database(),
 'tasks',(SELECT COALESCE(json_object_agg(state,n),'{}'::json) FROM (SELECT state,count(*) n FROM tasks GROUP BY state) t),
 'only_synthetic_tenants',(SELECT COALESCE(bool_and(tenant LIKE 'synthetic_%' OR tenant LIKE 'restart_%' OR tenant LIKE 'ws_%'),true) FROM tasks),
 'active_device_credentials',(SELECT count(*) FROM devices WHERE revoked=0),
 'counts',json_build_object('devices',(SELECT count(*) FROM devices),'media',(SELECT count(*) FROM media),
   'tasks',(SELECT count(*) FROM tasks),'events',(SELECT count(*) FROM events),'archive_claims',(SELECT count(*) FROM archive_claims)),
 'imports',(SELECT count(*) FROM gateway_imports),
 'one_active_task_constraint',EXISTS(SELECT 1 FROM pg_indexes WHERE indexname='one_active_task_per_device'),
 'gateway_connections',(SELECT count(*) FROM pg_stat_activity WHERE usename='rpa_gateway'),
 'gateway_role_connection_limit',(SELECT rolconnlimit FROM pg_roles WHERE rolname='rpa_gateway'),
 'gateway_role_superuser',(SELECT rolsuper FROM pg_roles WHERE rolname='rpa_gateway'));
""")
privileges=query('chatwoot',"""SELECT json_build_object(
 'chatwoot_messages_select',has_table_privilege('rpa_gateway','public.messages','SELECT'),
 'chatwoot_messages_insert',has_table_privilege('rpa_gateway','public.messages','INSERT'),
 'chatwoot_messages_update',has_table_privilege('rpa_gateway','public.messages','UPDATE'),
 'chatwoot_public_schema_create',has_schema_privilege('rpa_gateway','public','CREATE'));
""")
result={'health':health,'container_id':inspect['Id'][:12],'image':inspect['Image'],
 'container_health':inspect['State']['Health']['Status'],'port_bindings':inspect['HostConfig']['PortBindings'],
 'network_names':list(inspect['NetworkSettings']['Networks']),
 'same_postgres_container_as_chatwoot':pg['Id'][:12]==before['chatwoot-postgres-1'],
 'original_services_not_restarted':unchanged_started,
 'image_source_sha256':image_hashes,
 'postgres_original_network_retained':'chatwoot_default' in pg['NetworkSettings']['Networks'],
 'existing_service_id_changes':changed,'existing_services_checked':len(before)-1,
 'database':summary,'chatwoot_permissions':privileges,
 'source_sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
     for p in sorted((root/'rpa_gateway').glob('*.py')) if not p.name.startswith('._')},
 'checked_at_epoch':time.time()}
print(json.dumps(result,ensure_ascii=False,indent=2))
assert health['ready'] and health['storage']=='postgresql' and health['version']=='0.2.0'
assert result['port_bindings']=={'8080/tcp':[{'HostIp':'127.0.0.1','HostPort':'31080'}]}
assert result['same_postgres_container_as_chatwoot'] and result['postgres_original_network_retained']
assert result['existing_service_id_changes']=={}
assert all(unchanged_started.values())
assert image_hashes==result['source_sha256']
assert summary['active_device_credentials']==0 and summary['one_active_task_constraint'] and summary['imports']==1
assert summary['gateway_connections']<=8 and not summary['gateway_role_superuser']
assert not any(privileges.values())
assert summary['only_synthetic_tenants']
assert not any(summary['tasks'].get(state,0) for state in ('queued','leased','submitting','submitted','unknown'))
