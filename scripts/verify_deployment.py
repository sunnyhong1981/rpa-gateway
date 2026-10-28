"""Read-only host verification. Do not print environment values or credentials."""
import hashlib
import json
import sqlite3
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

root=Path('/opt/ai/rpa-gateway')
inspect=json.loads(subprocess.check_output(['docker','inspect','qunl-rpa-gateway-gateway-1']))[0]
current={r['Names']:r['ID'] for r in (json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines())}
before=json.loads((root/'baseline-services.json').read_text())
changed={name:{'before':container,'after':current.get(name)} for name,container in before.items() if current.get(name)!=container}
with urlopen('http://127.0.0.1:31080/healthz',timeout=5) as response:health=json.load(response)
with sqlite3.connect('file:'+str(root/'runtime/data/gateway.sqlite3')+'?mode=ro',uri=True) as db:
    tasks=dict(db.execute('SELECT state,COUNT(*) FROM tasks GROUP BY state'))
    tenants=[row[0] for row in db.execute('SELECT DISTINCT tenant FROM tasks')]
    active_devices=db.execute('SELECT COUNT(*) FROM devices WHERE revoked=0').fetchone()[0]
    integrity=db.execute('PRAGMA integrity_check').fetchone()[0]
network=json.loads(subprocess.check_output(['docker','network','inspect','qunl-rpa-gateway_isolated']))[0]
result={'health':health,'container_id':inspect['Id'][:12],'image':inspect['Image'],
    'container_health':inspect['State']['Health']['Status'],
    'port_bindings':inspect['HostConfig']['PortBindings'],
    'network_names':list(inspect['NetworkSettings']['Networks']),
    'network_driver':network['Driver'],'network_members':len(network['Containers']),
    'read_only':inspect['HostConfig']['ReadonlyRootfs'],'user':inspect['Config']['User'],
    'cpu_limit':inspect['HostConfig']['NanoCpus']/1e9,'memory_limit_mib':inspect['HostConfig']['Memory']/1024/1024,
    'existing_service_id_changes':changed,'existing_services_checked':len(before),
    'tasks':tasks,'only_synthetic_tenants':all(t.startswith(('synthetic_','restart_','ws_')) for t in tenants),
    'active_device_credentials':active_devices,'sqlite_integrity':integrity,
    'source_sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'rpa_gateway').glob('*.py')) if not p.name.startswith('._')},
    'checked_at_epoch':time.time()}
print(json.dumps(result,ensure_ascii=False,indent=2))
assert health['ready'] and not health['legacy_connected']
assert result['port_bindings']=={'8080/tcp':[{'HostIp':'127.0.0.1','HostPort':'31080'}]}
assert result['network_names']==['qunl-rpa-gateway_isolated'] and result['network_members']==1
assert result['only_synthetic_tenants'] and active_devices==0 and integrity=='ok'
assert not any(tasks.get(state,0) for state in ('queued','leased','submitting','submitted','unknown'))
