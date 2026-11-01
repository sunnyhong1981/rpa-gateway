"""Create a shadow-only local Mac device; write the device token only to a private file."""
import json,os,time,uuid
import psycopg
from pathlib import Path
from rpa_gateway.app import Settings,make_store
from rpa_gateway.models import DeviceCreate

root=Path(os.environ.get('RPA_GATEWAY_DEVICE_EXPORT_ROOT','/export'))
# This script is run in a gateway container with /data bound only for the private export.
store=make_store(Settings.from_env(),pool_size=1)
tenant='chatwoot_871';device='mac-sunny-pilot-469'
try:
    saved=root/'mac-pilot-device.json'
    if saved.exists():raise RuntimeError('A pilot device export already exists; preserve it and review before replacing')
    with store.transaction() as db:
        existing=db.execute('SELECT * FROM source_routes WHERE tenant=%s AND inbox=%s',(tenant,'469')).fetchone()
        active=db.execute("SELECT count(*) n FROM tasks WHERE tenant=%s AND device=%s AND state IN ('leased','submitting','submitted','unknown')",(tenant,device)).fetchone()['n']
    if existing:
        if existing['mode']!='shadow' or existing['client_ready_at'] is not None or active:
            raise RuntimeError('Existing pilot is not safely shadow-only; do not rotate its credential')
        registration=store.credentials(tenant,device,rotate=True)
    else:registration=store.provision(tenant,DeviceCreate(device_id=device,bindings=[{'inbox_id':'469','room_id':'wr93kQSAAARbMiA4ywr4_FJwO4l3jVjg'}]))
    with psycopg.connect(Settings.from_env().source_database_url) as source:
        floor=source.execute('SELECT COALESCE(max(id),0) FROM rpa_gateway_source.test_messages').fetchone()[0]
    # Do not enable real source jobs until the old native worker has stopped and a floor is set.
    with store.transaction() as db:
        db.execute("INSERT INTO source_routes(tenant,inbox,device,mode,floor,cursor,configured_at) VALUES(%s,'469',%s,'shadow',%s,%s,%s) ON CONFLICT(tenant,inbox) DO NOTHING",
                   (tenant,device,floor,floor,store.clock()))
    value={'base_url':'https://uat4.qunl.com/rpa-gateway','device_token':registration['token'],
           'installation_id':str(uuid.uuid4()),'runtime_version':'2026.10.29.gateway-pilot','inbox_id':'469'}
    fd=os.open(saved,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream:json.dump(value,stream)
    print(json.dumps({'registered':True,'device_id':device,'tenant':tenant,'mode':'shadow','credentials_printed':False}))
finally:store.close()
