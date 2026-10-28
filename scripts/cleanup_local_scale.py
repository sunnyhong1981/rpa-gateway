"""Stop owned local dependencies only; preserve source and all synthetic audit data."""
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from psycopg.conninfo import conninfo_to_dict
from rpa_gateway.store import Store
from rpa_gateway.models import Review

root=Path(__file__).resolve().parents[1];state=root/'runtime/scale-local'
if (state/'processes.json').exists():raise RuntimeError('Stop the owned API processes first')
dsn=(state/'test_database_url').read_text().strip()
assert conninfo_to_dict(dsn).get('dbname')=='rpa_gateway_test' and conninfo_to_dict(dsn).get('port')=='35432'
store=Store(dsn,pool_size=1)
closed=0
try:
    with store.transaction() as db:
        unknown=db.execute("SELECT id,tenant FROM tasks WHERE state='unknown' AND tenant LIKE 'fault_%'").fetchall()
        devices=db.execute("SELECT id,tenant FROM devices WHERE revoked=0 AND (tenant LIKE 'fault_%' OR tenant LIKE 'load_%')").fetchall()
    for task in unknown:
        store.review(task['tenant'],task['id'],Review(action='close_unknown_without_retry',reason='本地纯模拟故障验收已检查源代码与日志，未操作企微；关闭测试记录，不重发。'))
        closed+=1
    for device in devices:store.credentials(device['tenant'],device['id'])
finally:store.close()
subprocess.run(['/opt/homebrew/opt/postgresql@17/bin/pg_ctl','-D',str(state/'pg'),'stop','-m','fast'],check=True,stdout=subprocess.DEVNULL)
pid=int((state/'redis.pid').read_text())
command=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
if 'redis-server' not in command or '36379' not in command:raise RuntimeError('Redis ownership mismatch')
os.kill(pid,signal.SIGTERM)
moto=[]
for line in subprocess.check_output(['ps','-axo','pid=,command='],text=True).splitlines():
    fields=line.strip().split(maxsplit=1)
    if len(fields)!=2:continue
    command=fields[1]
    if str(root/'.venv/bin/python') in command and '.venv/bin/moto_server' in command and '-p 39000' in command:
        os.kill(int(fields[0]),signal.SIGTERM);moto.append(int(fields[0]))
Path('/tmp/rpa-gateway-noop.txt').unlink(missing_ok=True)
print(json.dumps({'stopped_owned_dependencies':True,'synthetic_unknown_closed_without_retry':closed,'synthetic_device_credentials_revoked':len(devices),'moto_pids':moto,'formal_rpa_touched':False}))
