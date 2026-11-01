"""Independent timeout recovery for the bounded Mac pilot installation only."""
import json,os,sqlite3,subprocess,sys,time
from pathlib import Path
plan_path=Path(sys.argv[1]);plan=json.loads(plan_path.read_text())
root=Path(plan['root']);success=Path(plan['success']);deadline=time.time()+180
while time.time()<deadline:
 if success.exists():raise SystemExit(0)
 time.sleep(1)
sys.path.insert(0,str(Path(plan['candidate'])))
from scripts.rpa_service import Service
try:Service(plan['candidate']).stop(timeout=20)
except Exception as exc:
 (plan_path.parent/'watchdog-hold.json').write_text(json.dumps({'reason':'Candidate ownership or stop could not be verified','type':type(exc).__name__}));raise SystemExit(1)
with sqlite3.connect('file:'+plan['state']+'?mode=ro',uri=True) as db:
 tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
 fenced=db.execute("SELECT count(*) FROM gateway_attempts WHERE state IN ('intent_pending','enter_marked','submitted','unknown')").fetchone()[0] if 'gateway_attempts' in tables else 0
if fenced:
 (plan_path.parent/'watchdog-hold.json').write_text(json.dumps({'reason':'Fenced gateway submissions require reconciliation; no old sender restarted','fenced':fenced}));raise SystemExit(1)
subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','chatwoot-prod',
 'docker exec chatwoot-postgres-1 psql -U postgres -d rpa_gateway -At -c "UPDATE source_routes SET mode=\'paused\',client_ready_at=NULL WHERE tenant=\'chatwoot_871\' AND inbox=\'469\';"'],capture_output=True,timeout=15)
temp=root/('current.restore.'+str(os.getpid()));temp.symlink_to(plan['old_root']);os.replace(temp,root/'current')
Service(plan['old_root']).start(timeout=15)
(plan_path.parent/'watchdog-restored.json').write_text(json.dumps({'restored':True,'old_root':plan['old_root'],'sqlite_restored':False}))
