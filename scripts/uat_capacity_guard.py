"""Resource guard for only the user-selected UAT capacity sandbox."""
import json,shutil,subprocess,time
from pathlib import Path
root=Path('/opt/ai/rpa-gateway-capacity-20261029')
stop=root/'guard-stop'
while not stop.exists():
    available=0
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):available=int(line.split()[1])*1024
    free=shutil.disk_usage('/').free
    reason='disk_reserve' if free<1024**3 else 'memory_reserve' if available<700*1024**2 else ''
    progress={'at':time.time(),'disk_free_bytes':free,'memory_available_bytes':available,'guarded_project':'qunl-rpa-capacity-lab'}
    (root/'guard-progress.json').write_text(json.dumps(progress))
    if reason:
        (root/'resource-stop.json').write_text(json.dumps({**progress,'reason':reason}))
        subprocess.run(['docker','compose','-f',str(root/'compose.json'),'stop'],capture_output=True)
        raise SystemExit(1)
    time.sleep(10)
