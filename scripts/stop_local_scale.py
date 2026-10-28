"""Stop only processes created by start_local_scale.py. Leave desktop RPA untouched."""
import json
import os
import signal
import subprocess
import time
from pathlib import Path
root=Path(__file__).resolve().parents[1];state=root/'runtime/scale-local';config=state/'processes.json'
if not config.exists():raise SystemExit('No owned local scale processes')
for item in json.loads(config.read_text()):
    result=subprocess.run(['ps','-p',str(item['pid']),'-o','command='],text=True,capture_output=True)
    if result.returncode:continue
    command=result.stdout
    if str(root/'.venv/bin') not in command or 'rpa_gateway' not in command:raise RuntimeError('Unexpected process ownership')
    os.kill(item['pid'],signal.SIGTERM)
subprocess.run(['/opt/homebrew/opt/nginx/bin/nginx','-c',str(state/'nginx.conf'),'-s','quit'],check=True)
config.unlink();time.sleep(1)
print('Stopped owned local scale services only')
