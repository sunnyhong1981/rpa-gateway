"""Bring up only the independent capacity sandbox after its owned image is available."""
import json,subprocess,time,shutil
from pathlib import Path
root=Path('/opt/ai/rpa-gateway-capacity-20261029')
if (root/'resource-stop.json').exists():raise RuntimeError('Resource guard has stopped this sandbox')
if shutil.disk_usage('/').free<1024**3:raise RuntimeError('Disk reserve too low')
command=['docker','run','--rm','-i','--network','qunl-rpa-capacity-lab_lab','--cpus','.2','--memory','192m','--read-only']
command+=['--mount','type=bind,src='+str(root/'runtime/secrets/object_config')+',dst=/run/secrets/object_config,readonly',
          'qunl-rpa-gateway:0.4.0','python','-']
bootstrap='''
from pathlib import Path
import json,time
from rpa_gateway.objects import Objects
objects=Objects(json.loads(Path('/run/secrets/object_config').read_text()))
for attempt in range(40):
 try:objects.client.list_buckets();break
 except Exception:time.sleep(1)
else:raise RuntimeError('Sandbox S3 not ready')
try:objects.client.head_bucket(Bucket=objects.bucket)
except Exception:objects.client.create_bucket(Bucket=objects.bucket)
print('sandbox_bucket_ready')
'''
p=subprocess.run(command,input=bootstrap,text=True,capture_output=True,timeout=60)
if p.returncode:raise RuntimeError('Sandbox object store initialization failed')
compose=['docker','compose','-f',str(root/'compose.json')]
subprocess.run(compose+['up','-d','--no-deps','gateway-a','gateway-b','media','worker','ingress'],check=True)
print(json.dumps({'started':True,'target':'uat.qunl.com','root':str(root),'role':'capacity_sandbox_only','existing_database_used':False,'public_ports_added':False}))
