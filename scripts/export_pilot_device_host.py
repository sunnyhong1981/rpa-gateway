import os,subprocess,json,shutil
from pathlib import Path
root=Path('/opt/ai/rpa-gateway');export=root/'runtime/device-export'
export.mkdir(exist_ok=True,mode=0o700);os.chown(export,10001,10001)
command=['docker','run','--rm','-i','--network','qunl-rpa-gateway_isolated','--cpus','0.5','--memory','256m','--read-only','--tmpfs','/tmp:size=16m,mode=1777',
 '--mount','type=bind,src='+str(export)+',dst=/export', '-e','RPA_GATEWAY_DEVICE_EXPORT_ROOT=/export']
for name in ['database_url','admin_token','backend_token','verifier_token','redis_url','object_config','source_database_url']:
 command+=['--mount','type=bind,src='+str(root/'runtime/secrets'/name)+',dst=/run/secrets/'+name+',readonly',
           '-e','RPA_GATEWAY_'+name.upper()+'_FILE=/run/secrets/'+name]
command+=['qunl-rpa-gateway:0.4.0','python','-']
result=subprocess.run(command,input=Path('/opt/ai/rpa-gateway-mac-pilot-candidate-20261029/scripts/prepare_mac_gateway_host.py').read_text(),text=True,capture_output=True,timeout=60)
if result.returncode:raise RuntimeError('Private device export failed; inspect with redaction')
saved=root/'runtime/secrets/mac_pilot_device'
if saved.exists():raise RuntimeError('Existing saved pilot credential must not be overwritten')
shutil.move(export/'mac-pilot-device.json',saved);saved.chmod(0o600)
print(result.stdout,end='')
