"""Add scoped uat4.qunl.com routes without changing its existing application locations."""
import json,os,shutil,subprocess,time
from pathlib import Path
root=Path('/opt/ai/rpa-gateway')
conf=Path('/opt/ai/chatwoot/config/public-nginx.conf')
source=conf.read_text()
marker='# BEGIN QUNL RPA GATEWAY'
existing=marker in source
# Locate only the existing TLS server for uat4.qunl.com, never the adjacent host.
lines=source.splitlines(keepends=True)
start=next(i for i,line in enumerate(lines) if 'server_name uat4.qunl.com;' in line and any('listen 443 ssl' in v for v in lines[max(0,i-5):i]))
insert=start+1
locations='''
  # BEGIN QUNL RPA GATEWAY
  location ^~ /rpa-gateway/ {
    resolver 127.0.0.11 valid=5s ipv6=off;
    set $rpa_gateway_upstream qunl-rpa-gateway-scale-ingress-1:8080;
    rewrite ^/rpa-gateway/(.*)$ /$1 break;
    proxy_pass http://$rpa_gateway_upstream;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_read_timeout 90s;
    proxy_send_timeout 30s;
    proxy_request_buffering off;
    proxy_next_upstream off;
    client_max_body_size 12m;
  }
  location = /rpa-gateway-originals {
    access_log off;
    resolver 127.0.0.11 valid=5s ipv6=off;
    set $rpa_objects_upstream qunl-rpa-gateway-scale-objects-1:9000;
    proxy_pass http://$rpa_objects_upstream$request_uri;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_read_timeout 30s;
    proxy_next_upstream off;
    client_max_body_size 13m;
  }
  location ^~ /rpa-gateway-originals/ {
    access_log off;
    resolver 127.0.0.11 valid=5s ipv6=off;
    set $rpa_objects_upstream qunl-rpa-gateway-scale-objects-1:9000;
    proxy_pass http://$rpa_objects_upstream$request_uri;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_read_timeout 30s;
    proxy_next_upstream off;
    client_max_body_size 13m;
  }
  # END QUNL RPA GATEWAY
'''
backup=root/'runtime/releases'/time.strftime('public-route-%Y%m%d-%H%M%S',time.gmtime())
backup.mkdir(parents=True,mode=0o700);(backup/'public-nginx.conf').write_text(source)
v=json.loads(subprocess.check_output(['docker','inspect','chatwoot-public-nginx']))[0]
if 'qunl-rpa-gateway_isolated' not in v['NetworkSettings']['Networks']:
 subprocess.run(['docker','network','connect','qunl-rpa-gateway_isolated','chatwoot-public-nginx'],check=True)
if existing:
 start_marker=source.index('  # BEGIN QUNL RPA GATEWAY')
 end_marker=source.index('  # END QUNL RPA GATEWAY',start_marker)+len('  # END QUNL RPA GATEWAY')
 updated=source[:start_marker]+locations.strip('\n')+source[end_marker:]
else:updated=''.join(lines[:insert])+locations+''.join(lines[insert:])
conf.write_text(updated)
try:
 subprocess.run(['docker','exec','chatwoot-public-nginx','nginx','-t'],check=True,capture_output=True)
 subprocess.run(['docker','exec','chatwoot-public-nginx','nginx','-s','reload'],check=True,capture_output=True)
except Exception:
 conf.write_text(source)
 subprocess.run(['docker','exec','chatwoot-public-nginx','nginx','-s','reload'],capture_output=True)
 raise
config=root/'runtime/secrets/object_config'
values=json.loads(config.read_text());values['public_endpoint']='https://uat4.qunl.com'
# Preserve secret-file inode and permissions used by running containers.
config.write_text(json.dumps(values)+'\n')
print(json.dumps({'enabled':True,'gateway_url':'https://uat4.qunl.com/rpa-gateway','object_endpoint':'https://uat4.qunl.com/rpa-gateway-originals',
 'nginx_reloaded':True,'nginx_container_restarted':False,'backup':str(backup)}))
