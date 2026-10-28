"""Run isolated PostgreSQL regressions without printing the private test DSN."""
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse
path=Path('/opt/ai/rpa-gateway/runtime/secrets/test_database_url')
secret=path.read_text().strip()
command=['docker','run','--rm','--network','qunl-rpa-gateway_isolated','--cpus','0.5','--memory','384m',
 '--pids-limit','128','--read-only','--tmpfs','/tmp:size=64m,mode=1777',
 '--mount','type=bind,src='+str(path)+',dst=/run/secrets/test_database_url,readonly',
 '-e','RPA_GATEWAY_TEST_DSN_FILE=/run/secrets/test_database_url','qunl-rpa-gateway-tests:0.2.0',
 'python','-m','pytest','-q','-p','no:cacheprovider','--tb=short','--show-capture=no']
result=subprocess.run(command,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
text=result.stdout
for value in (secret,urlparse(secret).password):
 if value:text=text.replace(value,'[REDACTED]')
print(text,end='')
sys.exit(result.returncode)
