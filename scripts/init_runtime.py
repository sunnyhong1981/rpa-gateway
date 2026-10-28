"""Initialize a NEW private runtime only. Never replace existing credentials or DB."""
import os
import secrets
from pathlib import Path

root=Path(__file__).resolve().parents[1]/'runtime'
root.mkdir(exist_ok=True,mode=0o700)
(root/'secrets').mkdir(exist_ok=True,mode=0o700)
(root/'data').mkdir(exist_ok=True,mode=0o700)
for name in ('admin_token','backend_token','verifier_token'):
    path=root/'secrets'/name
    if not path.exists():
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as file:
            file.write(secrets.token_urlsafe(48)+'\n')
print('Private runtime initialized; no credential values printed')
