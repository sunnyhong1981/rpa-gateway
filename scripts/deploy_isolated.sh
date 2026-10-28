#!/bin/sh
# First installation only. No commands target an existing business service.
set -eu
umask 077
bundle=/tmp/qunl-rpa-gateway-20261028-source.tar.gz
root=/opt/ai/rpa-gateway
if [ -e "$root" ]; then
  echo 'Gateway directory already exists; refusing to replace an existing installation.' >&2
  exit 1
fi
if [ -n "$(ss -ltnH 'sport = :31080')" ]; then
  echo 'Port 31080 is occupied; refusing deployment.' >&2
  exit 1
fi
mkdir -m 700 "$root"
tar -xzf "$bundle" -C "$root"
cd "$root"
python3 - <<'PY'
import json,subprocess
from pathlib import Path
rows=[json.loads(line) for line in subprocess.check_output(['docker','ps','--format','json'],text=True).splitlines()]
Path('baseline-services.json').write_text(json.dumps({r['Names']:r['ID'] for r in rows},indent=2))
PY
python3 scripts/init_runtime.py
chown 10001:10001 runtime/data runtime/secrets/admin_token runtime/secrets/backend_token runtime/secrets/verifier_token
docker compose -f deploy/compose.yaml build gateway
docker compose -f deploy/compose.yaml up -d --no-deps gateway
