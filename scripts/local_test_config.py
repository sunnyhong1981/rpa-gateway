"""Create loopback-only development dependencies with disposable credentials."""
import json
import os
from pathlib import Path
import boto3
from botocore.config import Config

root=Path(__file__).resolve().parents[1]/'runtime/scale-local'
root.mkdir(parents=True,exist_ok=True)
config={'endpoint':'http://127.0.0.1:39000','public_endpoint':'http://127.0.0.1:39000',
        'bucket':'rpa-gateway-local-tests','region':'us-east-1','prefix':'tests',
        'access_key':'local-test-only','secret_key':'local-test-only'}
client=boto3.client('s3',endpoint_url=config['endpoint'],aws_access_key_id=config['access_key'],aws_secret_access_key=config['secret_key'],region_name='us-east-1')
try:client.head_bucket(Bucket=config['bucket'])
except Exception:client.create_bucket(Bucket=config['bucket'])
(root/'objects.json').write_text(json.dumps(config));(root/'objects.json').chmod(0o600)
print('Local object bucket initialized; no production credentials used')
