"""Configuration checks are local rendering only; never start any container."""
from collections import Counter
import json,os,subprocess
from pathlib import Path
import yaml
import pytest,shutil

pytestmark=pytest.mark.skipif(shutil.which("docker") is None,reason="Compose CLI is required for deployment rendering checks")

ROOT=Path(__file__).resolve().parents[1]


def test_gateway_roles_have_bounded_shared_connection_and_cpu_budgets():
    raw=subprocess.check_output(['docker','compose','-f',str(ROOT/'deploy/scale.compose.yaml'),
        '-f',str(ROOT/'deploy/backend-workers.compose.yaml'),'config','--format','json'],text=True)
    config=json.loads(raw);services=config['services'];gw_connections=0;cpus=0
    for name,service in services.items():
        if name in ('source','worker'):
            assert service['deploy']['replicas']==0
            continue
        cpus+=float(service.get('cpus',0))
        if 'qunl-rpa-gateway' not in service.get('image',''):continue
        env=service['environment']
        if name in ('gateway-a','gateway-b','media'):pool=int(env['RPA_GATEWAY_POOL_SIZE'])
        elif name.startswith('source-'):pool=int(env['RPA_GATEWAY_SOURCE_POOL_SIZE'])
        elif name in ('dispatch','validation','maintenance'):pool=int(env['RPA_GATEWAY_WORKER_POOL_SIZE'])
        else:pool=1  # Existing alert delivery process uses a one-connection pool.
        gw_connections+=pool
        assert service.get('mem_limit') and float(service['cpus'])>0
    assert gw_connections==19 and cpus<=2
    assert services['source-prepare-a']['environment']['RPA_GATEWAY_SOURCE_GLOBAL_JOBS']=='8'
    assert services['source-prepare-b']['environment']['RPA_GATEWAY_SOURCE_DECODE_SLOTS']=='2'
    assert services['source-prepare-a']['environment']['RPA_GATEWAY_SOURCE_ROLE']=='prepare'
    assert services['source-archive']['environment']['RPA_GATEWAY_SOURCE_ROLE']=='archive'
    assert services['source-receipts']['environment']['RPA_GATEWAY_SOURCE_ROLE']=='receipts'


def test_chatwoot_realtime_reports_and_scheduler_use_disjoint_complete_queues(tmp_path):
    original=yaml.safe_load((ROOT/'deploy/chatwoot/sidekiq-realtime.yml').read_text())
    scheduled=yaml.safe_load((ROOT/'deploy/chatwoot/sidekiq-scheduled.yml').read_text())
    background=yaml.safe_load((ROOT/'deploy/chatwoot/sidekiq-background.yml').read_text())
    queues=original[':queues']+scheduled[':queues']+background[':queues']
    assert all(n==1 for n in Counter(queues).values())
    assert 'agent_bot' in original[':queues'] and 'default' in original[':queues']
    assert scheduled[':queues']==['scheduled_jobs'] and background[':concurrency']==1
    assert 'low' in background[':queues'] and 'critical' not in background[':queues']
    base=tmp_path/'base.yml';base.write_text('services:\n  sidekiq:\n    image: local-chatwoot-fixture\n    environment:\n      RAILS_ENV: production\nnetworks:\n  default: {}\n')
    env={**os.environ,'CHATWOOT_BASE_COMPOSE':str(base)}
    raw=subprocess.check_output(['docker','compose','-f',str(base),'-f',str(ROOT/'deploy/chatwoot/workers.compose.yaml'),
        'config','--format','json'],env=env,text=True)
    services=json.loads(raw)['services']
    assert 'sidekiq' not in services
    assert 'statement_timeout=30000' in services['sidekiq-background']['environment']['PGOPTIONS']
    assert float(services['sidekiq-background']['cpus'])==.15
    assert [int(services[name]['environment']['SIDEKIQ_CONCURRENCY']) for name in
            ('sidekiq-realtime','sidekiq-scheduled','sidekiq-background')]==[4,2,1]


def test_uat4_trial_is_four_global_jobs_not_four_per_worker():
    config=json.loads(subprocess.check_output(['docker','compose','-f',str(ROOT/'deploy/scale.compose.yaml'),
        '-f',str(ROOT/'deploy/backend-workers.compose.yaml'),'-f',str(ROOT/'deploy/uat4-four-workers.compose.yaml'),
        'config','--format','json'],text=True))
    services=config['services']
    active=[]
    for name,service in services.items():
        if service.get('deploy',{}).get('replicas')==0:continue
        env=service.get('environment',{})
        if env.get('RPA_GATEWAY_SOURCE_ROLE')=='prepare':active.append(service)
        if name.startswith('source-'):
            assert env['RPA_GATEWAY_SOURCE_GLOBAL_JOBS']=='4'
            assert env['RPA_GATEWAY_SOURCE_NETWORK_SLOTS']=='4'
            assert env['RPA_GATEWAY_SOURCE_PER_GROUP_JOBS']=='2'
    assert len(active)==1
    assert active[0]['environment']['RPA_GATEWAY_SOURCE_WORKERS']=='4'
