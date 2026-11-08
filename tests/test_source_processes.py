"""Run the real source roles as distinct local OS processes against fixture-only services."""
from contextlib import closing
from pathlib import Path
import json,os,subprocess,sys,time
from urllib.parse import urlparse
from .source_fixture import SourceFixture,photo,eventually


def test_four_source_roles_are_independent_processes(env,tmp_path):
    assert urlparse(env.settings.redis_url).hostname=='127.0.0.1'
    assert urlparse(env.settings.object_config['endpoint']).hostname=='127.0.0.1'
    store=env.app.state.store
    script="""
from pathlib import Path
import json,sys,signal
from rpa_gateway.app import Settings,make_store
from rpa_gateway.source_pipeline import SourcePipeline
c=json.loads(Path(sys.argv[1]).read_text());role=sys.argv[2]
s=Settings(c['database'],'a'*48,'b'*48,'v'*48,redis_url=c['redis_url'],redis_namespace=c['namespace'],object_config=c['objects'])
store=make_store(s,clock=lambda:c['clock'],pool_size=1)
pipeline=SourcePipeline(store,c['database'],c['configs'],instance='process_'+role)
for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:pipeline.stop.set())
try:pipeline.run(role)
finally:pipeline.close();store.close()
"""
    with closing(SourceFixture(env,groups=2)) as fixture:
        private=tmp_path/'private.json';private.write_text(json.dumps({'database':env.settings.database,'clock':env.clock(),
            'redis_url':env.settings.redis_url,'namespace':env.settings.redis_namespace,'objects':env.settings.object_config,'configs':fixture.configs}));private.chmod(0o600)
        children=[];logs=[]
        try:
            for role in ['capture','prepare','archive','receipts']:
                log=(tmp_path/(role+'.log')).open('w');logs.append(log)
                children.append(subprocess.Popen([sys.executable,'-c',script,str(private),role],stdout=log,stderr=subprocess.STDOUT))
            def ready():
                if any(p.poll() is not None for p in children):
                    raise AssertionError('Local source process exited: '+''.join(p.read_text()[-1000:] for p in tmp_path.glob('*.log')))
                with store.transaction() as db:return db.execute("SELECT count(*) n FROM worker_health WHERE name LIKE 'source:%:process_%'").fetchone()['n']==4
            eventually(ready,timeout=15)
            fixture.add(group=0,images=[photo()],file_delay=2);fixture.add(group=1)
            assert fixture.file_started.wait(5)
            def published():
                with store.transaction() as db:return db.execute('SELECT count(*) n FROM tasks').fetchone()['n']>=1
            eventually(published,timeout=1.5)
            d=fixture.devices[0];task=env.claim(d).json()['task'];assert task['payload']['inbox_id']=='inbox2'
            assert env.receipt(d,task,'submitting').status_code==200
            assert env.receipt(d,task,'submitted').status_code==200
            fixture.archive(task)
            eventually(lambda:env.inspect(d,task).json()['state']=='confirmed',timeout=1.5)
            eventually(lambda:any(r['status']=='delivered' for r in fixture.receipts),timeout=2)
            assert all(p.poll() is None for p in children)
        finally:
            for child in children:
                if child.poll() is None:child.terminate()
            for child in children:
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=3)
            for stream in logs:stream.close()
