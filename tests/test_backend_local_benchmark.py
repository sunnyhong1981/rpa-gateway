"""Measured local preparation only: real HTTP, hashes, PG/S3, injected external latency."""
from contextlib import closing
import io,json,os,random,statistics,time
from pathlib import Path
from PIL import Image
from datetime import datetime,timezone
from psycopg.types.json import Jsonb
from rpa_gateway.source_pipeline import SourcePipeline
from rpa_gateway.source_queue import SourceSettings
from .source_fixture import SourceFixture,eventually


def task_count(store):
    with store.transaction() as db:return db.execute('SELECT count(*) n FROM tasks').fetchone()['n']


def test_local_preparation_concurrency_comparison(env):
    rng=random.Random(29);photos=[]
    for _ in range(3):
        raw=Image.frombytes('RGB',(900,700),rng.randbytes(900*700*3));buf=io.BytesIO();raw.save(buf,format='PNG');photos.append(buf.getvalue())
    store=env.app.state.store
    # Equal verified-cache conditions for every concurrency sample; bytes are still freshly downloaded and hashed.
    for photo in photos:store.put_media('test',photo,'image/png')
    samples=[]
    with closing(SourceFixture(env,groups=8,devices=4)) as fixture:
        for round_number,order in enumerate([(1,4,8),(8,1,4),(4,8,1)]):
            for workers in order:
                before=task_count(store)
                for group in range(8):fixture.add(group=group,images=photos,identity_delay=.15,file_delay=.075)
                settings=SourceSettings(workers=workers,ready_per_device=64)
                with closing(SourcePipeline(store,env.settings.database,fixture.configs,settings=settings)) as pipeline:
                    pipeline.capture_once();started=time.monotonic();pipeline.start('prepare')
                    eventually(lambda:task_count(store)==before+8,timeout=15)
                    seconds=time.monotonic()-started
                    samples.append({'round':round_number+1,'workers':workers,'jobs':8,'seconds':round(seconds,4),
                                    'jobs_per_second':round(8/seconds,3)})
        result={'scope':'isolated_loopback_preparation_same_new_path_1_vs_4_vs_8_workers',
            'injected_identity_seconds':.15,'injected_per_image_seconds':.075,'originals_per_job':3,
            'original_sizes':[len(p) for p in photos],'fresh_downloads':True,'verified_media_cache':'warm_for_all_samples',
            'samples':samples,'median_seconds':{str(n):round(statistics.median(s['seconds'] for s in samples if s['workers']==n),4) for n in (1,4,8)},
            'global_network_cap':8,'global_decode_cap':2,'ready_per_device_for_benchmark':64,'real_wecom_messages_sent':0,'production_connections':0,
            'not_end_to_end_or_real_vendor_capacity':True}
        result['speedup_vs_serial']={str(n):round(result['median_seconds']['1']/result['median_seconds'][str(n)],3) for n in (4,8)}
        target=os.environ.get('RPA_GATEWAY_BENCHMARK_OUTPUT')
        if target:Path(target).write_text(json.dumps(result,indent=2)+'\n')
        assert task_count(store)==72
        assert fixture.max_files<=8


def test_ten_devices_fifty_authorized_groups_local_preparation(env):
    store=env.app.state.store
    with closing(SourceFixture(env,groups=50,devices=10)) as fixture,closing(SourcePipeline(store,env.settings.database,fixture.configs,settings=SourceSettings(workers=8))) as pipeline:
        for group in range(50):
            for _ in range(2):fixture.add(group=group,identity_delay=.01)
        # 1,200 incoming messages per group, including the two source questions above.
        with store.transaction() as db:
            with db.cursor().copy('COPY source_messages(id,inbox_id,message_type,private,content,attributes,attachments,created_at) FROM STDIN') as copy:
                for group in range(50):
                    for _ in range(1198):
                        fixture.counter+=1
                        copy.write_row((fixture.counter,'inbox'+str(group+1),0,False,'local non-trigger chat',Jsonb({}),Jsonb([]),datetime.fromtimestamp(env.clock(),timezone.utc)))
        def caught_up():
            with store.transaction() as db:
                n=db.execute('SELECT count(*) n FROM source_routes r WHERE r.cursor>=(SELECT max(id) FROM source_messages m WHERE m.inbox_id=r.inbox)').fetchone()['n']
            return n==50 and task_count(store)==100
        started=time.monotonic();pipeline.start()
        eventually(caught_up,timeout=30)
        with store.transaction() as db:
            rows=db.execute("SELECT payload::jsonb->>'inbox_id' inbox,count(*) n FROM tasks GROUP BY 1").fetchall()
            wrong=db.execute("SELECT count(*) n FROM tasks t JOIN source_routes r ON r.tenant=t.tenant AND r.inbox=t.payload::jsonb->>'inbox_id' WHERE t.device<>r.device").fetchone()['n']
            pending=db.execute("SELECT count(*) n FROM source_jobs WHERE state IN ('pending','preparing')").fetchone()['n']
        assert len(rows)==50 and all(r['n']==2 for r in rows) and wrong==0 and pending==0
        target=os.environ.get('RPA_GATEWAY_SCALE_LOCAL_OUTPUT')
        if target:Path(target).write_text(json.dumps({'devices':10,'groups':50,'incoming_messages':60000,'prepared_tasks':100,'seconds':round(time.monotonic()-started,3),
            'cross_device_or_group_errors':wrong,'pending_preparation':pending,'real_wecom_messages_sent':0,
            'scope':'local_fixture_routing_and_preparation_only_not_desktop_capacity'},indent=2)+'\n')
