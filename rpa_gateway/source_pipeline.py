"""Independent capture, bounded preparation, archive and receipt roles; no desktop sends."""
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
from pathlib import Path
import signal
import threading
import time
import uuid
from .source_bridge import SourceBridge
from .source_queue import SourceQueue,SourceSettings
from .source_resources import SourceReader,SourceResources

ROLES=('capture','prepare','archive','receipts')


class SourcePipeline:
    def __init__(self,store,source_dsn,configs,*,settings=None,read_pool_size=1,instance=None):
        self.store=store;self.settings=settings or SourceSettings();self.instance=instance or uuid.uuid4().hex
        self.stop=threading.Event();self.closed=False
        for config in configs:
            for name in ('tenant','inbox_id','archive_group_id','archive_sender_id','group_name','base_url',
                         'inbox_identifier','hmac_token','contact_source','public_conversation_id','delivery_client_id'):
                if not config.get(name):raise ValueError('Missing source configuration: '+name)
        routes=[(str(c['tenant']),str(c['inbox_id'])) for c in configs]
        if len(set(routes))!=len(routes):raise ValueError('Duplicate authorized source route')
        self.queue=SourceQueue(store,routes,self.settings)
        self.reader=SourceReader(source_dsn,size=read_pool_size)
        self.resources=SourceResources(store.presence,self.settings)
        self.bridges={(str(c['tenant']),str(c['inbox_id'])):SourceBridge(store,source_dsn,c,settings=self.settings,
            reader=self.reader,resources=self.resources,queue=self.queue) for c in configs}
        self.preparers=ThreadPoolExecutor(max_workers=self.settings.workers,thread_name_prefix='source-prepare')
        self.reporters=ThreadPoolExecutor(max_workers=self.settings.report_workers,thread_name_prefix='source-receipt')
        self.preparing={};self.reporting={};self.errors={};self.progress={};self.threads=[];self.rotation=0

    def _progress(self,role):
        self.progress[role]=self.store.clock();self.errors[role]=''

    def capture_once(self):
        bridges=list(self.bridges.values())
        if not bridges:return
        offset=self.rotation%len(bridges);self.rotation+=1
        error=None
        for bridge in bridges[offset:]+bridges[:offset]:
            try:bridge.capture()
            except Exception as exc:error=error or exc
        if error:raise error
        self._progress('capture')

    def prepare_once(self):
        for future in list(self.preparing):
            if future.done():
                future.result();self.preparing.pop(future);self._progress('prepare')
        while len(self.preparing)<self.settings.workers and not self.stop.is_set():
            job=self.queue.claim()
            if job is None:break
            bridge=self.bridges[(job['tenant'],job['inbox'])]
            future=self.preparers.submit(bridge.prepare,job,self.stop);self.preparing[future]=job
        if not self.preparing:self._progress('prepare')

    def archive_once(self):
        error=None
        for bridge in self.bridges.values():
            try:bridge.reconcile(publish_receipts=False)
            except Exception as exc:error=error or exc
        if error:raise error
        self._progress('archive')

    def receipt_once(self):
        for future in list(self.reporting):
            if future.done():
                future.result();self.reporting.pop(future);self._progress('receipts')
        while len(self.reporting)<self.settings.report_workers and not self.stop.is_set():
            job=self.queue.claim_report()
            if job is None:break
            bridge=self.bridges[(job['tenant'],job['inbox'])]
            future=self.reporters.submit(bridge.publish_report,job);self.reporting[future]=job
        if not self.reporting:self._progress('receipts')

    def heartbeat(self,roles):
        now=self.store.clock()
        with self.store.transaction() as db:
            for role in roles:
                count=len(self.preparing) if role=='prepare' else len(self.reporting) if role=='receipts' else 0
                db.execute('''INSERT INTO worker_health(name,updated,error_type,last_progress,active_jobs)
                  VALUES(%s,%s,%s,%s,%s) ON CONFLICT(name) DO UPDATE SET updated=EXCLUDED.updated,
                  error_type=EXCLUDED.error_type,last_progress=EXCLUDED.last_progress,active_jobs=EXCLUDED.active_jobs''',
                  ('source:'+role+':'+self.instance,now,self.errors.get(role,''),self.progress.get(role),count))
        path=os.environ.get('RPA_GATEWAY_SOURCE_HEALTH_FILE')
        if path:
            target=Path(path);pending=target.with_suffix('.next')
            pending.write_text(json.dumps({'at':time.time(),'roles':list(roles),'instance':self.instance}))
            pending.replace(target)

    def start(self,role='all'):
        roles=ROLES if role=='all' else (role,)
        if any(r not in ROLES for r in roles):raise ValueError('Unknown source pipeline role')
        if self.threads:raise RuntimeError('Pipeline already started')
        callbacks={'capture':(self.capture_once,.5),'prepare':(self.prepare_once,.05),
                   'archive':(self.archive_once,.25),'receipts':(self.receipt_once,.05)}
        def run(component,callback,interval):
            previous=''
            while not self.stop.is_set():
                try:
                    callback()
                    if previous:logging.info('Source component recovered role=%s',component);previous=''
                except Exception as exc:
                    kind=type(exc).__name__;self.errors[component]=kind
                    if kind!=previous:logging.warning('Source component failed role=%s error_type=%s',component,kind)
                    previous=kind
                self.stop.wait(interval)
        for name in roles:
            callback,interval=callbacks[name]
            self.threads.append(threading.Thread(target=run,args=(name,callback,interval),name='source-'+name,daemon=True))
        self.threads.append(threading.Thread(target=run,args=('health',lambda:self.heartbeat(roles),1),name='source-health',daemon=True))
        for thread in self.threads:thread.start()
        return self

    def run(self,role='all'):
        self.start(role)
        try:
            while not self.stop.wait(.5):pass
        finally:self.close()

    def close(self):
        if self.closed:return
        self.closed=True;self.stop.set()
        for thread in self.threads:thread.join(timeout=10)
        self.preparers.shutdown(wait=True,cancel_futures=True)
        self.reporters.shutdown(wait=True,cancel_futures=True)
        self.resources.close();self.reader.close()


def main():
    from .app import Settings,make_store
    raw=json.loads(Path(os.environ['RPA_GATEWAY_SOURCE_CONFIG_FILE']).read_text())
    configs=raw if isinstance(raw,list) else [raw]
    values={name:int(os.environ['RPA_GATEWAY_SOURCE_'+name.upper()]) for name in
            ('workers','global_jobs','per_group_jobs','image_workers','network_slots','decode_slots','report_workers','max_attempts','ready_per_device')
            if 'RPA_GATEWAY_SOURCE_'+name.upper() in os.environ}
    for name in ('lease_seconds','prepare_seconds','queue_seconds'):
        key='RPA_GATEWAY_SOURCE_'+name.upper()
        if key in os.environ:values[name]=float(os.environ[key])
    settings=SourceSettings(**values)
    store=make_store(Settings.from_env(),pool_size=int(os.environ.get('RPA_GATEWAY_SOURCE_POOL_SIZE','2')))
    source_dsn=Path(os.environ['RPA_GATEWAY_SOURCE_DATABASE_URL_FILE']).read_text().strip()
    pipeline=SourcePipeline(store,source_dsn,configs,settings=settings,
        read_pool_size=int(os.environ.get('RPA_GATEWAY_SOURCE_READ_POOL_SIZE','1')),
        instance=os.environ.get('RPA_GATEWAY_INSTANCE_ID'))
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:pipeline.stop.set())
    try:pipeline.run(os.environ.get('RPA_GATEWAY_SOURCE_ROLE','all'))
    finally:pipeline.close();store.close()


if __name__=='__main__':main()
