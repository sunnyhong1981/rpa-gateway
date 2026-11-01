"""Independent health probes: a statistics failure must never mark Redis down."""
import asyncio
import logging
import os
from pathlib import Path
import resource
import shutil
import sys
import time
import traceback

from starlette.concurrency import run_in_threadpool


SNAPSHOT_SQL = "SELECT (SELECT count(*) FROM outbox WHERE delivered_at IS NULL) pending, (SELECT min(created) FROM outbox WHERE delivered_at IS NULL) oldest, (SELECT count(*) FROM tasks WHERE state='unknown') unknown, (SELECT count(*) FROM tasks WHERE state='queued') queued, (SELECT updated FROM worker_health WHERE name='maintenance') worker, (SELECT count(*) FROM media_uploads WHERE state IN ('queued','validating')) media_pending, (SELECT min(created) FROM media_uploads WHERE state IN ('queued','validating')) media_oldest, (SELECT max(validation_seconds) FROM media_uploads WHERE state='ready' AND updated>EXTRACT(EPOCH FROM now())-300) media_seconds"


class ProbeRejected(RuntimeError):
    pass


class HealthObserver:
    def __init__(self, store, metrics):
        self.store, self.metrics = store, metrics
        self.failures = {}
        self.last_logged = {}

    async def probe(self, component, callback, availability=None):
        try:
            value = await run_in_threadpool(callback)
            if value is False:
                raise ProbeRejected()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            kind = type(exc).__name__
            self.metrics.probe_up.labels(component).set(0)
            self.metrics.probe_errors.labels(component, kind).inc()
            if availability is not None:
                availability.set(0)
            now = time.monotonic()
            if self.failures.get(component) != kind or now-self.last_logged.get(component, 0) >= 60:
                # Never log exception messages: they can contain credentials/URLs.
                frames = traceback.extract_tb(exc.__traceback__)
                origin = frames[-1] if frames else None
                logging.warning('Gateway health probe failed component=%s error_type=%s origin=%s:%s',
                                component, kind, Path(origin.filename).name if origin else '-',
                                origin.lineno if origin else 0)
                self.last_logged[component] = now
            self.failures[component] = kind
            return False, None
        else:
            if self.failures.pop(component, None):
                logging.info('Gateway health probe recovered component=%s', component)
            self.metrics.probe_up.labels(component).set(1)
            self.metrics.probe_success.labels(component).set(time.time())
            if availability is not None:
                availability.set(1)
            return True, value
        finally:
            self.metrics.probe_completed.labels(component).set(time.time())

    def snapshot(self):
        with self.store.transaction() as db:
            return db.execute(SNAPSHOT_SQL).fetchone()

    def update_statistics(self, values):
        m, now = self.metrics, self.store.clock()
        m.outbox.set(values['pending']); m.outbox_age.set(max(0, now-(values['oldest'] or now)))
        m.unknown.set(values['unknown']); m.queue.set(values['queued']); m.worker.set(values['worker'] or 0)
        m.pool_wait.set(self.store.database.pool.get_stats().get('requests_waiting', 0))
        m.media_pending.set(values['media_pending']); m.media_oldest.set(max(0, now-(values['media_oldest'] or now)))
        m.media_seconds.set(values['media_seconds'] or 0)

    async def database_sample(self):
        ok, values = await self.probe('database', self.snapshot, self.metrics.database_up)
        if ok:
            await self.probe('statistics', lambda: self.update_statistics(values))

    def process_sample(self):
        m = self.metrics
        m.rss.set(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        m.disk_free.set(shutil.disk_usage('/data' if Path('/data').is_dir() else '.').free)
        try:
            m.fds.set(len(os.listdir('/proc/self/fd' if sys.platform=='linux' else '/dev/fd')))
        except OSError:
            pass

    async def run(self):
        async def repeat(component, sample, interval):
            self.metrics.probe_completed.labels(component).set(time.time())
            while True:
                await sample()
                await asyncio.sleep(interval)

        async def process():
            start = time.monotonic()
            await self.probe('process', self.process_sample)
            await asyncio.sleep(1)
            self.metrics.loop_lag.set(max(0, time.monotonic()-start-1))

        jobs = [asyncio.create_task(repeat('database', self.database_sample, 5)),
                asyncio.create_task(repeat('process', process, 0))]
        if self.store.presence:
            jobs.append(asyncio.create_task(repeat('redis', lambda: self.probe('redis', self.store.presence.ping, self.metrics.redis_up), 5)))
        if self.store.objects:
            jobs.append(asyncio.create_task(repeat('objects', lambda: self.probe('objects', self.store.objects.ready, self.metrics.objects_up), 5)))
        try:
            await asyncio.gather(*jobs)
        finally:
            for job in jobs:
                job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
