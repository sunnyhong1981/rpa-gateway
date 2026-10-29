import time
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST


class Metrics:
    def __init__(self):
        self.registry=CollectorRegistry()
        self.rss=Gauge('rpa_gateway_process_max_rss_bytes','Process peak RSS',registry=self.registry)
        self.fds=Gauge('rpa_gateway_process_fds','Current open file descriptors',registry=self.registry)
        self.disk_free=Gauge('rpa_gateway_runtime_disk_free_bytes','Free bytes on runtime filesystem',registry=self.registry)
        self.requests=Counter('rpa_gateway_http_total','HTTP requests',['route','method','status'],registry=self.registry)
        self.latency=Histogram('rpa_gateway_http_seconds','HTTP duration',['route','method'],
            buckets=(.005,.01,.025,.05,.1,.2,.5,1,2,5,10),registry=self.registry)
        self.websockets=Gauge('rpa_gateway_websockets','Authenticated local connections',registry=self.registry)
        self.loop_lag=Gauge('rpa_gateway_event_loop_lag_seconds','Event loop scheduling lag',registry=self.registry)
        self.outbox=Gauge('rpa_gateway_outbox_pending','Undelivered outbox events',registry=self.registry)
        self.outbox_age=Gauge('rpa_gateway_outbox_oldest_seconds','Oldest undelivered event age',registry=self.registry)
        self.unknown=Gauge('rpa_gateway_unknown_tasks','Tasks awaiting reconciliation',registry=self.registry)
        self.queue=Gauge('rpa_gateway_queued_tasks','Queued tasks',registry=self.registry)
        self.pool_wait=Gauge('rpa_gateway_pool_waiting','Requests waiting for DB pool',registry=self.registry)
        self.objects_up=Gauge('rpa_gateway_objects_up','Object storage availability',registry=self.registry)
        self.media_pending=Gauge('rpa_gateway_media_pending','Media awaiting validation',registry=self.registry)
        self.media_oldest=Gauge('rpa_gateway_media_oldest_seconds','Oldest queued media age',registry=self.registry)
        self.media_seconds=Gauge('rpa_gateway_media_validation_seconds_max5m','Max successful validation duration in last 5 minutes',registry=self.registry)
        self.redis_up=Gauge('rpa_gateway_redis_up','Redis availability',registry=self.registry)
        self.database_up=Gauge('rpa_gateway_database_up','Database probe availability',registry=self.registry)
        self.probe_up=Gauge('rpa_gateway_probe_up','Last probe result (1 healthy, 0 failed)',['component'],registry=self.registry)
        self.probe_success=Gauge('rpa_gateway_probe_last_success_timestamp','Last successful probe timestamp',['component'],registry=self.registry)
        self.probe_completed=Gauge('rpa_gateway_probe_last_completed_timestamp','Last completed probe timestamp',['component'],registry=self.registry)
        self.probe_errors=Counter('rpa_gateway_probe_errors_total','Health probe failures',['component','error_type'],registry=self.registry)
        self.worker=Gauge('rpa_gateway_worker_last_success_timestamp','Worker progress timestamp',registry=self.registry)
        self.notifications=Counter('rpa_gateway_notifications_total','Notification actions',['action'],registry=self.registry)

    def render(self):return generate_latest(self.registry)


class HttpMetrics:
    def __init__(self,app,metrics):self.app,self.metrics=app,metrics
    async def __call__(self,scope,receive,send):
        if scope['type']!='http':return await self.app(scope,receive,send)
        start=time.monotonic();status=500
        async def wrap(message):
            nonlocal status
            if message['type']=='http.response.start':status=message['status']
            await send(message)
        try:await self.app(scope,receive,wrap)
        finally:
            route=scope.get('route')
            label=route.path if route else 'unmatched'
            # Labels are route templates, never device IDs, tokens, waybills or signed URLs.
            self.metrics.requests.labels(label,scope['method'],str(status)).inc()
            self.metrics.latency.labels(label,scope['method']).observe(time.monotonic()-start)
