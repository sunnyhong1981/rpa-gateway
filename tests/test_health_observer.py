import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import Mock

from rpa_gateway.health_observer import HealthObserver
from rpa_gateway.metrics import Metrics
from rpa_gateway.alerting import notification_body


def observer():
    return HealthObserver(SimpleNamespace(), Metrics())


def test_database_failure_does_not_mark_redis_down(caplog):
    o = observer()
    o.snapshot = Mock(side_effect=RuntimeError('credential-must-not-be-logged'))
    async def sample():
        await o.database_sample()
        await o.probe('redis', lambda: True, o.metrics.redis_up)
    with caplog.at_level(logging.WARNING):
        asyncio.run(sample())
    assert o.metrics.database_up._value.get() == 0
    assert o.metrics.redis_up._value.get() == 1
    assert 'component=database error_type=RuntimeError' in caplog.text
    assert 'credential-must-not-be-logged' not in caplog.text


def test_statistics_failure_is_separate_from_database_and_redis():
    o = observer()
    o.snapshot = Mock(return_value={})
    o.update_statistics = Mock(side_effect=TypeError())
    asyncio.run(o.database_sample())
    assert o.metrics.database_up._value.get() == 1
    assert o.metrics.probe_up.labels('statistics')._value.get() == 0
    assert o.failures == {'statistics': 'TypeError'}


def test_true_redis_failure_and_recovery():
    o = observer()
    async def sample():
        await o.probe('redis', Mock(side_effect=TimeoutError()), o.metrics.redis_up)
        assert o.metrics.redis_up._value.get() == 0
        await o.probe('redis', lambda: True, o.metrics.redis_up)
    asyncio.run(sample())
    assert o.metrics.redis_up._value.get() == 1
    assert o.metrics.probe_errors.labels('redis', 'TimeoutError')._value.get() == 1
    assert o.metrics.probe_success.labels('redis')._value.get() > 0
    assert not o.failures


def test_slow_database_does_not_block_redis_and_cancellation_propagates():
    o = observer()
    import threading
    started, release = threading.Event(), threading.Event()
    def slow():
        started.set()
        release.wait(2)
        return True
    async def sample():
        task = asyncio.create_task(o.probe('database', slow, o.metrics.database_up))
        try:
            while not started.is_set():
                await asyncio.sleep(.001)
            await asyncio.wait_for(o.probe('redis', lambda: True, o.metrics.redis_up), .5)
            assert not task.done()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('Cancellation swallowed')
        finally:
            release.set()
    asyncio.run(sample())
    assert 'database' not in o.failures


def test_recovery_uses_recovery_summary_and_identifies_module():
    body = dict(status='resolved', summary='在线状态不可用', recovery_summary='Redis 在线状态探测已恢复',
                module='Redis 在线状态服务', alertname='RPAGatewayRedisUnavailable', severity='critical',
                instance='gateway-a:8080', fingerprint='a'*16)
    text = notification_body(body)
    assert '不可用' not in text
    assert '模块：Redis 在线状态服务' in text
    assert '规则：RPAGatewayRedisUnavailable' in text
    del body['recovery_summary']
    assert '已恢复；原告警：' in notification_body(body)
