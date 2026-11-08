import asyncio
import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import redis.asyncio as redis
from rpa_gateway.metrics import Metrics
from rpa_gateway.notifications import Connection, Hub


def test_idle_subscription_survives_read_timeout_and_checks_pong():
    async def sample():
        channel = 'test_health_' + uuid.uuid4().hex
        metrics = Metrics()
        metrics.redis_up.set(1)
        hub = Hub(None, SimpleNamespace(channel=channel), metrics)
        url = os.environ.get('RPA_GATEWAY_TEST_REDIS_URL', 'redis://127.0.0.1:36379/0')
        publisher = redis.Redis.from_url(url)
        connection = Connection({}, 'test')
        hub.by_device[('test', 'desktop')] = {'test': connection}
        await hub.start(url)
        try:
            async with asyncio.timeout(3):
                while not metrics.subscriber_up._value.get():
                    await asyncio.sleep(.01)
            await asyncio.sleep(3.5)
            assert metrics.subscriber_up._value.get() == 1
            await publisher.publish(channel, 'malformed')
            await publisher.publish(channel, '{"tenant":"test","device":"desktop"}')
            await asyncio.wait_for(connection.queue.get(), 2)
            async with asyncio.timeout(18):
                while metrics.notifications.labels('subscriber_pong')._value.get() < 1:
                    await asyncio.sleep(.05)
            assert metrics.subscriber_up._value.get() == 1
            assert metrics.redis_up._value.get() == 1
            assert metrics.notifications.labels('invalid_notification')._value.get() == 1
            assert not metrics.subscriber_errors._metrics
        finally:
            await hub.close()
            await publisher.aclose()
    asyncio.run(sample())


def test_subscriber_failure_never_overwrites_redis_probe():
    async def sample():
        metrics = Metrics(); metrics.redis_up.set(1)
        hub = Hub(None, SimpleNamespace(channel='unused'), metrics)
        subscription = SimpleNamespace(subscribe=AsyncMock(), get_message=AsyncMock(side_effect=ConnectionError()))
        context = AsyncMock()
        context.__aenter__.return_value = subscription
        hub.redis = SimpleNamespace(pubsub=Mock(return_value=context))
        with patch('rpa_gateway.notifications.asyncio.sleep', AsyncMock(side_effect=asyncio.CancelledError())):
            try:
                await hub.subscribe()
            except asyncio.CancelledError:
                pass
        assert metrics.subscriber_up._value.get() == 0
        assert metrics.subscriber_errors.labels('ConnectionError')._value.get() == 1
        assert metrics.redis_up._value.get() == 1
    asyncio.run(sample())
