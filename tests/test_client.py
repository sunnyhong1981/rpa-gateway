import pytest
from rpa_gateway.client import GatewayClient, GatewayError


def client():return GatewayClient('http://127.0.0.1:31080','d'*48,'install','test',[])

@pytest.mark.parametrize('base',['http://public.example','http://127.0.0.1.evil.test','ftp://localhost'])
def test_sdk_requires_secure_transport(base):
    with pytest.raises(ValueError):GatewayClient(base,'x','i','v',[])

@pytest.mark.parametrize('result',[{'state':'unknown','duplicate':False},{'state':'submitting','duplicate':True},{'state':'submitted','duplicate':False}])
def test_sdk_does_not_authorize_enter_on_replayed_or_uncertain_fence(result):
    c=client()
    c.receipt=lambda *args,**kwargs:result
    with pytest.raises(GatewayError):c.begin_submit({'id':'task'},'persisted_id')


def test_sdk_authorizes_only_fresh_fence():
    c=client()
    c.receipt=lambda *args,**kwargs:{'state':'submitting','duplicate':False}
    assert c.begin_submit({'id':'task'},'persisted_id')['state']=='submitting'


def test_sdk_reuses_pool_and_never_retries_or_redirects_a_fence():
    from unittest.mock import Mock
    c=client();c.pool=Mock();c.pool.request.return_value=Mock(status=200,data=b'{"state":"submitting","duplicate":false}')
    c.request('/v1/device/heartbeat',{'session_id':'test'})
    c.request('/v1/device/tasks/t/receipts',{'state':'submitting'})
    assert c.pool.request.call_count==2
    for call in c.pool.request.call_args_list:
        assert call.kwargs['retries'] is False and call.kwargs['redirect'] is False
    assert c.pool.request.call_args.kwargs['headers']['Authorization']=='Bearer '+'d'*48


def test_sdk_transport_failure_is_never_automatically_retried():
    import urllib3
    from unittest.mock import Mock
    c=client();c.pool=Mock();c.pool.request.side_effect=urllib3.exceptions.ProtocolError('connection lost')
    with pytest.raises(GatewayError,match='uncertain'):c.request('/fence',{'state':'submitting'})
    c.pool.request.assert_called_once()


def test_sdk_reconnect_does_not_block_on_a_threading_event():
    import asyncio,json,threading
    from unittest.mock import Mock,AsyncMock,MagicMock,patch
    c=client();c.session_id='session';c.hello=Mock()
    stop=threading.Event();stop.wait=Mock(side_effect=AssertionError('Blocking wait must not run on the event loop'))
    ws=Mock();ws.send=AsyncMock();ws.recv=AsyncMock(return_value=json.dumps({'type':'tasks_changed','tasks':[],'cursor':1}))
    manager=MagicMock();manager.__aenter__=AsyncMock(return_value=ws);manager.__aexit__=AsyncMock(return_value=None)
    async def scenario():
        with patch('websockets.connect',side_effect=[ConnectionError('one drop'),manager]) as connect,patch('asyncio.sleep',new_callable=AsyncMock):
            stream=c.notifications(stop)
            try:
                message=await asyncio.wait_for(anext(stream),timeout=2)
                assert message['cursor']==1 and connect.call_count==2
                stop.wait.assert_not_called()
            finally:await stream.aclose()
    asyncio.run(scenario());c.close()
