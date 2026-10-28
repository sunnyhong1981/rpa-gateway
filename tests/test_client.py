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
