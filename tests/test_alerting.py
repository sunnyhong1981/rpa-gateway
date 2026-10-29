import json
from unittest.mock import Mock
from rpa_gateway.alerting import Alerts
from rpa_gateway.monitor_alerts import MonitorAlerts


def alert(status='firing'):
    return {'alerts':[{'fingerprint':'a'*16,'status':status,'startsAt':'2026-10-29T00:00:00Z','endsAt':'2026-10-29T00:05:00Z',
        'labels':{'severity':'warning','instance':'gateway-a'},'annotations':{'summary':'测试告警，不发送真实消息'}}]}


def test_alert_ingest_idempotent_and_rapid_recovery_coalesces(env):
    manager=Alerts(env.app.state.store,channel='rpa_monitor')
    assert manager.ingest(alert())['accepted']==1
    assert manager.ingest(alert())['accepted']==0
    assert manager.ingest(alert('resolved'))['accepted']==0
    with env.app.state.store.transaction() as db:assert db.execute('SELECT state FROM alert_deliveries').fetchone()['state']=='superseded'


def test_published_alert_gets_resolution_even_if_rpa_not_delivered_yet(env):
    manager=Alerts(env.app.state.store,channel='rpa_monitor');manager.ingest(alert())
    with env.app.state.store.transaction() as db:db.execute("UPDATE alert_deliveries SET state='published'")
    assert manager.ingest(alert('resolved'))['accepted']==1


def test_monitor_reconciles_ambiguous_publish_without_duplicate_post(env):
    store=env.app.state.store;Alerts(store,channel='rpa_monitor').ingest(alert())
    config={'account_id':871,'inbox_id':491,'conversation_id':2090,'group_name':'RPA漏回监控群'}
    sender=MonitorAlerts(store,config,'not-used');sender.existing=Mock(return_value=None);sender.publish=Mock(side_effect=TimeoutError())
    sender.one();env.clock.advance(6);sender.one()
    sender.publish.assert_called_once()
    sender.existing.return_value={'id':123,'attributes':{'rpa_delivery':{'status':'delivered'}}}
    env.clock.advance(6);sender.one()
    with store.transaction() as db:assert db.execute('SELECT state,remote_message_id FROM alert_deliveries').fetchone()=={'state':'delivered','remote_message_id':123}


def test_monitor_destination_is_not_user_selectable(env):
    import pytest
    with pytest.raises(ValueError):MonitorAlerts(env.app.state.store,{'account_id':871,'inbox_id':469,'conversation_id':1370,'group_name':'RPA测试'},'unused')
