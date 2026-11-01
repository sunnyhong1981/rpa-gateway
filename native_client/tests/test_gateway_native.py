import json,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
CLIENT_SOURCE=Path(os.environ.get('RPA_GATEWAY_CLIENT_SOURCE',ROOT.parents[1]/'wecom-cs-mano'))
sys.path[:0]=[str(ROOT),str(CLIENT_SOURCE)]
from rpa_gateway_bridge import GatewayBridge
from gateway_mac_desktop import GatewayMacDesktop
from mac_desktop import MacDesktop,DesktopNotReady


class NativeFenceTests(unittest.TestCase):
    def bridge(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        config={'base_url':'https://test','account_id':'1','inbox_id':'469','inbox_identifier':'test','hmac_token':'test',
                'group_name':'RPA测试','collection_mode':'official_archive','archive_group_id':'room',
                'gateway_config_path':str(Path(temp.name)/'gateway.json')}
        Path(config['gateway_config_path']).write_text(json.dumps({'base_url':'https://test/gateway','device_token':'dummy',
            'installation_id':'test','runtime_version':'pilot','inbox_id':'469'}))
        client=Mock();client.session_id='session';client.request.return_value={'mode':'gateway','floor':100,'generation':1}
        with patch('rpa_gateway_bridge.GatewayClient',return_value=client),patch('rpa_gateway_bridge.threading.Thread'):
            bridge=GatewayBridge(config,Path(temp.name)/'state.sqlite3')
        self.addCleanup(bridge.db.close)
        task={'id':'task','lease_token':'lease','payload':{'caption':'运单号 JSE123','media_ids':[],'context':{'chatwoot_message_id':101}}}
        with bridge.db:
            bridge.db.execute('INSERT INTO outgoing(scope,message_id,content) VALUES(?,?,?)',(bridge.scope,101,'运单号 JSE123'))
            bridge.db.execute('INSERT INTO gateway_attempts(scope,message_id,task_id,lease_token,session_id,payload,state,intent_event,submit_event,entered_at,receipt_error) VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL)',
                             (bridge.scope,101,'task','lease','session',json.dumps(task),'leased','intent','submitted'))
        bridge.active_gateway_message=101
        return bridge,client

    def test_durable_mark_precedes_ack_and_enter_marker_follows_it(self):
        bridge,client=self.bridge()
        def ack(task,event):
            self.assertEqual(bridge._row(101)['state'],'intent_pending')
            self.assertIsNone(bridge._row(101)['entered_at'])
        client.begin_submit.side_effect=ack
        bridge.before_enter()
        self.assertEqual(bridge._row(101)['state'],'enter_marked')
        self.assertIsNotNone(bridge._row(101)['entered_at'])
        with self.assertRaises(RuntimeError):bridge.before_enter()
        client.begin_submit.assert_called_once()

    def test_lost_ack_never_marks_enter_or_allows_retry(self):
        bridge,client=self.bridge();client.begin_submit.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):bridge.before_enter()
        self.assertEqual(bridge._row(101)['state'],'intent_pending')
        with self.assertRaises(RuntimeError):bridge.before_enter()
        client.begin_submit.assert_called_once()

    def test_mention_picker_enter_does_not_request_send_fence(self):
        desktop=GatewayMacDesktop.__new__(GatewayMacDesktop);desktop.gateway_bridge=Mock(active_gateway_message=101)
        desktop._gateway_picker_depth=1
        with patch.object(MacDesktop,'_press') as key:desktop._press(42,36)
        desktop.gateway_bridge.before_enter.assert_not_called();key.assert_called_once_with(42,36)

    def test_group_change_during_ack_prevents_final_key(self):
        desktop=GatewayMacDesktop.__new__(GatewayMacDesktop);desktop.gateway_bridge=Mock(active_gateway_message=101)
        desktop._gateway_prepared=(None,(42,9),'field');desktop._input_value=Mock(return_value='caption')
        desktop.window=Mock(return_value=(42,10))
        with patch.object(MacDesktop,'_press') as key:
            with self.assertRaises(DesktopNotReady):desktop._press(42,36)
        key.assert_not_called()

    def test_native_batch_is_rechecked_after_gateway_ack(self):
        desktop=GatewayMacDesktop.__new__(GatewayMacDesktop);desktop.gateway_bridge=Mock(active_gateway_message=101)
        desktop._gateway_prepared=(None,(42,9),'field');desktop._input_value=Mock(return_value='caption')
        desktop.window=Mock(return_value=(42,9));desktop._image_rows=Mock();desktop._gateway_batch_check=(('arguments',),{})
        with patch.object(MacDesktop,'_batch_draft_matches',return_value=False),patch.object(MacDesktop,'_press') as key:
            with self.assertRaises(DesktopNotReady):desktop._press(42,36)
        key.assert_not_called()

    def test_non_gateway_sends_preserve_native_path(self):
        desktop=GatewayMacDesktop.__new__(GatewayMacDesktop);desktop.gateway_bridge=Mock(active_gateway_message=None)
        with patch.object(MacDesktop,'_press') as key:desktop._press(42,36)
        desktop.gateway_bridge.before_enter.assert_not_called();key.assert_called_once()

    def test_desktop_released_only_after_native_return_group_and_empty_input(self):
        desktop=GatewayMacDesktop.__new__(GatewayMacDesktop);desktop.gateway_bridge=Mock(active_gateway_message=101)
        desktop._gateway_prepared=(None,(42,9),'field');desktop.window=Mock(return_value=(42,9))
        desktop._input_value=Mock(return_value='');desktop._image_rows=Mock()
        with patch.object(MacDesktop,'send_images',return_value='frame'):
            self.assertEqual(desktop.send_images(['original'],desktop._gateway_prepared),'frame')
        desktop.gateway_bridge.verify_desktop_submission.assert_called_once()
        desktop.gateway_bridge.verify_desktop_submission.reset_mock();desktop._input_value.return_value='remaining draft'
        with patch.object(MacDesktop,'send_images',return_value='frame'):
            with self.assertRaises(DesktopNotReady):desktop.send_images(['original'],desktop._gateway_prepared)
        desktop.gateway_bridge.verify_desktop_submission.assert_not_called()

    def test_release_is_not_emitted_without_durable_native_verification(self):
        bridge,client=self.bridge();bridge.before_enter();bridge._submitted(101)
        states=[call.args[1]['state'] for call in client.request.call_args_list if len(call.args)>1 and isinstance(call.args[1],dict) and 'state' in call.args[1]]
        self.assertEqual(states,['submitted'])
        self.assertEqual(bridge._row(101)['desktop_released'],0)

    def test_lost_release_ack_retries_same_identity_without_another_enter(self):
        bridge,client=self.bridge();bridge.before_enter();bridge.verify_desktop_submission()
        release_ids=[]
        def request(path,body=None):
            if body and body.get('state')=='desktop_released':
                release_ids.append(body['event_id'])
                if len(release_ids)==1:raise TimeoutError()
            return {'state':'submitted'}
        client.request.side_effect=request
        bridge._submitted(101)
        self.assertEqual(bridge._row(101)['state'],'submitted')
        self.assertEqual(bridge._row(101)['desktop_released'],0)
        entered=bridge._row(101)['entered_at']
        bridge._report_submitted(bridge._row(101))
        self.assertEqual(release_ids,['release-submitted','release-submitted'])
        self.assertEqual(bridge._row(101)['desktop_released'],1)
        self.assertEqual(bridge._row(101)['entered_at'],entered)
        client.begin_submit.assert_called_once()

    def test_unfenced_success_callback_cannot_release_desktop(self):
        bridge,_=self.bridge()
        with self.assertRaises(RuntimeError):bridge.verify_desktop_submission()
        self.assertEqual(bridge._row(101)['desktop_verified'],0)

    def test_new_gateway_reply_precedes_slow_legacy_poll(self):
        import time
        from rpa_gateway_bridge import LegacyBridge
        bridge,_=self.bridge();bridge.next_route=time.monotonic()+60;bridge.next_heartbeat=time.monotonic()+15
        bridge._poll_gateway=Mock(return_value=[{'message_id':102}]);bridge.dirty.set()
        with patch.object(LegacyBridge,'poll',side_effect=AssertionError('Legacy RPC blocked fast claim')):
            self.assertEqual(bridge.poll(),[{'message_id':102}])
        bridge._poll_gateway.assert_called_once()

    def test_empty_queue_hint_does_not_disable_periodic_claim_compensation(self):
        import time
        bridge,client=self.bridge();bridge._set(101,'confirmed');bridge.notice_has_queue=False
        bridge.next_claim=time.monotonic()-1;client.claim.return_value=None
        self.assertEqual(bridge._poll_gateway(False,time.monotonic()),[])
        client.claim.assert_called_once()
        bridge._poll_gateway(False,time.monotonic())
        client.claim.assert_called_once()

    def test_busy_gateway_still_checks_legacy_files_on_bounded_interval(self):
        import time
        from rpa_gateway_bridge import LegacyBridge
        bridge,_=self.bridge();bridge.next_route=time.monotonic()+60;bridge.next_heartbeat=time.monotonic()+15
        bridge.next_source_poll=time.monotonic()-1;bridge._poll_gateway=Mock(return_value=[{'message_id':102}])
        with patch.object(LegacyBridge,'poll',return_value=[]) as legacy:
            self.assertEqual(bridge.poll(),[{'message_id':102}])
        legacy.assert_called_once()
