"""One real test-group question via the existing native adapter; always restore its worker."""
import fcntl,json,sys,time,argparse,re
from pathlib import Path
root=Path('/Users/sunny/AI/QUNL-RPA-mac/current').resolve()
if root.name!='2026.10.29.90':raise RuntimeError('Unexpected active pilot version')
sys.path.insert(0,str(root))
from scripts.rpa_service import Service
from mac_group_messages import GroupReader
from mac_desktop import MacDesktop
from mac_group_navigation import MacGroupNavigator
from rpa_chatwoot import Bridge
service=Service(root);settings=service.settings();status=service.inspect()
if not status['running'] or status['ownership']!='verified':raise RuntimeError('Native worker ownership not verified')
manifest=json.loads(Path(settings['config']).read_text())
config=None
for group in manifest['groups']:
 p=Path(group['config']);p=p if p.is_absolute() else Path(settings['config']).parent/p
 value=json.loads(p.read_text())
 if str(value['inbox_id'])=='469':config=value;break
if config is None:raise RuntimeError('Bound test inbox missing')
parser=argparse.ArgumentParser();parser.add_argument('--waybill',default='JSEJG0625531376YQ');parser.add_argument('--second',default='');args=parser.parse_args()
if not re.fullmatch('JSE[A-Z0-9]{8,40}',args.waybill):raise ValueError('Invalid test waybill')
if args.second and not re.fullmatch('JSE[A-Z0-9]{8,40}',args.second):raise ValueError('Invalid second waybill')
content='【网关多单验收】@AI客服 '+args.waybill+('\n'+args.second if args.second else '')+' 签收图'
start=time.time();stopped=False
try:
 service.stop(timeout=20);stopped=True
 with (Path.home()/'.qunl-rpa-wecom-desktop.lock').open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  reader=GroupReader(config);desktop=MacDesktop(reader,Path('/Users/sunny/Downloads/facebook-chat/outputs/rpa-gateway-mac-pilot-20261029/query-window.png'))
  bridge=Bridge(config,settings['state'])
  try:
   reader.positioned_source=desktop.message_records;reader.incoming_text=desktop.incoming_text;reader.native_source=bridge.native_text
   MacGroupNavigator([config['group_name']],activate_wecom=False).select(desktop)
   prepared=desktop.ready_to_send()
   desktop.send(content,prepared)
   result={'query_sent':True,'group':config['group_name'],'content':content,'at':start,
           'send_actions':desktop.last_send_counters.get('send_actions'),'source':'real_native_wecom_test_account'}
  finally:bridge.db.close()
finally:
 if stopped:
  restarted=service.start(timeout=20)
  if 'result' in locals():result['worker_restored']=True;result['pid']=restarted['pid']
print(json.dumps(result,ensure_ascii=False))
