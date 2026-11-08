"""Use only the scoped Mac pilot credential; no native desktop or send operations."""
import asyncio,json,sys,time
from pathlib import Path
root=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(root/'native_client'),str(root/'native_client/gateway_vendor')]
from gateway_transport.client import GatewayClient,GatewayError
import websockets
config=json.loads(Path('/Users/sunny/AI/QUNL-RPA-mac/data/gateway-pilot/gateway.json').read_text())
client=GatewayClient(config['base_url'],config['device_token'],config['installation_id'],config['runtime_version'],
                     ['text_v1','caption_images_v1','official_identity_v1'])
hello=client.hello()
client.heartbeat(worker_alive=True,desktop_unlocked=True,wecom_logged_in=True,auto_send=False)
route=client.request('/v1/device/source-route?inbox_id=469&session_id='+client.session_id)
assert route['mode']=='shadow'
try:client.claim();raise RuntimeError('Shadow device unexpectedly permitted a claim')
except GatewayError as error:
 if 'HTTP 409' not in str(error):raise
async def probe():
 async with websockets.connect(config['base_url'].replace('https://','wss://')+'/v1/device/notifications',ssl=client.ssl_context,open_timeout=15) as ws:
  await ws.send(json.dumps({'token':config['device_token'],'session_id':client.session_id}))
  snapshot=json.loads(await asyncio.wait_for(ws.recv(),15));assert snapshot['type']=='tasks_changed'
  await ws.send('ping');assert json.loads(await asyncio.wait_for(ws.recv(),15))['type']=='pong'
asyncio.run(probe())
print(json.dumps({'connected':True,'public_https':True,'public_wss':True,'mode':route['mode'],'source_floor':route['floor'],
                  'send_permitted':False,'native_desktop_touched':False,'existing_daemon_restarted':False}))
