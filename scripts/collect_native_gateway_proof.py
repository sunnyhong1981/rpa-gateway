"""Read-only local proof. Does not expose credentials or replay tasks."""
import json,sqlite3,hashlib
from pathlib import Path
root=Path('/Users/sunny/AI/QUNL-RPA-mac')
with sqlite3.connect('file:'+str(root/'data/state.sqlite3')+'?mode=ro',uri=True) as db:
 db.row_factory=sqlite3.Row
 rows=db.execute('SELECT message_id,task_id,state,entered_at,receipt_error,payload FROM gateway_attempts ORDER BY message_id').fetchall()
 data=[]
 for row in rows:
  task=json.loads(row['payload'])
  data.append({'message_id':row['message_id'],'task_id':row['task_id'],'state':row['state'],'entered_at':row['entered_at'],
    'waybill':task['payload']['waybill'],'media_count':len(task.get('media',[])),
    'media':[{'sha256':item['sha256'],'md5':item['md5'],'size':item['size'],'mime':item['mime']} for item in task.get('media',[])],
    'error_type':row['receipt_error']})
result={'local_gateway_journal':data,'runtime':json.loads((root/'current/logs/rpa-status.json').read_text())}
out=Path('/Users/sunny/Downloads/facebook-chat/outputs/rpa-gateway-mac-pilot-20261029/native-final-proof.json')
out.write_text(json.dumps(result,ensure_ascii=False,indent=2))
print(json.dumps({'gateway_entries':len(data),'confirmed':sum(item['state']=='confirmed' for item in data),
                  'current_version':result['runtime']['runtime_version']},ensure_ascii=False))
