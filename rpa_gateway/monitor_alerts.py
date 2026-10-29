"""Gateway alerts through the existing, explicitly selected RPA monitor group."""
import json,time
from urllib.request import Request,build_opener,HTTPRedirectHandler
import psycopg
from psycopg.rows import dict_row


class MonitorAlerts:
    def __init__(self,store,config,source_dsn):
        if (config['account_id'],config['inbox_id'],config['conversation_id'],config['group_name'])!=(871,491,2090,'RPA漏回监控群'):
            raise ValueError('Monitor transport is restricted to the approved existing RPA monitor group')
        self.store,self.config,self.source_dsn=store,config,source_dsn
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):return None
        self.opener=build_opener(NoRedirect())

    def existing(self,alert_id):
        with psycopg.connect(self.source_dsn,row_factory=dict_row,connect_timeout=3) as db:
            return db.execute("SELECT id,attributes FROM rpa_gateway_source.monitor_alert_messages WHERE attributes->>'rpa_gateway_alert_id'=%s ORDER BY id LIMIT 1",(alert_id,)).fetchone()

    def publish(self,row):
        body=json.loads(row['payload']);title='告警' if body['status']=='firing' else '恢复'
        content='RPA网关'+title+'\n'+body['summary']+'\n级别：'+body['severity']+'\n实例：'+body['instance']+'\n事件：'+body['fingerprint'][:12]
        content=content.encode()[:3800].decode('utf-8','ignore')
        payload={'content':content,'message_type':'outgoing','private':False,
                 'content_attributes':{'rpa_gateway_alert_id':row['id'],'rpa_gateway_alert_status':body['status']}}
        url=self.config['base_url'].rstrip('/')+'/api/v1/accounts/871/conversations/738/messages'
        request=Request(url,data=json.dumps(payload,ensure_ascii=False).encode(),headers={'Content-Type':'application/json',
            'api_access_token':self.config['api_token'],'Host':self.config['host_header'],'X-Forwarded-Proto':'https'})
        with self.opener.open(request,timeout=5) as response:result=json.load(response)
        if not isinstance(result.get('id'),int):raise RuntimeError('Monitor API returned no message identity')
        return result['id']

    def one(self):
        now=self.store.clock()
        with self.store.transaction() as db:
            row=db.execute("SELECT * FROM alert_deliveries WHERE state IN ('queued','published','publish_unknown') AND next_at<=%s ORDER BY created LIMIT 1 FOR UPDATE SKIP LOCKED",(now,)).fetchone()
            if not row:return False
            remote=self.existing(row['id'])
            if remote:
                delivery=(remote['attributes'] or {}).get('rpa_delivery') or {}
                status='delivered' if delivery.get('status')=='delivered' else 'published'
                db.execute('UPDATE alert_deliveries SET state=%s,remote_message_id=%s,next_at=%s,delivered_at=%s,last_error=%s WHERE id=%s',
                    (status,remote['id'],now+2,now if status=='delivered' else None,delivery.get('status',''),row['id']))
            elif row['state']=='queued':
                try:message_id=self.publish(row)
                except Exception as exc:
                    # An ambiguous API response is reconciled, never blindly posted a second time.
                    db.execute("UPDATE alert_deliveries SET state='publish_unknown',attempts=attempts+1,next_at=%s,last_error=%s WHERE id=%s",(now+5,type(exc).__name__,row['id']))
                else:db.execute("UPDATE alert_deliveries SET state='published',remote_message_id=%s,next_at=%s WHERE id=%s",(message_id,now+1,row['id']))
            else:db.execute('UPDATE alert_deliveries SET next_at=%s WHERE id=%s',(now+5,row['id']))
        return True


def main():
    import os
    from pathlib import Path
    from .app import Settings,make_store
    store=make_store(Settings.from_env(),pool_size=1)
    config=json.loads(Path(os.environ['RPA_GATEWAY_MONITOR_ALERT_CONFIG_FILE']).read_text())
    source_dsn=Path(os.environ['RPA_GATEWAY_SOURCE_DATABASE_URL_FILE']).read_text().strip()
    sender=MonitorAlerts(store,config,source_dsn)
    try:
        while True:
            error=''
            try:sender.one()
            except Exception as exc:error=type(exc).__name__
            with store.transaction() as db:
                db.execute("INSERT INTO worker_health(name,updated,error_type) VALUES('alerts',%s,%s) ON CONFLICT(name) DO UPDATE SET updated=EXCLUDED.updated,error_type=EXCLUDED.error_type",(store.clock(),error))
            time.sleep(1)
    finally:store.close()

if __name__=='__main__':main()
