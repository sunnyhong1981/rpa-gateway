"""Durable, deduplicated WeCom robot alerts. Secrets never appear in logs or payloads."""
import hashlib,json,re,time
from urllib.parse import urlparse,parse_qs
from urllib.request import Request,HTTPRedirectHandler,build_opener


def validate_webhook(value):
    if not value:return False
    u=urlparse(value);q=parse_qs(u.query)
    if u.scheme!='https' or u.hostname!='qyapi.weixin.qq.com' or u.path!='/cgi-bin/webhook/send' or set(q)!={'key'}:
        raise ValueError('A direct official WeCom robot webhook is required')
    if len(q['key'])!=1 or not re.fullmatch(r'[A-Za-z0-9_-]{20,128}',q['key'][0]):raise ValueError('Invalid robot webhook key')
    return True


class Alerts:
    def __init__(self,store,webhook='',channel='wecom_robot'):
        validate_webhook(webhook);self.store=store;self.webhook=webhook;self.channel=channel

    def ingest(self,payload):
        alerts=payload.get('alerts')
        if not isinstance(alerts,list) or not 1<=len(alerts)<=100:raise ValueError('Invalid Alertmanager batch')
        accepted=0
        with self.store.transaction() as db:
            for alert in alerts:
                fingerprint=alert.get('fingerprint','');status=alert.get('status')
                if not re.fullmatch(r'[a-f0-9]{16,64}',fingerprint) or status not in ('firing','resolved'):
                    raise ValueError('Invalid alert identity')
                labels=alert.get('labels') or {};annotations=alert.get('annotations') or {}
                summary=str(annotations.get('summary') or labels.get('alertname') or 'RPA网关告警')[:500]
                identity=[fingerprint,status,str(alert.get('startsAt',''))[:64],str(alert.get('endsAt',''))[:64] if status=='resolved' else '']
                key=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
                content={'fingerprint':fingerprint,'status':status,'summary':summary,
                         'instance':str(labels.get('instance',''))[:160],'severity':str(labels.get('severity','warning'))[:20]}
                if status=='resolved':
                    delivered=db.execute("SELECT 1 FROM alert_deliveries WHERE fingerprint=%s AND state IN ('delivered','published','publish_unknown') LIMIT 1",(fingerprint,)).fetchone()
                    db.execute("UPDATE alert_deliveries SET state='superseded' WHERE fingerprint=%s AND state='queued'",(fingerprint,))
                    if not delivered:continue
                row=db.execute('INSERT INTO alert_deliveries(id,fingerprint,payload,state,created,next_at) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING RETURNING id',
                               (key,fingerprint,json.dumps(content,ensure_ascii=False),'queued',self.store.clock(),self.store.clock())).fetchone()
                accepted+=bool(row)
        return {'accepted':accepted,'delivery_configured':bool(self.webhook) or self.channel=='rpa_monitor'}

    def send_one(self):
        if not self.webhook:return False
        now=self.store.clock()
        with self.store.transaction() as db:
            row=db.execute("SELECT * FROM alert_deliveries WHERE state='queued' AND next_at<=%s ORDER BY created LIMIT 1 FOR UPDATE SKIP LOCKED",(now,)).fetchone()
            if not row:return False
            # Keep a bounded send inside a locked transaction; retries may repeat an alert after an ambiguous remote response.
            body=json.loads(row['payload']);title='告警' if body['status']=='firing' else '恢复'
            text='**RPA网关'+title+'**\n> '+body['summary']+'\n级别：'+body['severity']+'\n实例：'+body['instance']+'\n事件：'+body['fingerprint'][:12]
            text=text.encode()[:3800].decode('utf-8','ignore')
            data=json.dumps({'msgtype':'markdown','markdown':{'content':text}},ensure_ascii=False).encode()
            class NoRedirect(HTTPRedirectHandler):
                def redirect_request(self,*args,**kwargs):return None
            error=''
            try:
                with build_opener(NoRedirect()).open(Request(self.webhook,data=data,headers={'Content-Type':'application/json'}),timeout=5) as response:
                    result=json.load(response)
                if result.get('errcode')!=0:raise RuntimeError('Robot rejected alert')
            except Exception as exc:error=type(exc).__name__
            if error:
                attempts=row['attempts']+1
                db.execute('UPDATE alert_deliveries SET attempts=%s,state=%s,next_at=%s,last_error=%s WHERE id=%s',
                    (attempts,'failed' if attempts>=8 else 'queued',now+min(900,2**attempts),error,row['id']))
            else:db.execute("UPDATE alert_deliveries SET state='delivered',delivered_at=%s,last_error='' WHERE id=%s",(self.store.clock(),row['id']))
        return not error
