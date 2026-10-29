"""Scoped existing Chatwoot inbox APIs. No Rails source edits or broad account token."""
import hashlib
import hmac
import json
import time
from urllib.parse import quote,unquote
from urllib.request import Request,build_opener,HTTPRedirectHandler


class SourceUnavailable(RuntimeError):pass


class Chatwoot:
    def __init__(self,config):
        self.config=config
        self.base=config['base_url'].rstrip('/')+'/public/api/v1/inboxes/'+quote(config['inbox_identifier'],safe='')
        self.messages='/contacts/'+quote(config['contact_source'],safe='')+'/conversations/'+str(config['public_conversation_id'])+'/messages'
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):return None
        self.opener=build_opener(NoRedirect())

    def request(self,suffix,body=None,headers=None,raw=False):
        stamp=str(int(time.time()))
        data=None if body is None else json.dumps(body,separators=(',',':'),ensure_ascii=False).encode()
        signed=stamp.encode()+b'.'+suffix.encode()+b'.'+(data or b'')
        values={'X-Rpa-Timestamp':stamp,'X-Rpa-Signature':hmac.new(self.config['hmac_token'].encode(),signed,hashlib.sha256).hexdigest()}
        if data is not None:values['Content-Type']='application/json'
        if self.config.get('host_header'):
            values['Host']=self.config['host_header']
            values['X-Forwarded-Proto']='https'
        values.update(headers or {})
        request=Request(self.base+suffix,data=data,headers=values)
        try:
            with self.opener.open(request,timeout=15) as response:
                if raw:
                    data=response.read(5*1024*1024+1)
                    digest=response.headers.get('X-Rpa-File-Sha256','')
                    if not data or len(data)>5*1024*1024 or hashlib.sha256(data).hexdigest()!=digest:
                        raise SourceUnavailable('Attachment integrity mismatch')
                    return data,unquote(response.headers.get('X-Rpa-Filename',''))
                return json.load(response)
        except SourceUnavailable:raise
        except Exception as exc:raise SourceUnavailable('Chatwoot '+type(exc).__name__) from None

    def identity(self,source):
        payload={'operation':'resolve','source_message_id':source['id'],'sender_id':source['attributes']['rpa_sender_id'],
                 'room_id':source['attributes']['rpa_archive_room_id'],
                 'source_sha256':hashlib.sha256(source['content'].encode()).hexdigest()}
        result=self.request('/member_identity',payload)
        if (result.get('status')!='verified' or result.get('sender_id')!=payload['sender_id']
            or result.get('room_id')!=payload['room_id'] or result.get('source_message_id')!=source['id']
            or not str(result.get('origin','')).startswith('official_') or not result.get('sender_name')):
            raise SourceUnavailable('Official source identity is not verified')
        return result

    def attachment(self,message_id,attachment_id):
        return self.request(self.messages+'/'+str(message_id)+'/rpa_file/'+str(attachment_id),raw=True)

    def receipt(self,message_id,status,caption='',after=None,reason=None,send_not_attempted=None):
        body={'status':status,'client_id':self.config['delivery_client_id'],'group_name':self.config['group_name']}
        if after is not None:body['delivered_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(after))
        if status=='delivered':body['reconciliation']={'method':'exact_content','content_sha256':hashlib.sha256(caption.encode()).hexdigest()}
        if reason:body['reason']=reason[:500]
        if send_not_attempted is not None:body['send_not_attempted']=send_not_attempted
        return self.request(self.messages+'/'+str(message_id)+'/rpa_receipt',body)
