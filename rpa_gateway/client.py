"""Opt-in transport SDK. Does not import, start, or modify a desktop RPA worker."""
import hashlib
import json
import uuid
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


class GatewayError(RuntimeError):
    pass


class GatewayClient:
    def __init__(self, base_url, token, installation_id, runtime_version, capabilities):
        parsed=urlparse(base_url)
        if parsed.scheme!='https' and not (parsed.scheme=='http' and parsed.hostname in ('localhost','127.0.0.1','::1')):
            raise ValueError('HTTPS is required except on loopback/SSH tunnels')
        self.base=base_url.rstrip('/')
        self.token=token
        self.hello_body={'protocol':1,'installation_id':installation_id,'runtime_version':runtime_version,'capabilities':capabilities}
        self.session_id=None

    def request(self,path,body=None):
        request=Request(self.base+path,data=None if body is None else json.dumps(body).encode(),headers={
            'Authorization':'Bearer '+self.token,'Content-Type':'application/json'},method='GET' if body is None else 'POST')
        # Never follow a redirect carrying a credential to another host.
        from urllib.request import HTTPRedirectHandler, build_opener
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs): return None
        try:
            with build_opener(NoRedirect()).open(request,timeout=10) as response:
                return json.load(response)
        except HTTPError as exc:
            raise GatewayError(f'Gateway HTTP {exc.code}; task must not be sent without a successful fence') from None

    def hello(self):
        result=self.request('/v1/device/hello',self.hello_body)
        self.session_id=result['session_id']
        return result

    def heartbeat(self,*,worker_alive,desktop_unlocked,wecom_logged_in,auto_send):
        return self.request('/v1/device/heartbeat',{'session_id':self.session_id,'worker_alive':worker_alive,
            'desktop_unlocked':desktop_unlocked,'wecom_logged_in':wecom_logged_in,'auto_send':auto_send})

    def claim(self):
        return self.request('/v1/device/tasks/claim',{'session_id':self.session_id})['task']

    def receipt(self,task,state,*,event_id=None,reason=''):
        return self.request(f"/v1/device/tasks/{task['id']}/receipts",{
            'session_id':self.session_id,'lease_token':task['lease_token'],
            'event_id':event_id or uuid.uuid4().hex,'state':state,'reason':reason})

    def download(self,task,media):
        from urllib.request import HTTPRedirectHandler, build_opener
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs): return None
        if media not in task['media']:
            raise GatewayError('Media is not part of the immutable task manifest')
        url=self.base+f"/v1/device/tasks/{task['id']}/media/{media['id']}"
        request=Request(url,headers={'Authorization':'Bearer '+self.token})
        with build_opener(NoRedirect()).open(request,timeout=15) as response:
            data=response.read(media['size']+1)
        if len(data)!=media['size'] or hashlib.sha256(data).hexdigest()!=media['sha256']:
            raise GatewayError('Original image integrity check failed')
        return data

    def begin_submit(self,task,event_id):
        """Caller must persist event_id locally and retry that exact ID on transport failure.

        Only after this returns may the adapter perform ONE native UI Enter.
        On any ambiguous response, stop; reconnect/reconcile without resending.
        """
        result=self.receipt(task,'submitting',event_id=event_id)
        if result['state']!='submitting' or result.get('duplicate'):
            raise GatewayError('Submission is no longer authorized; do not press Enter')
        return result
