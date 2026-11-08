"""Scoped authoritative callbacks for managed native deliveries; no desktop actions."""
import json
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler


class ManagedBackend:
    def __init__(self, base_url, token):
        self.base = base_url.rstrip('/')
        self.token = token
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs): return None
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def request(self, operation, payload, **values):
        raw = json.dumps(dict(payload=payload, **values)).encode()
        request = Request(self.base+'/'+operation, data=raw, headers={
            'Content-Type':'application/json', 'Authorization':'Bearer '+self.token})
        with self.opener.open(request, timeout=5) as response:
            data=response.read(1024*1024+1)
            if len(data)>1024*1024:raise ValueError('Managed callback exceeds limit')
            return json.loads(data)

    def verify(self, payload):
        result=self.request('verify',payload)
        return result.get('valid') is True and result.get('source_fingerprint')==payload['context']['source_fingerprint']
