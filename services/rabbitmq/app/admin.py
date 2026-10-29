"""Operator/probe HTTP API helper. This credential is never mounted into runtime workers."""
import base64
import json
import os
from pathlib import Path
import urllib.request
from urllib.parse import quote


def api(method,path,body=None):
    values=json.loads(Path(os.environ.get('RABBIT_ADMIN_FILE','/run/secrets/admin')).read_text())
    token=base64.b64encode((values['username']+':'+values['password']).encode()).decode()
    request=urllib.request.Request(os.environ.get('RABBIT_ADMIN_URL','http://rabbitmq:15672')+'/api/'+path,
        data=json.dumps(body).encode() if body is not None else None,method=method,
        headers={'Authorization':'Basic '+token,'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=20) as response:
        data=response.read()
        return json.loads(data) if data else None


def escaped(value): return quote(value,safe='')
