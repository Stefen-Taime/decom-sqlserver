import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
from contextlib import contextmanager
import pika
from common import digest,encoded,load_contract,paths,require
from topology import VHOST,EXCHANGE,ROUTING_KEY


def contract():
    path=os.environ.get('INVOICE_CONTRACT_FILE')
    value=json.loads(Path(path).read_text()) if path else load_contract('invoice_details')
    require(value['dataset']=='invoice_details' and value['version']==1,'Only invoice_details/v1 is supported')
    return value


def message_for(marker,publication,spec):
    require(marker['protocol']=='full-export/v1' and marker['dataset']==spec['dataset']
            and marker['version']==spec['version'],'Unexpected publication contract')
    root,key=paths(spec,marker['run_id'])
    require(publication==key and marker['manifest_key']==root+'/manifest.json','Unexpected publication path')
    require(re.fullmatch('[0-9a-f]{64}',marker['manifest_sha256']),'Invalid manifest hash')
    body=dict(type='export.ready/v1',dataset=spec['dataset'],contract_version=spec['version'],
              run_id=marker['run_id'],bucket=spec['bucket'],publication=publication,
              manifest_sha256=marker['manifest_sha256'])
    return {'event_id':digest(encoded(body)),**body}


def parse_message(body,properties,spec):
    require(len(body)<=16384,'Oversized notification')
    event=json.loads(body)
    require(isinstance(event,dict),'Notification must be an object')
    root,_=paths(spec,event['run_id'])
    marker=dict(protocol='full-export/v1',dataset=event['dataset'],version=event['contract_version'],
                run_id=event['run_id'],manifest_key=root+'/manifest.json',manifest_sha256=event['manifest_sha256'])
    require(event==message_for(marker,event['publication'],spec),'Invalid notification contract or event ID')
    require(properties.message_id==event['event_id'] and properties.content_type=='application/json',
            'Notification metadata mismatch')
    return event,marker


def connection(credentials=None):
    values=json.loads(Path(credentials or os.environ['RABBIT_CREDENTIALS_FILE']).read_text())
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.environ.get('RABBIT_HOST','rabbitmq'),virtual_host=os.environ.get('RABBIT_VHOST',VHOST),
        credentials=pika.PlainCredentials(values['username'],values['password']),heartbeat=30,
        blocked_connection_timeout=30,socket_timeout=10,stack_timeout=20,connection_attempts=1))


def publish(channel,event,routing_key=ROUTING_KEY):
    channel.basic_publish(exchange=EXCHANGE,routing_key=routing_key,body=encoded(event),mandatory=True,
        properties=pika.BasicProperties(content_type='application/json',delivery_mode=2,
            message_id=event['event_id'],type='export.ready/v1'))


def database(path):
    db=sqlite3.connect(path,timeout=10)
    db.row_factory=sqlite3.Row
    # One writer per local file. Avoid WAL-reset races in the base image's SQLite 3.40.1.
    require(db.execute('PRAGMA journal_mode=DELETE').fetchone()[0]=='delete','Unexpected journal mode')
    db.execute('PRAGMA synchronous=EXTRA')
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA busy_timeout=10000')
    return db


@contextmanager
def exclusive(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    with Path(path).open('a') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another worker owns this local state') from None
        yield


def fault(point):
    if os.environ.get('MESSAGING_TEST_MODE')=='1' and os.environ.get('MESSAGING_FAULT')==point:
        print(json.dumps({'injected_crash':point}),flush=True)
        os._exit(86)
