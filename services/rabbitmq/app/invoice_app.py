"""Invoice application: validate a full export and atomically replace its local read model."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import signal
import time
from botocore.exceptions import ClientError
import pyarrow as pa
from common import Store,atomic_json,digest,encoded,require,utcnow,verify_manifest
from messaging import contract,connection,database,exclusive,fault,parse_message
from topology import QUEUE


def state_path():
    directory=Path(os.environ.get('APP_STATE_DIR','/state'))
    directory.mkdir(parents=True,exist_ok=True)
    return directory


def initialize(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS deliveries(event_id TEXT PRIMARY KEY,run_id TEXT UNIQUE NOT NULL,
      publication TEXT UNIQUE NOT NULL,body TEXT NOT NULL,captured_at TEXT NOT NULL,
      statistics TEXT NOT NULL,decision TEXT NOT NULL,committed_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS active_snapshot(id INTEGER PRIMARY KEY CHECK(id=1),
      event_id TEXT NOT NULL REFERENCES deliveries(event_id),captured_at TEXT NOT NULL,run_id TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS invoice_lines(invoice_line_id INTEGER PRIMARY KEY,invoice_id INTEGER NOT NULL,
      customer_id INTEGER NOT NULL,invoice_date TEXT NOT NULL,row_json TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS invoice_lines_invoice ON invoice_lines(invoice_id);
    CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,body_sha256 TEXT NOT NULL,
      outcome TEXT NOT NULL,error_type TEXT,finished_at TEXT NOT NULL);
    ''')


def record_attempt(body,outcome,error=None):
    with database(state_path()/'invoices.sqlite3') as db:
        initialize(db)
        db.execute('INSERT INTO attempts(body_sha256,outcome,error_type,finished_at) VALUES(?,?,?,?)',
                   (digest(body),outcome,error,utcnow()))


def apply_delivery(body,properties):
    spec=contract();event,expected_marker=parse_message(body,properties,spec)
    if os.environ.get('MESSAGING_TEST_MODE')=='1' and os.environ.get('MESSAGING_FAULT')=='transient':
        raise OSError('Injected transient application failure')
    path=state_path()/'invoices.sqlite3'
    with database(path) as db:
        initialize(db)
        db.execute('BEGIN IMMEDIATE')
        previous=db.execute('SELECT body FROM deliveries WHERE event_id=?',(event['event_id'],)).fetchone()
        if previous:
            require(previous['body']==encoded(event).decode(),'Conflicting duplicate')
            return {'outcome':'duplicate','event_id':event['event_id']}
        require(not db.execute('SELECT 1 FROM deliveries WHERE run_id=? OR publication=?',
                (event['run_id'],event['publication'])).fetchone(),'Conflicting delivery identity')
        store=Store()
        require(store.bucket==event['bucket'],'Unexpected bucket')
        actual=json.loads(store.get(event['publication'],limit=16384))
        require(actual==expected_marker,'Notification differs from the committed publication')
        raw_manifest=store.get(actual['manifest_key'])
        require(digest(raw_manifest)==actual['manifest_sha256'],'Manifest checksum mismatch')
        metadata=json.loads(raw_manifest)
        captured=datetime.fromisoformat(metadata['snapshot']['captured_at'])
        require(captured.tzinfo is not None,'Snapshot timestamp must have a timezone')
        captured_at=captured.astimezone(timezone.utc).isoformat(timespec='microseconds')
        active=db.execute('SELECT * FROM active_snapshot WHERE id=1').fetchone()
        newer=not active or (captured_at,event['run_id'])>(active['captured_at'],active['run_id'])
        if newer:
            db.execute('DELETE FROM invoice_lines')
        first=True
        def insert(batch):
            nonlocal first
            db.executemany('INSERT INTO invoice_lines VALUES(?,?,?,?,?)',[
                (r['invoice_line_id'],r['invoice_id'],r['customer_id'],r['invoice_date'].isoformat(),encoded(r).decode())
                for r in batch.to_pylist()])
            if first:
                first=False
                fault('during-replacement')
        manifest=verify_manifest(store,spec,actual,event['publication'],on_batch=insert if newer else None)
        require(manifest==metadata,'Manifest changed during verification')
        decision='applied' if newer else 'superseded'
        db.execute('INSERT INTO deliveries VALUES(?,?,?,?,?,?,?,?)',(
            event['event_id'],event['run_id'],event['publication'],encoded(event).decode(),captured_at,
            encoded(manifest['statistics']).decode(),decision,utcnow()))
        if newer:
            db.execute('INSERT INTO active_snapshot VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET event_id=excluded.event_id,captured_at=excluded.captured_at,run_id=excluded.run_id',
                       (event['event_id'],captured_at,event['run_id']))
        db.commit()
        fault('after-app-commit')
        return {'outcome':decision,'event_id':event['event_id'],'rows':manifest['statistics']['rows']}


def status():
    path=state_path()/'invoices.sqlite3'
    if not path.exists():
        return {'status':'not_initialized'}
    with database(path) as db:
        initialize(db)
        active=db.execute('SELECT d.* FROM active_snapshot a JOIN deliveries d USING(event_id)').fetchone()
        return {'status':'ready' if active else 'empty',
            'active':dict(active) if active else None,
            'invoice_lines':db.execute('SELECT count(*) FROM invoice_lines').fetchone()[0],
            'deliveries':db.execute('SELECT count(*) FROM deliveries').fetchone()[0],
            'attempts':[dict(r) for r in db.execute('SELECT outcome,count(*) AS count FROM attempts GROUP BY outcome')]}


def worker(once=False):
    stop=False;handled=False
    def shutdown(*_):
        nonlocal stop
        stop=True
    signal.signal(signal.SIGTERM,shutdown);signal.signal(signal.SIGINT,shutdown)
    with exclusive(state_path()/'worker.lock'):
        while not stop:
            try:
                with connection() as conn,ThreadPoolExecutor(max_workers=1) as executor:
                    channel=conn.channel();channel.basic_qos(prefetch_count=1)
                    processing=False
                    def received(ch,method,properties,body):
                        nonlocal processing
                        processing=True
                        future=executor.submit(apply_delivery,body,properties)
                        def finish():
                            nonlocal processing,handled,stop
                            try:
                                result=future.result()
                                record_attempt(body,result['outcome'])
                                if once:
                                    ch.basic_cancel(consumer_tag)
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                print(json.dumps(result),flush=True)
                            except Exception as exc:
                                permanent=isinstance(exc,(ValueError,KeyError,TypeError,pa.ArrowInvalid))
                                if isinstance(exc,ClientError):
                                    permanent=exc.response['Error']['Code'] in ('NoSuchKey','404')
                                record_attempt(body,'rejected' if permanent else 'retry',type(exc).__name__)
                                # basic.reject increments failed-delivery count in RabbitMQ >=4.3; basic.nack does not.
                                if not permanent:
                                    conn.sleep(5)
                                if once:
                                    ch.basic_cancel(consumer_tag)
                                ch.basic_reject(delivery_tag=method.delivery_tag,requeue=not permanent)
                                print(json.dumps({'outcome':'rejected' if permanent else 'retry',
                                                  'error_type':type(exc).__name__,'body_sha256':digest(body)}),flush=True)
                            processing=False;handled=True
                            if once: stop=True
                        future.add_done_callback(lambda _:conn.add_callback_threadsafe(finish) if conn.is_open else None)
                    consumer_tag=channel.basic_consume(queue=QUEUE,on_message_callback=received,auto_ack=False)
                    started=time.monotonic()
                    while not stop:
                        conn.process_data_events(time_limit=1)
                        atomic_json(state_path()/'health.json',{'status':'ok','checked_at':time.time(),'processing':processing})
                        if once and not processing and time.monotonic()-started>20:
                            break
                    if once: return
            except Exception as exc:
                atomic_json(state_path()/'health.json',{'status':'error','checked_at':time.time(),'error_type':type(exc).__name__})
                print(json.dumps({'connection_error':type(exc).__name__}),flush=True)
                if once: raise
                for _ in range(5):
                    if stop: break
                    time.sleep(1)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['watch','once','status'],default='watch',nargs='?')
    args=parser.parse_args()
    if args.action=='status': print(json.dumps(status()))
    else: worker(once=args.action=='once')
