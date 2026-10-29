"""Discover committed invoice exports and publish persistent notifications with a durable outbox."""
import argparse
import json
import os
from pathlib import Path
import signal
import time
from common import Store,atomic_json,encoded,require,utcnow
from messaging import contract,connection,database,exclusive,fault,message_for,publish


def scan_and_send(replay=None):
    directory=Path(os.environ.get('DISPATCH_STATE_DIR','/state'))
    directory.mkdir(parents=True,exist_ok=True)
    spec=contract();store=Store()
    require(store.bucket==spec['bucket'],'Unexpected bucket')
    with exclusive(directory/'dispatcher.lock'),database(directory/'outbox.sqlite3') as db:
        db.execute('CREATE TABLE IF NOT EXISTS outbox(publication TEXT PRIMARY KEY,event_id TEXT UNIQUE NOT NULL,body TEXT NOT NULL,created_at TEXT NOT NULL,confirmed_at TEXT,attempts INTEGER NOT NULL DEFAULT 0)')
        prefix=f"{spec['publication_prefix']}/dataset=invoice_details/version=1/"
        discovered=0
        for key in sorted(store.list(prefix)):
            marker=json.loads(store.get(key,limit=16384))
            event=message_for(marker,key,spec);body=encoded(event).decode()
            previous=db.execute('SELECT body FROM outbox WHERE publication=?',(key,)).fetchone()
            if previous:
                require(previous['body']==body,'Published marker changed; operator review required')
            else:
                db.execute('INSERT INTO outbox(publication,event_id,body,created_at) VALUES(?,?,?,?)',
                           (key,event['event_id'],body,utcnow()));discovered+=1
        db.commit()
        fault('after-outbox')
        if replay:
            require(db.execute('SELECT 1 FROM outbox WHERE event_id=?',(replay,)).fetchone(),'Unknown replay event ID')
            db.execute('UPDATE outbox SET confirmed_at=NULL WHERE event_id=?',(replay,));db.commit()
        pending=db.execute('SELECT event_id,body FROM outbox WHERE confirmed_at IS NULL ORDER BY publication').fetchall()
        sent=0
        # A successful connection also makes the idle watch health meaningful.
        with connection() as conn:
            channel=conn.channel();channel.confirm_delivery()
            for item in pending:
                db.execute('UPDATE outbox SET attempts=attempts+1 WHERE event_id=?',(item['event_id'],));db.commit()
                publish(channel,json.loads(item['body']))
                fault('after-confirm')
                db.execute('UPDATE outbox SET confirmed_at=? WHERE event_id=?',(utcnow(),item['event_id']));db.commit()
                sent+=1
        result=dict(status='ok',discovered=discovered,confirmed=sent,
                    pending=db.execute('SELECT count(*) FROM outbox WHERE confirmed_at IS NULL').fetchone()[0],
                    last_success=utcnow(),checked_at=time.time())
        atomic_json(directory/'health.json',result)
        return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--watch',action='store_true')
    parser.add_argument('--replay-event',help='Explicitly republish a known outbox event; consumer deduplicates')
    args=parser.parse_args()
    stop=False
    def shutdown(*_):
        global stop
        stop=True
    signal.signal(signal.SIGTERM,shutdown);signal.signal(signal.SIGINT,shutdown)
    while not stop:
        try:
            print(json.dumps(scan_and_send(args.replay_event)),flush=True)
        except Exception as exc:
            result={'status':'error','error_type':type(exc).__name__,'checked_at':time.time()}
            atomic_json(Path(os.environ.get('DISPATCH_STATE_DIR','/state'))/'health.json',result)
            print(json.dumps(result),flush=True)
            if not args.watch:
                raise SystemExit(1)
        if not args.watch:
            break
        for _ in range(30):
            if stop: break
            time.sleep(1)
