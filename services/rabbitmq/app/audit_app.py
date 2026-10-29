"""Reconcile every stored application row to its accepted export statistics."""
from datetime import date
from decimal import Decimal
import json
import pyarrow as pa
from common import Stats,arrow_schema,require
from messaging import contract,database
from invoice_app import state_path,status

spec=contract();stats=Stats(spec)
with database(state_path()/'invoices.sqlite3') as db:
    db.execute('BEGIN')
    active=db.execute('SELECT d.statistics,d.event_id FROM active_snapshot a JOIN deliveries d USING(event_id)').fetchone()
    require(active is not None,'No active application snapshot')
    cursor=db.execute('SELECT row_json FROM invoice_lines ORDER BY invoice_line_id')
    while rows:=cursor.fetchmany(5000):
        batch=[json.loads(r[0]) for r in rows]
        for row in batch:
            for field in spec['fields']:
                value=row[field['name']]
                if value is None: continue
                if field['type'].startswith('decimal'): row[field['name']]=Decimal(value)
                elif field['type']=='date32': row[field['name']]=date.fromisoformat(value)
        stats.add(pa.Table.from_pylist(batch,schema=arrow_schema(spec)))
    require(stats.result()==json.loads(active['statistics']),'Stored invoice rows differ from accepted export')
    require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok','Application database integrity check failed')
    result={'event_id':active['event_id'],'statistics':stats.result(),'all_stored_rows_reconciled':True,
            'sqlite_version':db.execute('SELECT sqlite_version()').fetchone()[0],
            'journal_mode':db.execute('PRAGMA journal_mode').fetchone()[0],
            'synchronous':db.execute('PRAGMA synchronous').fetchone()[0],'quick_check':'ok'}
print(json.dumps(result))
