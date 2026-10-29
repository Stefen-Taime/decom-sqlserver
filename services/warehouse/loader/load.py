"""Verified SeaweedFS batches -> transactional five-table sales Data Vault."""
import argparse
from collections import Counter
from decimal import Decimal
import gzip
import json
import os
from pathlib import Path
import re
import time

import psycopg
from psycopg.types.json import Jsonb
import simplejson

from archive import ArchiveStore, DEFAULT_PREFIX, LAYOUT, batch_partition, parse_batch, s3_client, sha256
from sales_contract import TABLE_SPECS, CONSUMER_VERSION, SOURCE_INSTANCE, SNAPSHOT_GENERATION
from resolve import refresh_batch

TABLES = {table:spec['pk'] for table,spec in TABLE_SPECS.items()}
LOAD_SQL = Path(__file__).with_name('load-batch.sql')
COLUMNS = ('event_id','semantic_sha256','table_name','source_pk','operation','stream_phase',
    'commit_lsn','change_lsn','event_serial_no','source_ts_ms','capture_ts_ms','entity_hk',
    'customer_hk','link_hk','hashdiff','row_data','source_metadata','first_batch_id',
    'order_hk','invoice_hk','bill_to_customer_hk','bill_to_link_hk','order_link_hk')


def canonical(value):
    return simplejson.dumps(value, use_decimal=True, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def digest(value):
    return sha256(canonical(value).encode())


def connect():
    return psycopg.connect(host=os.environ.get('PGHOST', 'postgres'), port=5432,
        dbname=os.environ.get('PGDATABASE', 'warehouse'), user=os.environ.get('PGUSER', 'warehouse_loader'),
        password=Path(os.environ['PGPASSWORD_FILE']).read_text().strip(),
        connect_timeout=10, application_name='wwi-archive-loader', autocommit=True,
        options='-c statement_timeout=120000 -c lock_timeout=15000 -c timezone=UTC')


def lsn(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-fA-F]{8}:[0-9a-fA-F]{8}:[0-9a-fA-F]{4}', value):
        raise ValueError('Missing or malformed source LSN')
    return bytes.fromhex(value.replace(':',''))


def integer_key(value):
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError('Invalid source identifier')
    return value


def transform(event, batch_id):
    source, op = event['source'], event['op']
    table = f"{source['schema']}.{source['table']}"
    if table not in TABLES:
        return None
    row = event['before'] if op == 'd' else event['after']
    key = integer_key(row[TABLES[table]])
    if op == 'u' and (not isinstance(event.get('before'), dict) or event['before'].get(TABLES[table]) != key):
        raise ValueError('An update must preserve its primary key; key changes require delete/create events')
    if op == 'r':
        if str(source.get('snapshot')).lower() not in {'first','first_in_data_collection','true','last_in_data_collection','last'}:
            raise ValueError('Only the initial snapshot generation is supported')
        expected_snapshot_lsn = os.environ.get('SNAPSHOT_COMMIT_LSN')
        if not expected_snapshot_lsn or lsn(source.get('commit_lsn')) != lsn(expected_snapshot_lsn):
            raise ValueError('Snapshot position differs from the configured bootstrap; a new generation requires review')
        commit, change, serial, phase = bytes(10), bytes(10), 0, 0
        identity = [SOURCE_INSTANCE, SNAPSHOT_GENERATION, table, key]
    else:
        commit, change = lsn(source.get('commit_lsn')), lsn(source.get('change_lsn'))
        serial = source.get('event_serial_no')
        if type(serial) is not int or serial < 1:
            raise ValueError('Missing CDC event serial number')
        phase = 1
        identity = [SOURCE_INSTANCE, table, key, op, commit.hex(), change.hex(), serial]
    entity = TABLE_SPECS[table]['entity']
    entity_hk = digest([SOURCE_INSTANCE, entity, key])
    customer_hk=link_hk=order_hk=invoice_hk=bill_to_hk=bill_link=order_link=None
    def hk(kind,value):
        return digest([SOURCE_INSTANCE,kind,integer_key(value)])
    def link(kind,parent):
        return digest([SOURCE_INSTANCE,kind,key,integer_key(parent)])
    if entity=='customer':
        customer_hk=entity_hk
    elif entity=='order':
        order_hk=entity_hk
        customer_hk=hk('customer',row.get('CustomerID'))
        link_hk=link('order-customer',row['CustomerID'])
    elif entity=='order-line':
        order_hk=hk('order',row.get('OrderID'))
        link_hk=link('order-line-order',row['OrderID'])
    elif entity=='invoice':
        invoice_hk=entity_hk
        customer_hk=hk('customer',row.get('CustomerID'))
        bill_to_hk=hk('customer',row.get('BillToCustomerID'))
        link_hk=link('invoice-customer',row['CustomerID'])
        bill_link=link('invoice-bill-to-customer',row['BillToCustomerID'])
        if 'OrderID' not in row:
            raise ValueError('Missing nullable OrderID field in invoice image')
        if row['OrderID'] is not None:
            order_hk=hk('order',row['OrderID'])
            order_link=link('invoice-order',row['OrderID'])
    elif entity=='invoice-line':
        invoice_hk=hk('invoice',row.get('InvoiceID'))
        link_hk=link('invoice-line-invoice',row['InvoiceID'])
    attributes = {k:v for k,v in row.items() if k != TABLES[table]}
    # Exclude capture clocks and batch boundaries from event identity and replay comparison.
    semantic = digest({'identity': identity, 'before': event.get('before'), 'after': event.get('after'), 'op': op})
    return (digest(identity), semantic, table, key, op, phase, commit, change, serial,
        source.get('ts_ms'), event['ts_ms'], entity_hk, customer_hk, link_hk,
        digest({'deleted': op=='d', 'attributes': attributes}),
        Jsonb(row, dumps=canonical), Jsonb(source, dumps=canonical), batch_id,
        order_hk,invoice_hk,bill_to_hk,bill_link,order_link)


def verified_batch(store, manifest_key):
    manifest_bytes = store._get(manifest_key)
    if manifest_bytes is None:
        raise IOError('Manifest is missing')
    receipt = json.loads(manifest_bytes)
    if (receipt.get('layout') != LAYOUT or receipt.get('format') != 'debezium-json-batch/v1'
            or receipt.get('archive_prefix') != store.prefix or receipt.get('purge_authorized') is not False):
        raise ValueError('Unsupported archive contract')
    batch_id = receipt['batch_id']
    if not re.fullmatch('[0-9a-f]{64}', batch_id):
        raise ValueError('Invalid batch identifier')
    partition = receipt['partition']
    if (receipt['payload_key'] != f'{store.prefix}/data/{partition}/{batch_id}.json.gz'
            or manifest_key != f'{store.prefix}/manifests/{partition}/{batch_id}.receipt.json'):
        raise ValueError('Manifest paths are inconsistent')
    compressed = store._get(receipt['payload_key'])
    if compressed is None or sha256(compressed) != receipt['compressed_sha256']:
        raise IOError('Archive compressed checksum mismatch')
    raw = gzip.decompress(compressed)
    if sha256(raw) != receipt['payload_sha256'] or sha256(raw) != batch_id:
        raise IOError('Archive payload checksum mismatch')
    events = parse_batch(raw)
    actual_partition, first, last = batch_partition(events)
    if (actual_partition != partition or first != receipt['capture_ts_ms_min'] or last != receipt['capture_ts_ms_max']
            or len(events) != receipt['records']
            or dict(Counter(f"{e['source']['schema']}.{e['source']['table']}" for e in events)) != receipt['tables']
            or dict(Counter(e['op'] for e in events)) != receipt['operations']):
        raise ValueError('Manifest content does not match its batch')
    return receipt, sha256(manifest_bytes), events


def apply_batch(conn, receipt, manifest_key, manifest_sha, events, *, fail_after_stage=False):
    selected = [row for event in events if (row := transform(event, receipt['batch_id'])) is not None]
    unique = {}
    for row in selected:
        if row[0] in unique and unique[row[0]][1] != row[1]:
            raise ValueError('Conflicting copies of the same source event')
        unique[row[0]] = row
    with conn.transaction(), conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(782641901)')
        existing = cur.execute('SELECT manifest_sha256 FROM ops.archive_batches WHERE batch_id=%s', (receipt['batch_id'],)).fetchone()
        if existing:
            if existing[0] != manifest_sha:
                raise ValueError('An acknowledged manifest was modified')
        acknowledged=cur.execute('SELECT 1 FROM ops.consumer_batches WHERE consumer_version=%s AND batch_id=%s',
            (CONSUMER_VERSION,receipt['batch_id'])).fetchone()
        if acknowledged:
            refresh_batch(conn,events,selected)
            return 0
        cur.execute('CREATE TEMP TABLE incoming (LIKE staging.cdc_event INCLUDING DEFAULTS) ON COMMIT DROP')
        with cur.copy('COPY incoming (' + ','.join(COLUMNS) + ') FROM STDIN') as copy:
            for row in unique.values():
                copy.write_row(row)
        conflict = cur.execute('SELECT 1 FROM incoming i JOIN staging.cdc_event e USING(event_id) WHERE i.semantic_sha256<>e.semantic_sha256 LIMIT 1').fetchone()
        if conflict:
            raise ValueError('Replayed event has different source content')
        cur.execute(LOAD_SQL.read_text())
        refresh_batch(conn,events,selected)
        if fail_after_stage:
            raise RuntimeError('Injected failure before batch acknowledgment')
        inserted = cur.execute('SELECT count(*) FROM new_events').fetchone()[0]
        cur.execute('''INSERT INTO ops.archive_batches(batch_id,manifest_key,manifest_sha256,payload_key,archive_prefix,
            total_records,selected_records,new_events,ignored_records,consumer_version)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(batch_id) DO NOTHING''',
            (receipt['batch_id'], manifest_key, manifest_sha, receipt['payload_key'], receipt['archive_prefix'],
             len(events), len(selected), inserted, len(events)-len(selected), CONSUMER_VERSION))
        cur.execute('''INSERT INTO ops.consumer_batches(consumer_version,batch_id,total_records,selected_records,new_events,ignored_records)
            VALUES(%s,%s,%s,%s,%s,%s)''',
            (CONSUMER_VERSION,receipt['batch_id'],len(events),len(selected),inserted,len(events)-len(selected)))
    return inserted


def run_once(store, conn, *, replay=False):
    expected={'tables':sorted(TABLES),'source_instance':SOURCE_INSTANCE,'snapshot_generation':SNAPSHOT_GENERATION,
        'snapshot_commit_lsn':os.environ.get('SNAPSHOT_COMMIT_LSN'),'archive_prefix':store.prefix,'layout':LAYOUT}
    contract=conn.execute('SELECT specification FROM ops.consumer_contracts WHERE consumer_version=%s',(CONSUMER_VERSION,)).fetchone()
    if not contract or contract[0]!=expected:
        raise ValueError('Consumer scope or source generation differs from its approved database contract')
    known = set() if replay else {row[0] for row in conn.execute('''SELECT a.manifest_key FROM ops.archive_batches a
        JOIN ops.consumer_batches c USING(batch_id) WHERE c.consumer_version=%s''',(CONSUMER_VERSION,))}
    keys = []
    for table in [*TABLES, '_mixed']:
        for page in store.client.get_paginator('list_objects_v2').paginate(
                Bucket=store.bucket, Prefix=f'{store.prefix}/manifests/table={table}/'):
            keys.extend(o['Key'] for o in page.get('Contents', []) if o['Key'].endswith('.receipt.json'))
    processed, inserted = 0, 0
    for key in sorted(keys):
        if key in known:
            continue
        try:
            receipt, receipt_sha, events = verified_batch(store, key)
            inserted += apply_batch(conn, receipt, key, receipt_sha, events)
        except Exception as error:
            print(json.dumps({'failed_manifest':key,'error_type':type(error).__name__,
                'validation_reason':str(error) if isinstance(error,ValueError) else None}),flush=True)
            raise
        processed += 1
        if processed % 50 == 0:
            print(json.dumps({'processed_batches': processed, 'new_events': inserted}), flush=True)
    conn.execute('''INSERT INTO ops.consumer_status(consumer,last_success_at,error_type,scanned_manifests)
        VALUES(%s,now(),NULL,%s) ON CONFLICT(consumer) DO UPDATE SET last_success_at=now(),error_type=NULL,scanned_manifests=excluded.scanned_manifests''',
        (CONSUMER_VERSION,len(keys)))
    result = {'scanned_manifests':len(keys), 'verified_batches_this_run':processed, 'new_events':inserted, 'purge_authorized':False}
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--replay', action='store_true', help='Re-read and verify even previously acknowledged batches')
    parser.add_argument('--health', action='store_true')
    args = parser.parse_args()
    with connect() as conn:
        if args.health:
            status = conn.execute('SELECT last_success_at>now()-interval \'180 seconds\' AND error_type IS NULL FROM ops.consumer_status WHERE consumer=%s', (CONSUMER_VERSION,)).fetchone()
            raise SystemExit(0 if status and status[0] else 1)
        store = ArchiveStore(s3_client(), os.environ.get('S3_BUCKET','cdc-raw'))
        while True:
            try:
                run_once(store, conn, replay=args.replay)
            except Exception as error:
                # Never log row data or credentials. Failed transactions leave no checkpoint.
                try:
                    conn.execute('''INSERT INTO ops.consumer_status(consumer,last_error_at,error_type) VALUES(%s,now(),%s)
                        ON CONFLICT(consumer) DO UPDATE SET last_error_at=now(),error_type=excluded.error_type''',
                        (CONSUMER_VERSION,type(error).__name__))
                except Exception:
                    pass
                print(json.dumps({'load_failed':type(error).__name__, 'purge_authorized':False}), flush=True)
                raise SystemExit(1)
            if not args.watch:
                return
            time.sleep(30)


if __name__ == '__main__':
    main()
