"""One consistent PostgreSQL snapshot per attempt; publication is the final S3 write."""
import argparse
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
import re

import psycopg
from psycopg import sql
import pyarrow as pa
import pyarrow.parquet as pq

from common import (Stats, Store, arrow_schema, atomic_json, digest, encoded, load_contract,
                    paths, require, utcnow, verify_manifest)


def fault(point):
    if os.environ.get('EXPORT_TEST_MODE') == '1' and os.environ.get('EXPORT_FAULT') == point:
        print(json.dumps({'injected_crash':point}), flush=True)
        os._exit(86)


def connect():
    return psycopg.connect(host=os.environ['PGHOST'], dbname=os.environ['PGDATABASE'],
        user=os.environ['PGUSER'], password=Path(os.environ['PGPASSWORD_FILE']).read_text().strip(),
        application_name='warehouse-full-export/v1', connect_timeout=10,
        options='-c statement_timeout=600000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=300000 -c transaction_timeout=1800000')


def write_snapshot(contract, store, run_id, request_id):
    schema = arrow_schema(contract)
    root, publication = paths(contract, run_id)
    fields = [f['name'] for f in contract['fields']]
    table_id = sql.Identifier(*contract['source'].split('.'))
    stats, files = Stats(contract), []
    with connect() as db, tempfile.TemporaryDirectory() as directory:
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        snap, captured_at = db.execute('SELECT pg_current_snapshot()::text,transaction_timestamp()').fetchone()
        context_rows = db.execute('SELECT row_to_json(c) FROM delivery.ingestion_context c').fetchall()
        require(len(context_rows) == 1, 'Expected active ingestion context')
        context = context_rows[0][0]
        aggs = [sql.SQL('count(*)')]
        aggs.extend(sql.SQL('coalesce(sum({}),0)').format(sql.Identifier(c)) for c in contract['sum_columns'])
        source = db.execute(sql.SQL('SELECT {} FROM {}').format(sql.SQL(',').join(aggs), table_id)).fetchone()
        require(contract['allow_empty'] or source[0] > 0, 'Empty source rejected')
        expected_stats = Stats(contract)
        expected_stats.sums.update(dict(zip(contract['sum_columns'], source[1:])))
        expected = {'rows':source[0], 'sums':expected_stats.result()['sums']}
        query = sql.SQL('SELECT {} FROM {} ORDER BY {}').format(
            sql.SQL(',').join(map(sql.Identifier, fields)), table_id, sql.Identifier(contract['primary_key']))
        with db.cursor(name='export_rows') as cursor:
            cursor.execute(query)
            exhausted = False
            while not exhausted:
                index, rows = len(files)+1, 0
                require(index <= 10000, 'File count exceeds limit')
                path = Path(directory)/'part.parquet'
                with pq.ParquetWriter(path, schema, compression='zstd', version='2.6',
                                      write_page_checksum=True) as writer:
                    while rows < contract['rows_per_file']:
                        values = cursor.fetchmany(min(contract['row_group_rows'], contract['rows_per_file']-rows))
                        if not values:
                            exhausted = True
                            break
                        batch = pa.Table.from_pylist([dict(zip(fields, row)) for row in values], schema=schema)
                        stats.add(batch)
                        writer.write_table(batch, row_group_size=contract['row_group_rows'])
                        rows += len(values)
                if not rows and files:
                    break
                require(path.stat().st_size <= contract['max_file_bytes'], 'Parquet file exceeds size limit')
                with path.open('rb') as data:
                    sha = hashlib.file_digest(data, 'sha256').hexdigest()
                key = f'{root}/data/part-{index:05d}.parquet'
                store.upload(key, path)
                files.append(dict(key=key, rows=rows, bytes=path.stat().st_size, sha256=sha))
                print(json.dumps({'event':'part_uploaded','run_id':run_id,'part':index,'rows':rows}), flush=True)
                path.unlink()
                if index == 1:
                    fault('after-first-file')
        result = stats.result()
        require(result['rows'] == expected['rows'] and result['sums'] == expected['sums'], 'Source aggregate mismatch')
    # The database snapshot is closed before network read-back and publication.
    manifest = dict(protocol='full-export/v1', run_id=run_id, request_id=request_id,
        contract=contract, contract_sha256=digest(encoded(contract)), created_at=utcnow(),
        snapshot={'isolation':'REPEATABLE READ READ ONLY','postgres_snapshot':snap,
                  'captured_at':captured_at.isoformat(),'ingestion_context':context,
                  'source_transaction_boundary_certified':False,'purge_authorized':False},
        source_aggregates=expected, statistics=result, files=files,
        writer={'framework':'decom-export:1','pyarrow':pa.__version__,'format':'Parquet 2.6','compression':'zstd'})
    marker = dict(protocol='full-export/v1', dataset=contract['dataset'], version=contract['version'],
        run_id=run_id, manifest_key=root+'/manifest.json', manifest_sha256=digest(encoded(manifest)))
    store.immutable_json(marker['manifest_key'], manifest)
    verify_manifest(store, contract, marker, publication)
    return publication, marker


def run(contract, request_id):
    require(re.fullmatch(r'[A-Za-z0-9_-]{1,80}', request_id), 'Invalid request identifier')
    store = Store()
    require(store.bucket == contract['bucket'], 'Bucket differs from contract')
    state_dir = Path(os.environ['EXPORT_STATE_DIR'])
    state_dir.mkdir(parents=True, exist_ok=True)
    name = f"{contract['dataset']}-v{contract['version']}-{request_id}"
    state_path = state_dir/(name+'.json')
    contract_sha = digest(encoded(contract))
    with (state_dir/(name+'.lock')).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('This request is already running') from None
        state = json.loads(state_path.read_text()) if state_path.exists() else {
            'request_id':request_id, 'contract_sha256':contract_sha, 'attempts':[]}
        require(state['request_id'] == request_id, 'Request journal identity mismatch')
        require(state['contract_sha256'] == contract_sha, 'Request contract changed; use a new request identifier')
        attempts = state['attempts']
        if attempts and attempts[-1]['status'] in ('ready', 'published'):
            attempt = attempts[-1]
            manifest = verify_manifest(store, contract, attempt['marker'], attempt['publication'])
            require(manifest['request_id'] == request_id, 'Prepared manifest belongs to another request')
        else:
            if attempts:
                attempts[-1]['status'] = 'abandoned'
            run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-')+uuid.uuid4().hex
            attempt = dict(run_id=run_id, status='writing', started_at=utcnow())
            attempts.append(attempt)
            atomic_json(state_path, state)
            publication, marker = write_snapshot(contract, store, run_id, request_id)
            attempt.update(status='ready', publication=publication, marker=marker)
            atomic_json(state_path, state)
            fault('after-ready')
        store.immutable_json(attempt['publication'], attempt['marker'])
        fault('after-publish')
        attempt.update(status='published', completed_at=utcnow())
        atomic_json(state_path, state)
        return dict(dataset=contract['dataset'], request_id=request_id, run_id=attempt['run_id'],
                    publication=attempt['publication'], status='published', attempts=len(attempts))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--request-id', required=True, help='Stable idempotency key for this logical full export')
    args = parser.parse_args()
    try:
        print(json.dumps(run(load_contract(args.dataset), args.request_id)))
    except Exception as exc:
        # Never echo connection strings, SQL values or credentials in generic driver errors.
        print(json.dumps({'status':'failed','error_type':type(exc).__name__,
                          'message':str(exc) if isinstance(exc, (ValueError, RuntimeError)) else 'See protected operator diagnostics'}), file=sys.stderr)
        sys.exit(1)
