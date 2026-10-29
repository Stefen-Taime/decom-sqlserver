"""Copy verified v1 batches to the v2 layout; never remove the source archive."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import sys

from archive import ArchiveStore, LAYOUT, parse_batch, s3_client, sha256


def migrate_one(store, source_prefix, receipt_key):
    receipt_bytes = store._get(receipt_key)
    if receipt_bytes is None:
        raise IOError('source receipt is missing')
    receipt = json.loads(receipt_bytes)
    data_key = receipt['payload_key']
    if (receipt.get('format') != 'debezium-json-batch/v1'
            or not receipt_key.startswith(source_prefix + '/')
            or data_key != receipt_key.removesuffix('.receipt.json') + '.json.gz'
            or receipt.get('archive_prefix') != source_prefix):
        raise ValueError('source receipt is outside the expected v1 contract')
    compressed = store._get(data_key)
    if compressed is None or sha256(compressed) != receipt['compressed_sha256']:
        raise IOError('source compressed checksum mismatch')
    body = gzip.decompress(compressed)
    if sha256(body) != receipt['payload_sha256'] or sha256(body) != receipt['batch_id']:
        raise IOError('source payload checksum mismatch')
    events = parse_batch(body)
    tables = Counter(f"{e['source']['schema']}.{e['source']['table']}" for e in events)
    operations = Counter(e['op'] for e in events)
    if (len(events) != receipt['records'] or dict(tables) != receipt['tables']
            or dict(operations) != receipt['operations'] or receipt['purge_authorized'] is not False):
        raise ValueError('source receipt counts or controls do not match its payload')
    # Preserve the compressed bytes as well as the uncompressed JSON, including event order.
    result = store.archive(body, compressed=compressed)
    new_receipt = json.loads(store._get(result['receipt_key']))
    return {
        'source_receipt': receipt_key, 'source_payload': data_key,
        'target_receipt': result['receipt_key'], 'target_payload': new_receipt['payload_key'],
        'payload_sha256': sha256(body), 'compressed_sha256': sha256(compressed),
        'records': len(events), 'tables': dict(tables), 'operations': dict(operations),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-prefix', required=True)
    parser.add_argument('--report', required=True, help='Durable host-mounted JSON report path')
    args = parser.parse_args()
    source = args.source_prefix.rstrip('/')
    store = ArchiveStore(s3_client(), os.environ.get('S3_BUCKET', 'cdc-raw'))
    if not source or source == store.prefix or store.prefix.startswith(source + '/') or source.startswith(store.prefix + '/'):
        raise SystemExit('Source and destination prefixes must be separate generations.')
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    # Fail before writing archives if the report location cannot be written.
    staging = report_path.with_suffix('.partial.json')
    staging.write_text(json.dumps({'state': 'running', 'source_prefix': source, 'target_prefix': store.prefix}) + '\n')
    inventory = []
    for page in store.client.get_paginator('list_objects_v2').paginate(Bucket=store.bucket, Prefix=source + '/'):
        inventory.extend(o['Key'] for o in page.get('Contents', []))
    keys = sorted(key for key in inventory if key.endswith('.receipt.json'))
    if not keys:
        raise SystemExit('No source receipts; migration is not complete.')
    entries, tables, operations = [], Counter(), Counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        for start in range(0, len(keys), 16):
            for entry in pool.map(lambda key: migrate_one(store, source, key), keys[start:start + 16]):
                entries.append(entry)
                tables.update(entry['tables'])
                operations.update(entry['operations'])
            if len(entries) % 256 == 0 or len(entries) == len(keys):
                print(f'Verified migration: {len(entries)}/{len(keys)} batches', file=sys.stderr, flush=True)
    # Re-list the destination to check that every acknowledged source batch is reachable.
    destination = set()
    for page in store.client.get_paginator('list_objects_v2').paginate(Bucket=store.bucket, Prefix=store.prefix + '/'):
        destination.update(o['Key'] for o in page.get('Contents', []))
    required = {entry[field] for entry in entries for field in ['target_payload', 'target_receipt']}
    if not required <= destination or len(required) != 2 * len(keys):
        raise IOError('destination archive coverage mismatch')
    summary = {
        'state': 'complete', 'verified_at': datetime.now(timezone.utc).isoformat(), 'layout': LAYOUT,
        'source_prefix': source, 'target_prefix': store.prefix,
        'source_inventory_sha256': sha256('\n'.join(sorted(inventory)).encode()),
        'verified_batches': len(entries), 'verified_target_objects': len(required),
        'records': sum(entry['records'] for entry in entries),
        'tables': dict(tables), 'operations': dict(operations),
        'source_unreceipted_payloads': len({k for k in inventory if k.endswith('.json.gz')} - {e['source_payload'] for e in entries}),
        'payload_bytes_preserved': True, 'compressed_bytes_preserved': True,
        'source_deleted': False, 'purge_authorized': False,
    }
    staging.write_text(json.dumps({**summary, 'entries': entries}, indent=2) + '\n')
    # fsync before the final report name is published on the persistent disk.
    with staging.open('rb') as report:
        os.fsync(report.fileno())
    staging.replace(report_path)
    directory_fd = os.open(report_path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
