"""Read-only validation of archive checksums, snapshot coverage and CDC probes."""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os

from archive import DEFAULT_PREFIX, LAYOUT, TABLE_KEYS, batch_partition, parse_batch, s3_client, sha256

EXPECTED_SNAPSHOT = {
    'Sales.Customers': 663, 'Sales.Orders': 73595, 'Sales.OrderLines': 231412,
    'Sales.Invoices': 70510, 'Sales.InvoiceLines': 228265,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', action='append', default=[], help='Probe UUID to verify; may repeat')
    parser.add_argument('--snapshot', action='store_true', help='Check unique snapshot keys against this WWI fixture')
    args = parser.parse_args()
    client, bucket = s3_client(), os.environ.get('S3_BUCKET', 'cdc-raw')
    root = os.environ.get('ARCHIVE_PREFIX', DEFAULT_PREFIX).rstrip('/') + '/'
    prefixes = [root + 'manifests/'] if args.snapshot or not args.probe else [
        root + 'manifests/table=lab.CdcProbe/', root + 'manifests/table=_mixed/']
    keys = []
    for prefix in prefixes:
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(o['Key'] for o in page.get('Contents', []) if o['Key'].endswith('.receipt.json'))

    def read(key):
        response = client.get_object(Bucket=bucket, Key=key)
        with response['Body'] as stream:
            return stream.read()

    def validate(key):
        receipt = json.loads(read(key))
        assert receipt['layout'] == LAYOUT, key
        assert receipt['archive_prefix'] + '/' == root, key
        assert receipt['payload_key'].startswith(root + 'data/'), key
        compressed = read(receipt['payload_key'])
        assert sha256(compressed) == receipt['compressed_sha256'], key
        raw = gzip.decompress(compressed)
        assert sha256(raw) == receipt['payload_sha256'] == receipt['batch_id'], key
        records = parse_batch(raw)
        partition, first, last = batch_partition(records)
        assert receipt['partition'] == partition, key
        assert receipt['capture_ts_ms_min'] == first and receipt['capture_ts_ms_max'] == last, key
        assert receipt['payload_key'] == root + f"data/{partition}/{receipt['batch_id']}.json.gz", key
        assert key == root + f"manifests/{partition}/{receipt['batch_id']}.receipt.json", key
        assert len(records) == receipt['records'], key
        assert receipt['purge_authorized'] is False, key
        return records

    unique = defaultdict(set)
    totals, operations = Counter(), Counter()
    probes = {value.lower(): [] for value in args.probe}
    snapshot_last = False
    with ThreadPoolExecutor(max_workers=4) as pool:
        def bounded_results():
            for start in range(0, len(keys), 16):
                yield from pool.map(validate, keys[start:start + 16])
        for records in bounded_results():
            for event in records:
                source = event['source']
                table = f"{source['schema']}.{source['table']}"
                totals[table] += 1
                operations[event['op']] += 1
                row = event['before'] if event['op'] == 'd' else event['after']
                if event['op'] == 'r':
                    unique[table].add(row[TABLE_KEYS[table]])
                    snapshot_last |= str(source.get('snapshot')).lower() == 'last'
                elif table == 'lab.CdcProbe':
                    probe_id = str(row['ProbeID']).lower()
                    if probe_id in probes:
                        probes[probe_id].append({'op': event['op'], 'value': row['Payload'],
                            'commit_lsn': source['commit_lsn'], 'change_lsn': source.get('change_lsn'),
                            'event_serial_no': source.get('event_serial_no')})
    counts = {table: len(values) for table, values in sorted(unique.items())}
    if args.snapshot:
        assert all(counts.get(table) == count for table, count in EXPECTED_SNAPSHOT.items()), counts
        assert snapshot_last, 'Snapshot last-record marker is missing'
    for probe_id, events in probes.items():
        assert {e['op'] for e in events} == {'c', 'u', 'd'}, (probe_id, events)
        assert all(e['value'] == ('before' if e['op'] == 'c' else 'after') for e in events), probe_id
    print(json.dumps({'verified_receipts': len(keys), 'snapshot_unique_keys': counts,
        'snapshot_last_marker': snapshot_last, 'archived_events_by_table': dict(totals),
        'operations': dict(operations), 'probes': probes, 'purge_authorized': False}, indent=2))


if __name__ == '__main__':
    main()
