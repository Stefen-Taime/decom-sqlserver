"""Read-only integrity, retained CDC identity/value and warehouse checkpoint audit."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from archive import ArchiveStore, s3_client
from load import connect, verified_batch, transform, digest
from audit_core import archive_images, compare_images, decision, image_key, lsn, warehouse_record_count
from row_values import RowValues
from logical_values import LogicalValues
from resume_audit import ResumeAudit


def run():
    policy = json.loads(Path('/input/policy.json').read_text())
    source_bytes = Path('/input/source.json').read_bytes()
    source = json.loads(source_bytes)
    lower, upper = lsn(policy['snapshot_commit_lsn']), lsn(source['upper_lsn'])
    if upper < lower:
        raise ValueError('Observed source timeline is older than the configured bootstrap')
    values=RowValues(source,policy)
    resolution_bytes=Path('/input/resolution.json').read_bytes()
    logical=LogicalValues(source,policy,json.loads(resolution_bytes))
    checkpoint_bytes=Path('/input/checkpoint.json').read_bytes()
    resume=ResumeAudit(json.loads(checkpoint_bytes),source,policy)
    expected, observed = Counter(), Counter()
    pre_bootstrap = Counter()
    for table, rows in source['tables'].items():
        for row in rows:
            if lsn(row['commit_lsn']) <= lower:
                pre_bootstrap[table] += 1
            else:
                if lsn(row['commit_lsn']) > upper:
                    raise ValueError('Source row exceeds the fixed observation boundary')
                expected[image_key(table, row['pk'], row['operation'], row['commit_lsn'], row['change_lsn'])] += 1
    expected_scope = {(t, spec['capture_instance']) for t, spec in policy['tables'].items()}
    actual_scope = {(c['table_name'], c['capture_instance']) for c in source['capture_instances']}
    scope_ok = actual_scope == expected_scope and all(not c['has_drop_pending'] and c['end_lsn'] is None for c in source['capture_instances'])
    client = s3_client()
    store = ArchiveStore(client, policy['archive_bucket'], policy['archive_prefix'])
    keys = sorted(o['Key'] for page in client.get_paginator('list_objects_v2').paginate(
        Bucket=store.bucket, Prefix=store.prefix + '/manifests/') for o in page.get('Contents', [])
        if o['Key'].endswith('.receipt.json'))
    if not keys:
        raise ValueError('Archive manifest set is empty')
    errors, checkpoint_errors = [], []
    operations, tables = Counter(), Counter()
    seen, seen_warehouse, batch_ids = {}, {}, set()
    index = hashlib.sha256()
    verified_count = duplicates = conflicts = archive_only_batches = required_batches = 0
    with connect() as connection, connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        connection.execute("SET LOCAL application_name='cdc-purge-audit'")
        status = connection.execute("SELECT last_success_at>now()-interval '180 seconds' AND error_type IS NULL FROM ops.consumer_status WHERE consumer=%s",
                                    (policy['warehouse_consumer'],)).fetchone()
        acknowledged = {row[0]: row[1:] for row in connection.execute(
            '''SELECT b.batch_id,a.manifest_key,a.manifest_sha256,b.total_records,b.selected_records,b.ignored_records
               FROM ops.consumer_batches b JOIN ops.archive_batches a USING(batch_id)
               WHERE b.consumer_version=%s AND a.archive_prefix=%s''',
            (policy['warehouse_consumer'], store.prefix))}
        ledger = {row[0]: row[1] for row in connection.execute(
            'SELECT event_id,semantic_sha256 FROM staging.cdc_event WHERE stream_phase=1 AND commit_lsn>%s AND commit_lsn<=%s',
            (bytes.fromhex(lower), bytes.fromhex(upper)))}

        def read(key):
            try:
                return key, verified_batch(store, key), None
            except Exception as error:
                return key, None, type(error).__name__

        with ThreadPoolExecutor(max_workers=4) as pool:
            for start in range(0, len(keys), 16):
                for key, result, error in pool.map(read, keys[start:start+16]):
                    if error:
                        errors.append({'manifest': key, 'error_type': error}); continue
                    receipt, manifest_sha, events = result
                    verified_count += 1
                    batch_id = receipt['batch_id']; batch_ids.add(batch_id)
                    index.update(json.dumps([key, manifest_sha, batch_id], separators=(',', ':')).encode() + b'\n')
                    selected = warehouse_record_count(events, policy)
                    if selected: required_batches += 1
                    else: archive_only_batches += 1
                    expected_ack = (key, manifest_sha, len(events), selected, len(events)-selected)
                    if (selected or batch_id in acknowledged) and acknowledged.get(batch_id) != expected_ack:
                        checkpoint_errors.append(batch_id)
                    for event in events:
                        src = event['source']; table = src['schema'] + '.' + src['table']
                        operations[event['op']] += 1; tables[table] += 1
                        resume.observe(event)
                        if logical.wants(event):
                            logical.observe(event,transform(event,batch_id),key,manifest_sha)
                        if event['op'] == 'r':
                            values.snapshot_shape(event)
                            if lsn(src['commit_lsn']) != lower:
                                raise ValueError('Unexpected snapshot generation')
                            continue
                        if not lower < lsn(src['commit_lsn']) <= upper:
                            continue
                        pk = policy['tables'][table]['primary_key']
                        row = event['before'] if event['op'] == 'd' else event['after']
                        serial = src.get('event_serial_no')
                        if type(serial) is not int or serial < 1:
                            raise ValueError('Missing event serial number')
                        identity = [policy['source_instance'], table, row[pk], event['op'], lsn(src['commit_lsn']), lsn(src['change_lsn']), serial]
                        event_id = digest(identity)
                        semantic = digest({'identity': identity, 'before': event.get('before'), 'after': event.get('after'), 'op': event['op']})
                        if event_id in seen:
                            duplicates += 1
                            conflicts += int(seen[event_id] != semantic)
                            continue
                        seen[event_id] = semantic
                        observed.update(archive_images(event, policy))
                        values.observe(event)
                        if policy['tables'][table]['warehouse_required']:
                            transformed = transform(event, batch_id)
                            if transformed[0] != event_id or transformed[1] != semantic:
                                raise ValueError('Warehouse event identity implementation differs')
                            seen_warehouse[event_id] = semantic
                if verified_count and verified_count % 400 < 16:
                    print(json.dumps({'verified_manifests': verified_count, 'listed_manifests': len(keys)}), flush=True)
    missing_events = set(seen_warehouse) - set(ledger)
    extra_events = set(ledger) - set(seen_warehouse)
    mismatched_events = sum(ledger.get(key) != value for key, value in seen_warehouse.items() if key in ledger)
    missing_manifests = set(acknowledged) - batch_ids
    coverage = compare_images(expected, observed)
    row_comparison=values.report()
    resume_comparison=resume.report(source,policy)
    try:
        logical_comparison=logical.report(row_comparison,coverage)
    except (ValueError,KeyError,TypeError):
        # Never publish row contents or exception strings containing business data.
        logical_comparison={'contract':'sqlserver-cdc-logical-values/v1','matched':False,
                            'reason':'reconstruction_evidence_failed_revalidation'}
    per_table = {table: compare_images(Counter({k:v for k,v in expected.items() if k[0]==table}),
                                     Counter({k:v for k,v in observed.items() if k[0]==table})) for table in policy['tables']}
    checks = {
        'ARCHIVE_INTEGRITY': not errors and not conflicts,
        'RETAINED_POST_BOOTSTRAP_IDENTITIES_MATCH': coverage['matched'],
        'WAREHOUSE_BATCH_CHECKPOINTS_MATCH': not checkpoint_errors and not missing_manifests,
        'WAREHOUSE_STREAM_SEMANTICS_MATCH': not missing_events and not extra_events and not mismatched_events,
        'WAREHOUSE_SCAN_RECENT': bool(status and status[0]),
        'CAPTURE_SCOPE_MATCHES_POLICY': scope_ok,
        'CAPTURE_HISTORY_REACHES_BOOTSTRAP': all(c['start_lsn'] is not None and lsn(c['start_lsn'])<=lower for c in source['capture_instances']),
        'PRE_BOOTSTRAP_HISTORY_ACCOUNTED_FOR': not pre_bootstrap,
        'RETENTION_WINDOW_HAS_HISTORY': source['retention_observation_lsn'] is not None,
        'SOURCE_OBSERVATION_STABLE': None,
        'AUTOMATIC_CLEANUP_DISABLED': None,
        'RETAINED_LOGICAL_ROW_IMAGES_RECONCILED': logical_comparison['matched'],
        'WAREHOUSE_RESOLUTION_OBSERVATION_STABLE': None,
        'CONNECTOR_PERSISTENT_RESUME_VERIFIED': resume_comparison['verified'],
        'CONNECTOR_OBSERVATION_STABLE': None,
        'CONNECTOR_DURABLE_RESUME_CHECKPOINT_CERTIFIED': None,
        'TRANSACTION_FENCE_CERTIFIED': None,
        'ALL_CDC_CONSUMERS_REGISTERED_AND_APPROVED': None,
        'RETENTION_POLICY_APPROVED': None,
        'OFF_VM_ARCHIVE_RESTORE_PROVEN': None,
    }
    report = {'contract': policy['contract'], 'mode': 'audit-only', 'observed_at': datetime.now(timezone.utc).isoformat(),
        'source_observation_sha256': hashlib.sha256(source_bytes).hexdigest(),
        'policy_sha256': hashlib.sha256(Path('/input/policy.json').read_bytes()).hexdigest(),
        'source_upper_lsn_observed': source['upper_lsn'], 'bootstrap_lsn': policy['snapshot_commit_lsn'],
        'capture_instances': source['capture_instances'], 'retention_hours': policy['retention_hours'],
        'retention_cutoff_source_time': source['retention_cutoff_source_time'],
        'retention_observation_lsn': source['retention_observation_lsn'],
        'pre_bootstrap_retained_row_images': dict(pre_bootstrap),
        'archive': {'listed_manifests': len(keys), 'verified_manifests': verified_count, 'inventory_sha256': index.hexdigest(),
                    'operations': dict(operations), 'records_by_table': dict(tables), 'integrity_errors': errors[:20],
                    'integrity_error_count': len(errors), 'identical_stream_redeliveries': duplicates-conflicts,
                    'conflicting_stream_redeliveries': conflicts},
        'retained_identity_coverage': coverage, 'tables': per_table,
        'captured_column_values':row_comparison,'captured_columns':source['captured_columns'],
        'logical_column_values':logical_comparison,
        'resolution_observation_sha256':hashlib.sha256(resolution_bytes).hexdigest(),
        'connector_resume':resume_comparison,
        'checkpoint_observation_sha256':hashlib.sha256(checkpoint_bytes).hexdigest(),
        'warehouse': {'consumer': policy['warehouse_consumer'], 'acknowledged_batches': len(acknowledged),
                      'required_batches':required_batches,'archive_only_batches_not_required':archive_only_batches,
                      'checkpoint_mismatches': len(checkpoint_errors), 'acknowledged_but_missing_manifests': len(missing_manifests),
                      'archive_stream_events': len(seen_warehouse), 'ledger_stream_events': len(ledger),
                      'missing_stream_events': len(missing_events), 'unexpected_stream_events': len(extra_events),
                      'semantic_mismatches': mismatched_events},
        'checks': checks,
        'limitations': ['Value comparison covers retained post-bootstrap CDC images, not a full source snapshot or original values SQL CDC did not preserve.',
                        'A fixed maximum observed LSN is not a certified connector checkpoint or transaction fence.',
                        'The snapshot establishes state, not recovery of changes from before bootstrap.',
                        'Full-export application receipts do not establish a contiguous CDC checkpoint.',
                        'This audit contains no purge execution path and cannot produce an executable low watermark.']}
    report['decision'] = decision(checks)
    path = Path('/output/audit.json')
    path.write_text(json.dumps(report, indent=2)+'\n'); path.chmod(0o600)
    print(json.dumps({'report': str(path), 'purge_authorized': False, 'blockers': report['decision']['blockers']}))


if __name__ == '__main__':
    run()
