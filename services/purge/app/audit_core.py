"""Pure audit rules. This module cannot authorize or execute CDC cleanup."""
from collections import Counter
import hashlib
import json
import re


def lsn(value):
    if not isinstance(value, str):
        raise ValueError('Missing LSN')
    if re.fullmatch(r'[a-fA-F0-9]{8}:[a-fA-F0-9]{8}:[a-fA-F0-9]{4}', value):
        return value.replace(':', '').lower()
    if re.fullmatch(r'0x[a-fA-F0-9]{20}', value):
        return value[2:].lower()
    if re.fullmatch(r'[a-fA-F0-9]{20}', value):
        return value.lower()
    raise ValueError('Invalid 10-byte LSN')


def image_key(table, pk, operation, commit, change):
    if operation not in {1, 2, 3, 4} or pk is None:
        raise ValueError('Invalid CDC row identity')
    return (table, str(pk).lower(), operation, lsn(commit), lsn(change))


def archive_images(event, policy):
    if event['op'] == 'r':
        return []
    source = event['source']
    table = source['schema'] + '.' + source['table']
    pk = policy['tables'][table]['primary_key']
    commit, change = source['commit_lsn'], source['change_lsn']
    if event['op'] == 'u':
        if event['before'][pk] != event['after'][pk]:
            raise ValueError('Primary-key update requires a reviewed delete/create mapping')
        return [image_key(table, event['before'][pk], 3, commit, change),
                image_key(table, event['after'][pk], 4, commit, change)]
    if event['op'] == 'c':
        return [image_key(table, event['after'][pk], 2, commit, change)]
    if event['op'] == 'd':
        return [image_key(table, event['before'][pk], 1, commit, change)]
    raise ValueError('Unsupported CDC operation')


def counter_hash(counter):
    content = json.dumps(sorted(counter.items()), separators=(',', ':')).encode()
    return hashlib.sha256(content).hexdigest()


def warehouse_record_count(events, policy):
    return sum(policy['tables'][event['source']['schema']+'.'+event['source']['table']]['warehouse_required']
               for event in events)


def compare_images(expected, observed):
    missing, extra = expected - observed, observed - expected
    return {'matched': not missing and not extra, 'source_row_images': sum(expected.values()),
            'archive_row_images': sum(observed.values()), 'missing_row_images': sum(missing.values()),
            'unexpected_row_images': sum(extra.values()),
            'source_identity_sha256': counter_hash(expected), 'archive_identity_sha256': counter_hash(observed),
            'comparison': 'Retained row identities and multiplicities; see captured_column_values for the separate value comparison'}


def decision(checks):
    # Missing evidence stays unknown. A successful read-only audit is never a purge approval.
    blockers = [name for name, value in checks.items() if value is not True]
    blockers.append('PURGE_EXECUTION_NOT_IMPLEMENTED')
    return {'status': 'blocked', 'purge_authorized': False, 'executable_low_watermark': None,
            'blockers': blockers, 'changes_deleted': 0}
