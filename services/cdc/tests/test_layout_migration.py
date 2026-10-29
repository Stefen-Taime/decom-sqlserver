import gzip
import json

import pytest

from test_delivery import FakeS3, event_body
from archive import ArchiveStore, sha256
from migrate_layout import migrate_one


def legacy_fixture(client):
    body = event_body()
    digest = sha256(body)
    base = f'raw/wwi/bootstrap-v1/lab.CdcProbe/{digest[:2]}/{digest}'
    data_key, receipt_key = base + '.json.gz', base + '.receipt.json'
    # Non-default gzip header proves migration preserves the original compressed bytes.
    compressed = gzip.compress(body, mtime=123)
    receipt = {'format': 'debezium-json-batch/v1', 'archive_prefix': 'raw/wwi/bootstrap-v1',
        'payload_key': data_key, 'batch_id': digest, 'payload_sha256': digest,
        'compressed_sha256': sha256(compressed), 'records': 1,
        'tables': {'lab.CdcProbe': 1}, 'operations': {'c': 1}, 'purge_authorized': False}
    client.objects[data_key] = compressed
    client.objects[receipt_key] = json.dumps(receipt).encode()
    return data_key, receipt_key


def test_layout_migration_is_lossless_and_repeatable():
    client = FakeS3()
    data_key, receipt_key = legacy_fixture(client)
    originals = dict(client.objects)
    store = ArchiveStore(client, 'cdc-raw')
    result = migrate_one(store, 'raw/wwi/bootstrap-v1', receipt_key)
    assert client.objects[result['target_payload']] == originals[data_key]
    assert migrate_one(store, 'raw/wwi/bootstrap-v1', receipt_key) == result
    assert store.archive(event_body())['receipt_key'] == result['target_receipt']
    assert len(client.puts) == 2
    assert all(client.objects[key] == value for key, value in originals.items())


def test_partial_migration_can_resume_without_losing_the_source():
    client = FakeS3()
    data_key, receipt_key = legacy_fixture(client)
    client.fail_suffix = '.receipt.json'
    store = ArchiveStore(client, 'cdc-raw')
    with pytest.raises(IOError):
        migrate_one(store, 'raw/wwi/bootstrap-v1', receipt_key)
    assert data_key in client.objects and receipt_key in client.objects
    client.fail_suffix = None
    result = migrate_one(store, 'raw/wwi/bootstrap-v1', receipt_key)
    assert result['target_receipt'] in client.objects
    assert len(client.puts) == 2


def test_corrupted_source_is_not_migrated():
    client = FakeS3()
    data_key, receipt_key = legacy_fixture(client)
    client.objects[data_key] = b'corrupted'
    with pytest.raises(IOError, match='source compressed checksum'):
        migrate_one(ArchiveStore(client, 'cdc-raw'), 'raw/wwi/bootstrap-v1', receipt_key)
    assert not client.puts
