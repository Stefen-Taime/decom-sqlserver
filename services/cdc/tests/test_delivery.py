"""Failure tests for the HTTP acknowledgment boundary; no external services required."""
import base64
import gzip
import hashlib
import hmac
from http.client import HTTPConnection
from io import BytesIO
import json
from pathlib import Path
import sys
import threading
import time

from botocore.exceptions import ClientError
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archiver'))
from archive import ArchiveStore
from server import LimitedServer, handler_type

SECRET = base64.b64encode(b'x' * 32).decode()


def event_body(op='c', table='CdcProbe'):
    return json.dumps([{'schema': {'type': 'struct'}, 'payload': {
        'ts_ms': 1793203200000,
        'op': op, 'source': {'db': 'WideWorldImporters', 'schema': 'lab', 'table': table,
                            'commit_lsn': '0000001a:00000002:0001'},
        'before': {'ProbeID': 'test-id', 'Payload': 'before'},
        'after': {'ProbeID': 'test-id', 'Payload': 'after'},
    }}]).encode()


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.fail_suffix = None
        self.corrupt = False

    def head_bucket(self, **kwargs):
        return {}

    def get_object(self, Key, **kwargs):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        return {'Body': BytesIO(b'corrupted' if self.corrupt else self.objects[Key])}

    def put_object(self, Key, Body, **kwargs):
        if self.fail_suffix and Key.endswith(self.fail_suffix):
            raise IOError('Injected storage failure')
        self.puts.append(Key)
        self.objects[Key] = Body


@pytest.fixture
def receiver():
    client = FakeS3()
    server = LimitedServer(('127.0.0.1', 0), handler_type(ArchiveStore(client, 'cdc-raw'), SECRET))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def send(body, signed_body=None, age=0):
        timestamp = str(int(time.time()) - age)
        data = body if signed_body is None else signed_body
        signature = base64.b64encode(hmac.digest(b'x' * 32, f'msg_test.{timestamp}.'.encode() + data, 'sha256')).decode()
        conn = HTTPConnection(*server.server_address, timeout=5)
        conn.request('POST', '/events', body, {
            'webhook-id': 'msg_test', 'webhook-timestamp': timestamp,
            'webhook-signature': 'v1,' + signature, 'Content-Type': 'application/json',
        })
        response = conn.getresponse()
        status, payload = response.status, json.loads(response.read())
        conn.close()
        return status, payload

    yield client, send
    server.shutdown()
    server.server_close()
    thread.join()


def test_ack_only_after_raw_bytes_and_receipt_are_verified(receiver):
    client, send = receiver
    body = event_body()
    status, result = send(body)
    assert status == 200
    receipt = json.loads(client.objects[result['receipt_key']])
    compressed = client.objects[receipt['payload_key']]
    assert gzip.decompress(compressed) == body
    assert receipt['payload_sha256'] == hashlib.sha256(body).hexdigest()
    assert receipt['compressed_sha256'] == hashlib.sha256(compressed).hexdigest()
    assert receipt['purge_authorized'] is False
    assert '/manifests/table=lab.CdcProbe/batch_date=2026-10-28/' in result['receipt_key']
    assert '/data/table=lab.CdcProbe/batch_date=2026-10-28/' in receipt['payload_key']
    assert send(body) == (status, result)
    assert len(client.puts) == 2  # Exact retry never creates or rewrites objects.


@pytest.mark.parametrize('failure', ['payload', 'receipt', 'readback'])
def test_storage_failure_never_acknowledged_and_retry_completes(receiver, failure):
    client, send = receiver
    client.fail_suffix = {'payload': '.json.gz', 'receipt': '.receipt.json'}.get(failure)
    client.corrupt = failure == 'readback'
    assert send(event_body())[0] == 503
    assert not any(key.endswith('.receipt.json') for key in client.objects)
    client.fail_suffix, client.corrupt = None, False
    assert send(event_body())[0] == 200
    assert len(client.objects) == 2


def test_tampered_and_expired_requests_never_write(receiver):
    client, send = receiver
    assert send(event_body(), signed_body=b'different')[0] == 401
    assert send(event_body(), age=301)[0] == 401
    assert not client.objects


@pytest.mark.parametrize('body', [b'not-json', b'[]', event_body(op='x'), event_body(table='Unknown')])
def test_invalid_events_never_write(receiver, body):
    client, send = receiver
    assert send(body)[0] == 400
    assert not client.objects


def test_conflicting_existing_object_is_not_overwritten(receiver):
    client, send = receiver
    status, result = send(event_body())
    assert status == 200
    key = json.loads(client.objects[result['receipt_key']])['payload_key']
    client.objects[key] = b'damaged-on-storage'
    assert send(event_body())[0] == 503
    assert client.objects[key] == b'damaged-on-storage'


def test_retry_on_another_day_keeps_the_same_partition(receiver, monkeypatch):
    client, send = receiver
    body = event_body()
    first = send(body)
    monkeypatch.setattr(time, 'time', lambda: 2000000000)
    assert send(body) == first
    assert len(client.objects) == 2


def test_batch_across_midnight_uses_earliest_capture_time(receiver):
    client, send = receiver
    records = json.loads(event_body()) * 2
    records = json.loads(json.dumps(records))
    records[0]['payload']['ts_ms'] = 1793232000001  # 2026-10-29 00:00:00.001 UTC
    records[1]['payload']['ts_ms'] = 1793231999999  # 2026-10-28 23:59:59.999 UTC
    status, result = send(json.dumps(records).encode())
    assert status == 200
    receipt = json.loads(client.objects[result['receipt_key']])
    assert receipt['partition'] == 'table=lab.CdcProbe/batch_date=2026-10-28'
    assert receipt['capture_ts_ms_min'] == 1793231999999
    assert receipt['capture_ts_ms_max'] == 1793232000001


def test_mixed_table_batch_retains_every_event_and_its_bytes(receiver):
    client, send = receiver
    records = json.loads(event_body())
    records.append({'payload': {'op': 'r', 'ts_ms': 1793203200000,
        'source': {'db': 'WideWorldImporters', 'schema': 'Sales', 'table': 'Customers'},
        'after': {'CustomerID': 1}}})
    body = json.dumps(records).encode()
    status, result = send(body)
    assert status == 200
    receipt = json.loads(client.objects[result['receipt_key']])
    assert receipt['partition'].startswith('table=_mixed/')
    assert receipt['tables'] == {'lab.CdcProbe': 1, 'Sales.Customers': 1}
    assert gzip.decompress(client.objects[receipt['payload_key']]) == body


@pytest.mark.parametrize('timestamp', [None, True, '1793203200000', -1, 253402300800000])
def test_invalid_capture_timestamp_never_writes(receiver, timestamp):
    client, send = receiver
    records = json.loads(event_body())
    records[0]['payload']['ts_ms'] = timestamp
    assert send(json.dumps(records).encode())[0] == 400
    assert not client.objects
