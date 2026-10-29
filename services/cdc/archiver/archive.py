"""Verified, content-addressed archival of unchanged Debezium HTTP batches."""
import base64
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import hmac
import json
import os
from pathlib import Path
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

TABLE_KEYS = {
    "Sales.Customers": "CustomerID",
    "Sales.Orders": "OrderID",
    "Sales.OrderLines": "OrderLineID",
    "Sales.Invoices": "InvoiceID",
    "Sales.InvoiceLines": "InvoiceLineID",
    "lab.CdcProbe": "ProbeID",
}
MAX_BODY_BYTES = 16 * 1024 * 1024
DEFAULT_PREFIX = 'raw/wwi/bootstrap-v2'
LAYOUT = 'table-batch-date/v2'


def s3_client():
    credentials = json.loads(Path(os.environ["S3_CREDENTIALS_FILE"]).read_text())
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
        aws_access_key_id=credentials["access_key"],
        aws_secret_access_key=credentials["secret_key"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      connect_timeout=3, read_timeout=15,
                      retries={"mode": "standard", "total_max_attempts": 2},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def verify_signature(body, headers, secret, now=None):
    """Verify the exact Standard Webhooks scheme used by Debezium Server."""
    try:
        timestamp = headers["webhook-timestamp"]
        message_id = headers["webhook-id"]
        if abs((time.time() if now is None else now) - int(timestamp)) > 300:
            return False
        key = base64.b64decode(secret.removeprefix("whsec_"), validate=True)
        signed = f"{message_id}.{timestamp}.".encode() + body
        expected = base64.b64encode(hmac.digest(key, signed, "sha256")).decode()
        return any(hmac.compare_digest(item, "v1," + expected)
                   for item in (headers.get("webhook-signature") or "").split())
    except (KeyError, TypeError, ValueError):
        return False


def parse_batch(body):
    if not 0 < len(body) <= MAX_BODY_BYTES:
        raise ValueError("invalid batch size")
    # Decimal prevents validation from rounding numeric data; raw bytes are stored unchanged.
    records = json.loads(body, parse_float=Decimal)
    if not isinstance(records, list) or not 0 < len(records) <= 1000:
        raise ValueError("expected a JSON array of 1–1000 events")
    events = []
    for record in records:
        payload = record.get("payload") if isinstance(record, dict) else None
        if not isinstance(payload, dict) or payload.get("op") not in {"r", "c", "u", "d"}:
            raise ValueError("unexpected event envelope or operation")
        source = payload.get("source")
        if not isinstance(source, dict) or source.get("db") != "WideWorldImporters":
            raise ValueError("unexpected source database")
        table = f"{source.get('schema')}.{source.get('table')}"
        if table not in TABLE_KEYS:
            raise ValueError("unexpected source table")
        row = payload.get("before") if payload["op"] == "d" else payload.get("after")
        if not isinstance(row, dict) or row.get(TABLE_KEYS[table]) is None:
            raise ValueError("missing primary key in row image")
        if payload["op"] != "r" and not source.get("commit_lsn"):
            raise ValueError("streaming event without commit LSN")
        events.append(payload)
    return events


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def batch_partition(events):
    """Route unchanged batches using their earliest Debezium capture time in UTC.

    The receiver's clock is never used: retries must keep the same keys.
    Mixed-table batches remain intact in table=_mixed; receipts list every table.
    """
    timestamps = [event.get('ts_ms') for event in events]
    if any(type(value) is not int or not 0 <= value <= 253402300799999 for value in timestamps):
        raise ValueError('missing or invalid Debezium capture timestamp')
    first, last = min(timestamps), max(timestamps)
    date = datetime.fromtimestamp(first // 1000, timezone.utc).date().isoformat()
    tables = {f"{event['source']['schema']}.{event['source']['table']}" for event in events}
    namespace = next(iter(tables)) if len(tables) == 1 else '_mixed'
    return f'table={namespace}/batch_date={date}', first, last


class ArchiveStore:
    def __init__(self, client, bucket, prefix=None):
        self.client, self.bucket = client, bucket
        self.prefix = (prefix or os.environ.get('ARCHIVE_PREFIX', DEFAULT_PREFIX)).rstrip('/')

    def ready(self):
        self.client.head_bucket(Bucket=self.bucket)

    def _get(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        stream = response["Body"]
        try:
            return stream.read()
        finally:
            stream.close()

    def _verified_put(self, key, data, content_type):
        existing = self._get(key)
        if existing is not None:
            if sha256(existing) != sha256(data):
                raise IOError("content-addressed object mismatch")
            return
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType=content_type,
            ContentMD5=base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode(),
            Metadata={"sha256": sha256(data)},
        )
        if sha256(self._get(key) or b"") != sha256(data):
            raise IOError("archive read-back checksum mismatch")

    def archive(self, body, *, compressed=None):
        events = parse_batch(body)
        batch_id = sha256(body)
        tables = Counter(f"{e['source']['schema']}.{e['source']['table']}" for e in events)
        partition, first, last = batch_partition(events)
        payload_key = f"{self.prefix}/data/{partition}/{batch_id}.json.gz"
        receipt_key = f"{self.prefix}/manifests/{partition}/{batch_id}.receipt.json"
        if compressed is None:
            # Reuse a valid existing encoding, including files copied during migration.
            # Gzip headers may differ between implementations even for identical raw data.
            compressed = self._get(payload_key)
            if compressed is None:
                compressed = gzip.compress(body, compresslevel=6, mtime=0)
        if gzip.decompress(compressed) != body:
            raise IOError('supplied compressed archive differs from the original batch')
        operations = Counter(e["op"] for e in events)
        lsns = [e["source"]["commit_lsn"] for e in events if e["source"].get("commit_lsn")]
        receipt = {
            "format": "debezium-json-batch/v1", "batch_id": batch_id,
            "layout": LAYOUT, "partition": partition,
            "partition_time_basis": "minimum payload.ts_ms (Debezium capture time), UTC",
            "capture_ts_ms_min": first, "capture_ts_ms_max": last,
            "archive_prefix": self.prefix,
            "payload_key": payload_key, "payload_sha256": batch_id,
            "compressed_sha256": sha256(compressed), "records": len(events),
            "tables": dict(tables), "operations": dict(operations),
            "commit_lsn_min": min(lsns, default=None), "commit_lsn_max": max(lsns, default=None),
            "purge_authorized": False,
        }
        receipt_bytes = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        # A receipt is published only after the payload has been read back and verified.
        self._verified_put(payload_key, compressed, "application/gzip")
        self._verified_put(receipt_key, receipt_bytes, "application/json")
        return {"batch_id": batch_id, "records": len(events), "receipt_key": receipt_key}
