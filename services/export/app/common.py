"""Versioned full-snapshot contract and bounded-memory integrity checks."""
import hashlib
import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq


def require(condition, message):
    if not condition:
        raise ValueError(message)


def serial(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=serial,
                      ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        os.chmod(temporary, 0o600)
        stream.write(encoded(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def load_contract(dataset):
    require(re.fullmatch(r'[a-z][a-z0-9_]{0,63}', dataset), 'Invalid dataset')
    contract = json.loads((Path(__file__).parent/'contracts'/f'{dataset}.v1.json').read_text())
    require(contract['dataset'] == dataset and contract['mode'] == 'full', 'Unknown contract')
    require(0 < contract['row_group_rows'] <= contract['rows_per_file'] <= 100000, 'Invalid batch sizing')
    require(re.fullmatch(r'[a-z_]+\.[a-z0-9_]+', contract['source']), 'Invalid source identifier')
    return contract


def arrow_schema(contract):
    primitive = dict(int64=pa.int64(), int32=pa.int32(), string=pa.string(),
                     date32=pa.date32(), bool=pa.bool_())
    fields = []
    for field in contract['fields']:
        decimal = re.fullmatch(r'decimal128\((\d+),(\d+)\)', field['type'])
        kind = pa.decimal128(*map(int, decimal.groups())) if decimal else primitive[field['type']]
        fields.append(pa.field(field['name'], kind, nullable=field['nullable']))
    return pa.schema(fields)


class Stats:
    def __init__(self, contract):
        self.contract = contract
        self.rows, self.first, self.last = 0, None, None
        self.nulls = {f['name']: 0 for f in contract['fields']}
        self.sums = {c: Decimal(0) for c in contract['sum_columns']}
        self.hash = hashlib.sha256()

    def add(self, table):
        require(table.schema.equals(arrow_schema(self.contract), check_metadata=False), 'Schema mismatch')
        names = [f['name'] for f in self.contract['fields']]
        required = {f['name'] for f in self.contract['fields'] if not f['nullable']}
        with localcontext() as ctx:
            ctx.prec = 60
            for row in table.to_pylist():
                for name in names:
                    if row[name] is None:
                        require(name not in required, f'Required field is NULL: {name}')
                        self.nulls[name] += 1
                pk = row[self.contract['primary_key']]
                require(self.last is None or pk > self.last, 'Duplicate or unordered primary key')
                if self.first is None:
                    self.first = pk
                self.last = pk
                self.rows += 1
                for name in self.sums:
                    if row[name] is not None:
                        self.sums[name] += Decimal(row[name])
                self.hash.update(encoded([row[name] for name in names]) + b'\n')

    def result(self):
        # Fixed contract scales make Parquet and SQL aggregates comparable.
        types = {f['name']: f['type'] for f in self.contract['fields']}
        sums = {}
        for name, value in self.sums.items():
            decimal = re.fullmatch(r'decimal128\(\d+,(\d+)\)', types[name])
            scale = int(decimal[1]) if decimal else 0
            sums[name] = format(Decimal(value), f'.{scale}f')
        return dict(rows=self.rows, first_key=self.first, last_key=self.last,
                    null_counts=self.nulls, sums=sums, row_sha256=self.hash.hexdigest())


class Store:
    def __init__(self, credentials=None, bucket=None):
        values = json.loads(Path(credentials or os.environ['S3_CREDENTIALS_FILE']).read_text())
        self.bucket = bucket or os.environ['S3_BUCKET']
        self.client = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
            region_name='us-east-1', aws_access_key_id=values['access_key'],
            aws_secret_access_key=values['secret_key'], config=Config(
                s3={'addressing_style': 'path'}, retries={'mode':'standard','max_attempts':4},
                connect_timeout=5, read_timeout=60, request_checksum_calculation='when_required',
                response_checksum_validation='when_required'))

    def get(self, key, limit=8*1024*1024, optional=False):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if optional and exc.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise
        with response['Body'] as stream:
            require(response['ContentLength'] <= limit, 'Object exceeds size limit')
            body = stream.read(limit + 1)
        require(len(body) == response['ContentLength'], 'Truncated object')
        return body

    def immutable_json(self, key, value):
        body = encoded(value)
        previous = self.get(key, optional=True)
        if previous is None:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType='application/json')
        else:
            require(previous == body, 'Existing object differs; refusing overwrite')
        require(self.get(key) == body, 'JSON read-back mismatch')
        return digest(body)

    def upload(self, key, path):
        size = path.stat().st_size
        # Unique attempt paths, one owner under the local request lock; no multipart or rename assumptions.
        with path.open('rb') as body:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body,
                ContentLength=size, ContentType='application/vnd.apache.parquet')

    def download(self, key, path, expected_size, expected_sha, limit):
        require(0 < expected_size <= limit, 'Invalid file size')
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        h, size = hashlib.sha256(), 0
        with response['Body'] as source, path.open('wb') as target:
            require(response['ContentLength'] == expected_size, 'File size mismatch')
            while chunk := source.read(1024*1024):
                size += len(chunk)
                require(size <= expected_size, 'Oversized file')
                h.update(chunk)
                target.write(chunk)
        require(size == expected_size and h.hexdigest() == expected_sha, 'File checksum mismatch')

    def list(self, prefix):
        for page in self.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=prefix):
            for entry in page.get('Contents', []):
                yield entry['Key']


def paths(contract, run_id):
    require(re.fullmatch(r'\d{8}T\d{6}Z-[0-9a-f]{32}', run_id), 'Invalid run identifier')
    day = f'{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}'
    part = f"dataset={contract['dataset']}/version={contract['version']}/run_date={day}/run={run_id}"
    return f"{contract['data_prefix']}/{part}", f"{contract['publication_prefix']}/{part}.json"


def verify_manifest(store, contract, marker, publication, *, on_batch=None):
    """Validate the entire delivery. An optional sink must commit only after this returns."""
    require(store.bucket == contract['bucket'], 'Bucket differs from contract')
    root, expected_publication = paths(contract, marker['run_id'])
    require(publication == expected_publication, 'Unexpected publication path')
    require(marker['protocol'] == 'full-export/v1' and marker['dataset'] == contract['dataset']
            and marker['version'] == contract['version'], 'Unsupported publication contract')
    require(marker['manifest_key'] == root + '/manifest.json', 'Unexpected manifest path')
    body = store.get(marker['manifest_key'])
    require(digest(body) == marker['manifest_sha256'], 'Manifest checksum mismatch')
    manifest = json.loads(body)
    require(manifest['contract'] == contract and manifest['contract_sha256'] == digest(encoded(contract)),
            'Consumer contract differs from manifest')
    require(manifest['run_id'] == marker['run_id'] and manifest['protocol'] == marker['protocol'], 'Run mismatch')
    require(0 < len(manifest['files']) <= 10000, 'Invalid file count')
    combined = Stats(contract)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)/'part.parquet'
        for index, item in enumerate(manifest['files'], 1):
            require(item['key'] == f'{root}/data/part-{index:05d}.parquet', 'Unexpected data path')
            store.download(item['key'], path, item['bytes'], item['sha256'], contract['max_file_bytes'])
            file = pq.ParquetFile(path)
            try:
                require(file.schema_arrow.equals(arrow_schema(contract), check_metadata=False), 'Parquet schema mismatch')
                before = combined.rows
                for batch in file.iter_batches(batch_size=contract['row_group_rows'], use_threads=False):
                    table = pa.Table.from_batches([batch])
                    combined.add(table)
                    if on_batch is not None:
                        on_batch(table)
                require(combined.rows - before == item['rows'], 'File row count mismatch')
            finally:
                file.close()
            path.unlink()
    result = combined.result()
    require(result == manifest['statistics'], 'Dataset statistics mismatch')
    expected = manifest['source_aggregates']
    require(result['rows'] == expected['rows'] and result['sums'] == expected['sums'], 'Source reconciliation mismatch')
    require(contract['allow_empty'] or result['rows'] > 0, 'Empty delivery rejected')
    return manifest
