# PostgreSQL to SeaweedFS full exports

One reusable, on-demand Docker job exports `delivery.invoice_details_v1` and `delivery.order_details_v1` to Parquet. These stable views expose `mart.invoice_details` and `mart.order_details`. A separate demonstration consumer reads and validates deliveries using only its own S3 credentials. No permanent service is required for the export job itself. The [invoice application](../rabbitmq/README.md) now receives notifications through RabbitMQ and reads these same validated invoice files; order exports remain available for direct S3 consumption.

[Airflow](../airflow/README.md) now schedules both datasets daily and invokes these same bounded jobs. Retrying a task retains the DAG run's export request ID. Manual commands remain available for reviewed operator actions.

The checked-in [invoice](contracts/invoice_details.v1.json) and [order](contracts/order_details.v1.json) contracts specify the source view, selected columns, types, nullability, primary key, control totals, bucket, prefixes and chunk sizes. Configuration is deployed by an operator, not accepted from an untrusted application. Adding a dataset requires a reviewed view/grant and a new contract; the engine does not need another dataset-specific exporter.

Validated on 2026-10-28 with the independent S3 consumer:

| Dataset | Rows per full delivery | Parquet files | Compressed bytes |
| --- | ---: | ---: | ---: |
| `invoice_details/v1` | 228,265 | 5 | 4,529,968 |
| `order_details/v1` | 231,412 | 5 | 3,044,159 |

The [acceptance report](../../docs/validation/warehouse-exports-v1-2026-10-28.json) contains exact published keys, nine contract tests, twelve live protocol/access checks, six consumer-access/configuration checks, control totals and seven healthy services. The retained earlier validation runs are independent full snapshots, not additional rows to append. Browse the bucket through the existing SSH tunnel at http://localhost:8888/buckets/warehouse-exports/; use `published/` for the application completion contract.

## Delivery layout and reading rules

```text
s3://warehouse-exports/
  datasets/dataset=invoice_details/version=1/run_date=2026-10-28/
    run=<UTC timestamp>-<unique attempt id>/
      data/part-00001.parquet
      data/part-00002.parquet
      ...
      manifest.json
  published/dataset=invoice_details/version=1/run_date=2026-10-28/
    run=<same attempt id>.json
```

The day is the **UTC extraction attempt start date**, not the invoice/order business date. Each run contains a full current-state snapshot. The dataset, contract version and delivery date are readable directory names. File numbering is sequential. The raw `cdc-raw` bucket is unchanged.

Consumers discover **only `published/`**, then follow the manifest and its explicit file list. Do not glob all Parquet files under `datasets/`, combine different full runs, or infer completion from a directory, a manifest alone or an S3 ETag. The publication contains the manifest SHA-256; the manifest contains the contract, provenance, row counts, exact totals, complete ordered-row SHA-256 and each file's SHA-256/byte length. Consumers verify these before acknowledging or replacing application state. There is no mutable `latest` pointer; applications select a specific published run. Listings can be stale without making incomplete runs eligible.

The exporter uploads numbered parts to a unique attempt path, writes the manifest, rereads **every uploaded file** and reconciles its content, durably records a ready attempt, then writes the publication object last. This is an application protocol, not an atomic multi-object S3 transaction. The code does not assume an S3 rename, conditional-write compatibility, ETag=SHA-256 or bucket versioning. A published run is immutable by convention; the writer's S3 permission can still overwrite/delete it, so this is not WORM storage.

## Data semantics and resource limits

- One PostgreSQL `REPEATABLE READ READ ONLY` transaction spans source control totals and the ordered server-side cursor. The invoice and order jobs have **separate snapshots**; they do not form an atomic multi-dataset release.
- Invoice grain: one current invoice line. Order grain: one current order line. Keys must be strictly increasing and unique. Missing required parent/customer fields block publication. Invoice `order_id` and both datasets' `unit_price` preserve legitimate NULL values. Credit quantities and amounts retain their source sign.
- Amounts are decimal128(18,2), tax rates decimal128(18,3), dates date32, IDs int64 and quantities int32. Arrow rejects precision loss and overflow. No binary floating point or silent rounding is used for money.
- Defaults: 5,000 rows per fetch/row group, 50,000 rows per file, Zstandard compression, Parquet 2.6, maximum 128 MiB per file and 10,000 files. Only one file is staged/downloaded at a time. The job has a 768 MiB memory limit, 1 CPU and 256 MiB temporary filesystem; the consumer has 512 MiB. Database statements have a 10-minute timeout and the transaction a 30-minute limit. These lab settings are adjustable after measurement; they do not certify large-volume capacity.
- The manifest records visible `sales/v2` ingestion counts and the PostgreSQL snapshot identifier. These are provenance, **not a contiguous SQL Server CDC watermark**. The loader commits archive batches independently; cross-table source transactions can still be partly applied. Required-field checks catch some gaps, not all source transaction inconsistencies. No freshness SLA or source cutover boundary is certified.
- Full snapshots represent current rows only. Applications replace their previous full dataset after validation so absent rows disappear; appending full runs would duplicate data. There is no incremental deletion feed, SQL Server procedure emulation or RabbitMQ delivery in this phase.

## Deploy and run on the VM

Run on the existing Linux VM, with the repository synchronized under `/srv/decom-sqlserver/workspace`. Do not start these services on the memory-constrained Mac.

```bash
cd /srv/decom-sqlserver/workspace/services/export
sudo bash scripts/install-host.sh

sudo docker compose run --rm --no-deps exporter export.py \
  --dataset invoice_details --request-id invoice-full-001
sudo docker compose run --rm --no-deps exporter export.py \
  --dataset order_details --request-id order-full-001

sudo docker compose run --rm --no-deps consumer consumer.py list --dataset invoice_details
sudo docker compose run --rm --no-deps consumer consumer.py read \
  --dataset invoice_details --publication '<published/.../run=....json key from list>'
```

Use a **new request ID for a new full snapshot**. Repeat the same command/ID to recover or verify the original request. The consumer command persists a local verification acknowledgement only after all files pass. It demonstrates S3 consumption; a real application's transactional staging/replacement and business side effects still need implementation.

Protected host paths:

| Path under `/srv/decom-sqlserver` | Purpose |
| --- | --- |
| `secrets/export/pg_password` | Dedicated `warehouse_exporter` PostgreSQL login |
| `secrets/export/s3-writer.json` | Read/write/list on `warehouse-exports` only |
| `secrets/export/s3-consumer.json` | Independent read/list identity on `warehouse-exports` only |
| `export/requests/` | Durable request journal, attempt history and per-request locks |
| `export/consumer/` | Demo consumer acknowledgements keyed by manifest SHA-256 |
| `validation/warehouse-exports-v1-2026-10-28.json` | Sanitized acceptance evidence |

Passwords are generated once and mounted from files, never put into command arguments. Migration 005 adds narrow delivery views; the exporter receives SELECT on those views only. It receives neither raw archive access, underlying warehouse schema grants, DDL nor DML. The consumer receives no PostgreSQL secret or warehouse network membership. Deployment appends scoped S3 identities, briefly stops Debezium for the existing SeaweedFS restart procedure, preserves its offset volume, and resumes it. Existing credentials are retained.

S3 credential restrictions are tested. The internal Docker archive network and the loopback/IAP filer administration UI are **trusted lab surfaces**, not isolation against a hostile tenant. The unauthenticated filer interface is not a public application endpoint. Production network separation, TLS, identity lifecycle and audit/alerting remain to be designed.

## Recovery, validation and rollback

The local journal is fsynced and replaced atomically. A nonblocking OS lock prevents concurrent execution of the same request on this VM. It is not a distributed lock.

- Crash before `ready`: the old attempt becomes `abandoned`; the same request starts a new attempt with a new database snapshot and path. Its partial files remain for diagnosis and are not published. Do not resume halfway with a new snapshot.
- Crash after `ready`: revalidate the same stored manifest/data and publish the same marker. No new database read is necessary.
- Crash after the marker but before local acknowledgement: read back the existing marker, require identical bytes and mark the same attempt published. The publication is not rewritten.
- Corruption or missing data after `ready`: fail closed. Do not silently replace the published snapshot. Investigate/restore the original data or create a new request and explicitly switch the consumer.
- Lost journal/secrets: restore them with the S3 data before reusing request IDs. Exactly-once effects across host loss are not established. Do not delete the journal to force a retry.

```bash
sudo python3 scripts/verify-host.py
# Only after a completed standalone protocol run, reuse its durable report:
sudo python3 scripts/verify-host.py --reuse-protocol-report
```

Validation checks real complete exports; process crashes after the first file, after ready and after publication; idempotent retry; concurrent-request exclusion; contract drift; actual S3 corruption/missing files in an isolated synthetic prefix; exact numeric/null/key constraints; denied SQL/S3 operations; independent consumer reads; unchanged quiet-fixture CDC ledger counts; seven service health checks; and disabled SQL CDC cleanup. Re-running validation creates new delivery requests; it never alters business rows or existing business deliveries. Isolated synthetic S3 damage-test objects are removed; the abandoned full-export attempt is retained as evidence. Automatic retention/garbage collection is not enabled.

Rollback is additive: stop launching export jobs, let consumers keep their previously accepted snapshot, and retain the image, contracts, request journal, credentials and published objects. The warehouse loader does not depend on delivery views. Do not edit applied migration 005, remove data or reset CDC offsets. Any later revocation/removal is a reviewed follow-up migration, after checking downstream users.

The lab has one VM/disk, no off-VM export restore test, replication or HA. [Airflow scheduling](../airflow/README.md) and [RabbitMQ for one invoice application](../rabbitmq/README.md) are deployed. Larger-volume tuning/Spark assessment, additional consumer contracts, incremental exports and production application cutover are outside this POC. **Automatic CDC cleanup remains disabled.** The separate [technical purge pilot](../purge/README.md) covers only `lab.CdcProbe`; business cleanup is not authorized.

Design references: PostgreSQL [Repeatable Read](https://www.postgresql.org/docs/18/transaction-iso.html), Psycopg [server-side cursors](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html), Apache Arrow [ParquetWriter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) and [26.0.0 release](https://arrow.apache.org/release/26.0.0.html), SeaweedFS [bucket-scoped credentials](https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials). The path layout, ready/publication protocol and local request journal are explicit project conventions.
