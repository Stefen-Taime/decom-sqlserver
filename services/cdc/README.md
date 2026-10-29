# CDC capture without Kafka

`SQL Server → Debezium Server → HTTP archive receiver → SeaweedFS (S3)`

This stage captures five WWI business tables and `lab.CdcProbe`. The separate [PostgreSQL warehouse consumer](../warehouse/README.md) loads Customers and Orders from the verified archive. There is no Kafka broker or Kafka Connect service. Debezium still uses Kafka Connect **libraries** internally, including the file offset implementation; their class names in logs do not indicate a deployed broker.

Debezium Server 3.7.0.Final and SeaweedFS 4.51 are pinned by image digest. Three containers run alongside SQL Server: Debezium (1.5 GiB cap), the archive receiver (512 MiB), and SeaweedFS (1.5 GiB). CloudBeaver is optional and separately deployed.

## Deployment

Copy `services/cdc` and `services/sqlserver/sql/06-lab-reader-roles.sql` to the matching directories under `/srv/decom-sqlserver/workspace` on the lab VM, then run:

```bash
sudo bash /srv/decom-sqlserver/workspace/services/cdc/scripts/install-host.sh
```

The SQL Server service must already be healthy. The installer generates secrets once, provisions the connector login, builds the receiver, creates the bucket, applies the SeaweedFS `fsync` policy, and starts capture. It does not delete data or reset offsets on subsequent runs. The secret directory is `/srv/decom-sqlserver/secrets/cdc`; `.env` contains paths only.

Persistent state:

| Path on VM | Contents |
|---|---|
| `/srv/decom-sqlserver/cdc/seaweedfs` | Object volumes, filer metadata and master state |
| `/srv/decom-sqlserver/cdc/debezium` | Connector offsets and schema history |
| `s3://cdc-raw/raw/wwi/bootstrap-v2/` | Current archive generation: separate `data/` and `manifests/` |
| `/srv/decom-sqlserver/cdc/migration-reports` | Durable archive-layout migration reports |

Back up secrets and all persistent state together. Losing offsets or changing the archive prefix is a recovery operation, not a routine redeployment. Do not delete offsets to clear an error.

## Archive layout and reader contract

The active generation uses readable table/date partitions. For example:

```text
s3://cdc-raw/raw/wwi/bootstrap-v2/
  data/table=Sales.Customers/batch_date=2026-10-28/<sha256>.json.gz
  manifests/table=Sales.Customers/batch_date=2026-10-28/<sha256>.receipt.json
```

`data/` contains the original Debezium batches compressed with gzip. Decompress a `.json.gz` file to read the events. `manifests/` contains control receipts, not business rows. Readers discover completed batches through manifests and follow each `payload_key`, checking its checksum; they must not recursively treat every JSON file as an event file. The full SHA-256 filename identifies the exact original batch. There are no two-character hash directories in this generation.

`batch_date` is the UTC date of the earliest **envelope `payload.ts_ms`** in the batch: Debezium capture/processing time, not SQL commit time or business date. This makes paths stable when a retry happens the next day. Batches crossing midnight stay intact under their earliest date; manifests record both timestamp bounds. Batches containing multiple tables stay intact under `table=_mixed`; their manifests enumerate the tables. A table-specific consumer must also discover relevant `_mixed` batches.

Table/time partitioning and separate control files are established patterns, but these exact paths are a versioned project contract, not a universal industry standard. The lab has no measured need for hash fanout. Small raw batches will require a separate compaction/export stage for analytical consumers. See [the decision and primary references](../../docs/decisions/001-cdc-archive-layout.json).

## Acknowledgment and replay contract

1. Debezium sends JSON arrays of up to 200 value envelopes, signed using Standard Webhooks HMAC. Each envelope retains its schema and before/after/source fields. The HTTP sink does not send Kafka record keys; the configured primary key is available in the row image.
2. The receiver checks the signature, timestamp, source table, operation and primary key. Invalid requests never receive a success response.
3. The unchanged HTTP body is compressed with gzip and stored under its table/date partition with a SHA-256 filename. Exact-body retries address the same object, including across midnight. A valid existing gzip encoding is reused, preserving migrated files byte for byte.
4. The receiver reads the stored object back and checks its SHA-256, writes a deterministic receipt, reads that receipt back and verifies it. Only then does it return HTTP 200.
5. Debezium may commit the corresponding source offset after acknowledgment. Offsets and schema history are persisted on the data disk.

Delivery is **at least once**. Replaying the same events in a different batch can create additional objects. Downstream processing must deduplicate streaming events using their source coordinates, table, operation and primary key; snapshot rows require snapshot-generation and primary-key handling. Do not equate a content-addressed batch with exactly-once processing.

Receipts contain counts, checksums and observed minimum/maximum commit LSNs. Those bounds do **not** prove a contiguous consumed range and are **not** a safe purge checkpoint. Every receipt has `purge_authorized: false`.

The [purge readiness audit](../purge/README.md) compares retained post-bootstrap source CDC identities and all captured column values with the active archives, and checks warehouse acknowledgements. The audit remains read-only. A separate [manual technical pilot](../../docs/validation/cdc-purge-pilot-v1-2026-10-29.json) removed 48 old images only from `lab_CdcProbe`, preserving verified archives and a direct SQL-image evidence copy under `cdc-raw/control/purge-pilot/v1/`. Fresh CDC delivery and checkpoint advancement passed afterward. No automatic purge schedule is enabled.

The bucket has an explicit SeaweedFS path policy with `fsync=true` and one volume per growth operation. This is a single-VM lab with no storage replication or off-VM archive backup. It is not immutable/WORM storage. SQL Server automatic CDC cleanup remains disabled; no business rows are purged by this service.

## SQL Server details discovered during bootstrap

- WWI filters `Sales.Customers` using sales-territory roles. Without those memberships a limited login can read zero customers without any error. The shared `06-lab-reader-roles.sql` grants the connector and `lab_reader` membership in the existing policy's territory roles, creating empty roles if absent. The original predicate and security policy stay enabled. The connector installer compares customer row counts under SA and the connector identity before capture (663 in the initial fixture).
- The dedicated `debezium_cdc` login can read the six capture tables, CDC schema and required metadata/DMVs. It is neither SA nor a database owner.
- WWI's temporal `ValidTo` reaches `9999-12-31 23:59:59.9999999`. Epoch nanoseconds overflow an int64. `time.precision.mode=isostring` retains the value and fractional precision. Source `datetime2` itself has no timezone; the serializer's ISO UTC notation is not evidence of the original business timezone.
- Defaults such as sequence calls and `sysdatetime()` cannot be evaluated as literal defaults by Debezium. Their DDL belongs in the separate SQL-object migration workstream.
- During the first deployment an incomplete preflight snapshot exposed the row-security issue. Its objects remain outside the verified `bootstrap-v1` generation; its offset/history files were retained in `/srv/decom-sqlserver/cdc/preflight-state-20261028`. Only the verified v1 archive was migrated to v2. Consumers must use the active generation, not recursively ingest every historical experiment under `raw/wwi`.

## Migration from hash directories to table/date partitions

The `archive-migrate` tool copies verified v1 batches to v2, preserving both raw JSON and compressed bytes. It regenerates receipts with the new paths, verifies destination checksums, reconciles source/destination coverage, and writes a durable report. It never deletes source objects. Its default report is `/srv/decom-sqlserver/cdc/migration-reports/layout-v2-migration.json`, containing every source-to-target mapping. An incomplete run leaves a `.partial.json` status and can be rerun with the same inputs.

Maintenance sequence on the VM, after building and testing image `decom-cdc-archiver:2`:

1. Stop Debezium gracefully, then stop the old receiver. Keep SQL Server and SeaweedFS running. Preserve the old source bundle and receiver image, and copy the stopped connector state as a cutover backup.
2. Install the v2 Compose configuration and run the migration. Check the report's counts, byte-preservation flags and `source_unreceipted_payloads` (expected zero); investigate any discrepancy before resuming capture.
3. Validate the migrated snapshot using the updated reader, then start the v2 receiver and Debezium with the existing offset/history directory. Verify new CDC and recovery before updating downstream readers.

Commands for steps 2 and 3:

```bash
cd /srv/decom-sqlserver/workspace/services/cdc
sudo docker compose --profile tools run --rm archive-migrate
sudo docker compose --profile tools run --rm archive-migrate python verify_archive.py --snapshot
sudo docker compose up -d --wait archiver
sudo docker compose up -d --wait debezium
sudo python3 scripts/smoke-recovery.py
```

The old `bootstrap-v1` objects, including hash directories such as `6f`, are retained solely as the pre-cutover copy. They are not a second active feed. The rollback bundle is `/srv/decom-sqlserver/releases/cdc-layout-v1-20261028/source.tar.gz`; the stopped state backup is beside it in `debezium-state-cutover/`. Restoring the old writer after new v2 events have arrived requires reconciling those events first. Do not reset offsets, merge both generations blindly, or restore the cutover state during ordinary redeployment. Removal of the retained generation is a separate, reconciled retention decision.

## Checks and operations

On the VM:

```bash
cd /srv/decom-sqlserver/workspace/services/cdc
sudo docker compose ps
sudo docker compose logs --tail 30 debezium archiver
sudo docker compose exec -T archiver python verify_archive.py --snapshot
sudo python3 scripts/probe-host.py
# Use the UUID printed by the previous command:
sudo docker compose exec -T archiver python verify_archive.py --probe <UUID>
```

The snapshot verifier reads every receipt and payload in the active generation, verifies both checksums, counts unique primary keys and requires the snapshot completion marker. Expected fixture counts: Customers 663, Orders 73,595, OrderLines 231,412, Invoices 70,510, InvoiceLines 228,265. It is a fixture-coverage check, not a general reconciliation engine for a database being concurrently modified.

`probe-host.py` inserts, updates and deletes one UUID in `lab.CdcProbe`, without changing business tables. The verifier requires the corresponding `c`, `u`, `d` events and row-image values.

For a transient sink failure, Debezium retries and does not acknowledge the failed batch. If retries are exhausted or the connector encounters invalid source data, its engine can remain **unhealthy inside a running container**. Docker's restart policy alone does not recover that condition. Diagnose the cause, then `sudo docker compose restart debezium`, preserving its state. Operational alerting remains to be added.

The 21 receiver and migration tests cover signed requests, corruption, interrupted payload/receipt writes, retry idempotence, invalid source events/timestamps, cross-midnight and mixed-table batches, byte-preserving migration and interrupted-migration recovery:

```bash
python3 -m pytest services/cdc/tests -q
```

Tests need `boto3`, `pytest` and a local loopback socket. They were also run in a disposable Python container on the VM because the agent's local sandbox blocks socket binding.

After snapshot validation, `sudo python3 scripts/smoke-recovery.py` exercises a real storage outage and connector recreation. It temporarily stops SeaweedFS, requires receiver HTTP 503 and an unchanged source offset, restores storage, verifies the queued probe, then recreates Debezium and verifies another probe without a new snapshot. Run it during a lab maintenance window; it intentionally interrupts storage and capture briefly.

The separate [purge audit v3](../purge/README.md) now has a [validated persistent-resume exercise](../../docs/validation/cdc-local-resume-v1-2026-10-29.json). It decodes actual offsets, preserves and hashes schema history, verifies technical probes across a graceful Debezium recreation, and binds direct-mode command IDs to retained SQL CDC images. It does not stop storage or reset offsets. This bounded local test does not establish crash/power-loss durability, a complete transaction fence or off-VM restoration.

SeaweedFS browser access is described in [the CloudBeaver/UI guide](../cloudbeaver/README.md). The S3 endpoint is bound to VM loopback port 8333 and requires signed credentials. The receiver's loopback port 8090 exposes `/ready`; event writes additionally require the webhook signature.

References: [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html), [SQL Server connector](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html), [SeaweedFS S3 API](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API).
