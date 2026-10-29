# Invoice delivery through RabbitMQ

Exactly one demonstration application uses RabbitMQ: `invoice-app`. The dispatcher watches committed `invoice_details/v1` exports in SeaweedFS every 30 seconds. RabbitMQ carries a small notification; the application retrieves the Parquet data with its own read-only S3 identity. Order exports and other downstream readers keep their direct SeaweedFS/PostgreSQL access.

```text
PostgreSQL → validated invoice export in SeaweedFS → dispatcher → RabbitMQ → invoice-app
                                             SeaweedFS ── Parquet files ──→ invoice-app
                                                                          ↓
                                                               local invoice read model
```

The application materializes the complete invoice-line projection in a persistent SQLite database. It is a runnable consumer with durable business data, delivery history and a current-snapshot pointer. It has no connection or credentials to the warehouse/source database. It is not an existing business application's production cutover.

## Delivery contract

Only publication markers under `published/dataset=invoice_details/version=1/` are eligible. Notifications contain `type=export.ready/v1`, dataset/version, bucket, publication key, run ID, manifest SHA-256 and a deterministic event ID computed over those fields. No credentials, executable SQL, arbitrary endpoint URL or invoice rows are in the message. The consumer checks the exact schema, message ID, configured bucket/path and committed marker before reading files.

The dispatcher scans the complete publication prefix on each pass, so a late-visible older marker is not skipped by a maximum-date cursor. It records each immutable notification in a local SQLite outbox **before** sending it. Persistent messages use publisher confirms and `mandatory` routing. Only a confirmed, routed publication is marked sent. A changed marker at an already recorded key blocks the scan for operator review.

A publisher confirmation means that RabbitMQ accepted the message, not that the application processed it. An uncertain send is retried with the same event ID and can produce duplicates. The application's durable delivery ledger handles these duplicates.

The consumer uses prefetch 1 and a single active consumer. Data validation and materialization run in one worker thread while the AMQP thread continues servicing heartbeats. One SQLite transaction contains the full dataset replacement, the delivery record and the active pointer. It commits only after the shared export verifier has checked every file, schema, key, checksum, row count and exact total. ACK is sent afterward.

The local read model stores invoice lines by ID with invoice/customer/date lookup fields and the complete validated row as JSON. Decimal amounts remain exact strings in that JSON; they are never converted to binary floating point. A new complete snapshot replaces the old one, including removal of rows absent from the new snapshot. Older, previously unseen deliveries are validated and recorded as `superseded`; they cannot replace the current snapshot. Ordering is the normalized UTC warehouse capture time, then run ID. This is a project policy, not a certified source transaction order.

SQLite uses the rollback journal (`DELETE`) and `synchronous=EXTRA`, with one writer per state directory protected by an OS lock. This avoids the documented WAL-reset race in SQLite versions before 3.51.3, including the base image's 3.40.1. It also avoids introducing another database service for this lab application. Read/commit contention is possible; this design is not a high-concurrency application database benchmark.

## Broker, errors and permissions

Pinned dependencies: RabbitMQ **4.4.0-management** by image digest, Pika **1.4.4**, and the export framework's pinned Arrow/S3 packages. The broker uses the stable node name `rabbit@rabbitmq` and persistent data/cookie storage.

| Resource in vhost `/exports` | Configuration |
| --- | --- |
| `exports.ready` | Durable direct exchange; routing key `invoice_details.v1` |
| `invoice-app.ready` | Durable quorum queue, one member in this lab, single active consumer |
| `exports.dead` → `invoice-app.dead` | Durable dead-letter exchange and quorum queue |
| Ready-queue policy | Delivery limit 5; at-least-once dead lettering; reject-publish overflow; maximum 10,000 messages |
| Dead-queue policy | No automatic expiry; reject-publish overflow; maximum 10,000 messages |

Malformed notifications, missing published objects and integrity failures are rejected to the DLQ. Transient processing failures wait five seconds, then use `basic.reject(requeue=True)`. RabbitMQ 4.3+ increments failed-delivery count for `basic.reject`, while `basic.nack` does not; the distinction matters for bounded retries. The validation fixture reached the DLQ after six failed attempts with the configured limit. DLQ contents require operator review; there is no automatic infinite replay.

Runtime accounts have separate generated file-mounted secrets:

- `export_publisher`: may publish only to `exports.ready`; cannot consume or configure queues. Its S3 identity lists the export bucket and reads invoice publication markers only, not Parquet files.
- `invoice_app`: may consume only `invoice-app.ready`; cannot publish, configure queues or consume the DLQ. Its S3 identity reads invoice publications/data only; order exports, writes and CDC raw access are denied.
- `stefen`: lab administrator for management/provisioning. This secret is never mounted into either runtime worker. Broker definitions contain salted password hashes; there is no guest user.

The message network is internal. A separate bridge attached only to the broker enables Docker's loopback management-port publishing; AMQP has no host-published port. Workers also join the existing archive network, with no warehouse/source network membership. The filer and Docker networks remain trusted lab surfaces, not hostile-tenant isolation. HTTP/AMQP are unencrypted inside the VM; management access uses SSH/IAP. TLS, identity rotation automation and external alerting remain future work.

## Run and inspect

On the VM:

```bash
cd /srv/decom-sqlserver/workspace/services/rabbitmq
sudo bash scripts/install-host.sh
sudo docker compose up -d --wait dispatcher invoice-app
sudo docker compose ps
sudo docker compose logs --tail 30 dispatcher invoice-app
sudo docker compose run --rm --no-deps invoice-app invoice_app.py status
sudo docker compose run --rm --no-deps invoice-app audit_app.py
sudo docker compose run --rm --no-deps probe inspect_broker.py
```

The installer prepares the broker but does not initially start the workers, allowing first-deployment recovery tests with queued messages. Once started, the dispatcher and application watch continuously. [Airflow](../airflow/README.md) now schedules exports and checks the invoice application's durable receipt; on-demand export commands remain available.

On the Mac, run `bash infra/scripts/ui-tunnel.sh` and leave the terminal open. RabbitMQ management is at http://localhost:15672/; select vhost `/exports`. The ignored `.secrets/rabbitmq-access.txt` contains the generated lab administrator login. The same tunnel script includes CloudBeaver and SeaweedFS.

| Persistent path under `/srv/decom-sqlserver` | Contents |
| --- | --- |
| `secrets/messaging/` | Broker definitions, service credentials and scoped S3 keys |
| `messaging/broker/` | Broker data, stable node metadata and Erlang cookie |
| `messaging/dispatcher/outbox.sqlite3` | Discovery/send journal, stable event IDs, attempts and confirmations |
| `messaging/app/invoices.sqlite3` | Current invoice lines, accepted deliveries, active pointer and processing attempts |
| `messaging/validation/` | Isolated test databases and sanitized protocol report |
| `validation/rabbitmq-invoice-app-v1-2026-10-29.json` | Full acceptance evidence |

Service limits are 768 MiB / 1 CPU for RabbitMQ, 256 MiB / 0.5 CPU for the dispatcher and 512 MiB / 1 CPU for the app. Broker memory alarm threshold is 384 MiB; the disk alarm threshold is 2 GB. These are measured-lab defaults, not production throughput guarantees.

## Recovery and operator actions

- Before sending: an export marker remains discoverable; a committed outbox row remains pending after a dispatcher crash.
- After broker confirm but before journal update: replay may send the same event twice. Do not erase the outbox to force a resend.
- During application replacement: a crash rolls back the entire replacement. A corrupt later file also rolls back earlier inserted batches.
- After application commit but before ACK: redelivery finds the same durable event ID and ACKs without applying its data again.
- Broker/container restart: retain the node name, broker volume, secrets and both application state directories. The same-VM recreation tests are not disaster recovery.
- Poison message: inspect its `x-death` metadata in the DLQ and correlate the body hash with the app's `attempts` table. Repair the underlying cause before deciding to retry. Do not purge the queue to clear an alert.

For an explicitly reviewed replay of a known, valid notification, stop only the dispatcher to release its local state lock:

```bash
sudo docker compose stop dispatcher
sudo docker compose run --rm --no-deps dispatcher dispatcher.py --replay-event '<event ID from outbox/application status>'
sudo docker compose up -d --wait dispatcher
```

This republishes the same identity and does not remove any DLQ entry. An already committed application delivery remains deduplicated. Review and acknowledge the corresponding DLQ copy only after verifying the application's outcome. Preserve evidence of the original failure. A malformed/forged notification must not be blindly replayed.

For rollback, stop `dispatcher` and `invoice-app`, retain all broker/outbox/app data and return to the last verified image. Existing direct S3/PostgreSQL readers continue to work. Do not delete volumes, reset source offsets or remove published exports. Back up SQLite with its backup API or with writers stopped; do not copy only a live database file while ignoring its journal. Off-VM backup/restore and HA are not established. One-member quorum queues provide only one copy.

## Validation

```bash
sudo python3 scripts/verify-host.py
# After separately completing the isolated protocol tests:
sudo python3 scripts/verify-host.py --reuse-protocol-report
```

The suite retains a progress report to resume first-deployment checks without clearing application data. Nine shared-export regression tests and twenty isolated protocol/access checks cover outbox/ACK crash boundaries, full replacement/deletion, out-of-order snapshots, corrupt later files, forged notifications, bounded retry, unroutable publication and denied operations. The isolated vhost and synthetic S3 prefix are removed afterward; no business exports are corrupted for testing.

Real-data checks retain queued messages across broker recreation, crash the app after committing 228,265 invoice lines, recover without duplicate effects, recreate the application, reconcile every stored row to the accepted export, and create a fresh full export that the watch services deliver automatically. Ten services, unchanged CDC ledger counts and disabled source cleanup are checked. See [acceptance evidence](../../docs/validation/rabbitmq-invoice-app-v1-2026-10-29.json).

These are at-least-once notifications with idempotent effects inside this single SQLite transaction. They are not a guarantee of exactly-once external payments, emails or other future side effects. Neither an AMQP ACK nor a full-snapshot application checkpoint authorizes business CDC purge. **Automatic SQL CDC cleanup remains disabled.** The separate [manual technical pilot](../purge/README.md) is limited to `lab.CdcProbe`.

References: RabbitMQ [confirms and ACKs](https://www.rabbitmq.com/docs/confirms), [quorum queues and dead lettering](https://www.rabbitmq.com/docs/quorum-queues), [resource permissions](https://www.rabbitmq.com/docs/access-control), [definition import](https://www.rabbitmq.com/docs/definitions); Pika [thread/heartbeat considerations](https://pika.github.io/pika/latest/modules/adapters/blocking/); SQLite [rollback-journal/WAL behavior and WAL-reset fix](https://www.sqlite.org/wal.html) and [synchronous modes](https://www.sqlite.org/pragma.html#pragma_synchronous). The notification schema, local outbox, snapshot-selection rule and deployment topology are project decisions.
