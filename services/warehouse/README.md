# PostgreSQL sales warehouse

`SeaweedFS manifests → verified raw batches → PostgreSQL staging → Data Vault → query views`

The `sales/v2` consumer loads all five captured business tables from `raw/wwi/bootstrap-v2`:

| Source table | Current fixture rows | Query view |
|---|---:|---|
| Sales.Customers | 663 | mart.customers |
| Sales.Orders | 73,595 | mart.orders |
| Sales.OrderLines | 231,412 | mart.order_lines |
| Sales.Invoices | 70,510 | mart.invoices |
| Sales.InvoiceLines | 228,265 | mart.invoice_lines |

Total: **604,445 business rows**. The original two-table pilot remains documented in [its historical evidence](../../docs/validation/warehouse-pilot-2026-10-28.json). The extension has [separate validation evidence](../../docs/validation/warehouse-sales-v2-2026-10-28.json) and a [versioned scope/model decision](../../docs/decisions/003-five-table-sales-warehouse.json). No additional permanent service is needed.

## Deployment

Copy `services/warehouse`, `services/cdc/archiver/archive.py` and `services/.dockerignore` to the VM workspace, then:

```bash
sudo bash /srv/decom-sqlserver/workspace/services/warehouse/scripts/install-host.sh
cd /srv/decom-sqlserver/workspace/services/warehouse
sudo python3 scripts/test-host.py
sudo docker compose run --rm --no-deps backfill
sudo docker compose up -d --wait --wait-timeout 360 loader
```

The installer preserves generated secrets and existing PostgreSQL storage. SQL migrations are recorded with checksums; modifying an applied migration fails deployment. Add a new numbered SQL file for schema changes. `load-batch.sql` is the transactional DML used by the loader, not a schema migration.

PostgreSQL 18.6 and the Python base image are pinned by digest. Python package versions are pinned. PostgreSQL has a 1,280 MiB memory cap, 256 MiB shared buffers, 30 connections and a 1 CPU cap. The loader has a 512 MiB memory cap and 0.75 CPU cap. It polls every 30 seconds. The one-off `backfill` Compose profile runs with 768 MiB and 1.5 CPU. Stop the watch worker for backfill or replay; read-only reconciliation also uses this bounded profile. These are lab limits, not measured production capacity.

An existing two-table deployment must use the scope upgrade procedure below before starting version 2.

The first installation adds a **read/list-only S3 identity** for `cdc-raw`, briefly pauses Debezium, restarts SeaweedFS to load that identity and resumes capture using the existing offsets. It never resets the capture state.

## Storage, access and query surfaces

| Component | Location / identity |
|---|---|
| PostgreSQL data | `/srv/decom-sqlserver/warehouse/postgres/18/docker` |
| Generated secrets | `/srv/decom-sqlserver/secrets/warehouse` |
| Database | `warehouse` |
| Schema owner | `warehouse_owner`, no login |
| Loader | `warehouse_loader`, limited DML; no permanent DDL or DELETE |
| Reader | `warehouse_reader`, SELECT on vault and query views; no writes |
| Host connection | VM loopback `127.0.0.1:5432`; never public |
| Docker connection | `postgres:5432` on internal `decom-warehouse-backend` |
| Browser | Existing CloudBeaver, connection **Warehouse — PostgreSQL (read only)** |

Secrets are mounted as files. The loader cannot access SQL Server, admin credentials or the S3 writer identity. PostgreSQL host authentication uses SCRAM; connections within the single VM's Docker network are not configured with TLS. Remote browser access uses the SSH/IAP tunnel. This network boundary is a stated lab simplification.

```sql
SELECT * FROM mart.customers LIMIT 20;
SELECT * FROM mart.orders ORDER BY order_id LIMIT 20;
SELECT * FROM mart.order_details ORDER BY order_line_id LIMIT 20;
SELECT * FROM mart.invoice_details ORDER BY invoice_line_id LIMIT 20;
SELECT * FROM mart.invoice_summary ORDER BY invoice_id LIMIT 20;
SELECT * FROM ops.load_summary;
SELECT * FROM ops.consumer_load_summary;
SELECT * FROM ops.consumer_status;
```

The prepared CloudBeaver connection is added by its installer when warehouse secrets exist. Stop CloudBeaver before changing its persisted connection file; existing user connections and credentials must be preserved. Verify it using `sudo python3 scripts/verify-cloudbeaver-host.py --initialize-connection` once, then without that option for subsequent checks.

## Data model

- `staging.cdc_event`: persistent source-event journal with source coordinates, row image, SHA-256 identities, capture time and first archive-batch provenance.
- `staging.current_key`: latest event pointer for each source key; updated only for a strictly newer source position.
- Five hubs: `h_customer`, `h_order`, `h_order_line`, `h_invoice`, `h_invoice_line`. Missing parent events can create inferred hubs; a hub alone is not a current application row.
- Six links: order/customer, order-line/order, invoice/sold-to customer, invoice/bill-to customer, optional invoice/order and invoice-line/invoice. Historical relationships remain available.
- Five append-only event satellites, with attributes, hashdiff, deletion state, load timestamp and source. Distinct CDC events remain visible even if their hashdiff is unchanged.
- Five `mart` base views expose current, non-deleted rows. `order_details` has one row per order line; `invoice_details` one per invoice line. `invoice_summary` aggregates lines before joining invoices.
- `ops.archive_batches` preserves first-seen batch provenance, including original v1 statistics. `ops.consumer_batches` records atomic completion for each **(consumer_version, batch_id)**. `ops.consumer_contracts` fixes the approved scope and source generation.

Invoice `CustomerID` and `BillToCustomerID` are different roles. `OrderID` may be NULL. Invoice lines have no direct order-line key: joining the two kinds of lines on product IDs would multiply rows and is not implemented. Money uses exact `numeric`; source credit-note flags and signed amounts are retained.

SQL Server CDC returns NULL for computed columns in change tables. The invoice view rebuilds `ConfirmedDeliveryTime` and `ConfirmedReceivedBy` from `ReturnedDeliveryData`, while retaining unmodified raw images. The delivery-time field is ISO text so its seventh fractional digit is not rounded to PostgreSQL's six-digit timestamp precision. This is a fixture-specific ISO conversion contract, not a general T-SQL conversion engine; see the decision for its limits and official references.

## Unavailable-value reconstruction

Loader image `decom-warehouse-loader:3` adds the separate **`cdc-values/v1`** derived contract while retaining the raw `sales/v2` ledger and satellites. Migration 006 adds sparse `business.resolution_input`, `business.marker_proof` and `business.event_resolution` tables. The source archive and raw satellite values remain unchanged, including Debezium's unavailable-value markers.

A marker is resolved only when the source CDC type, ordinal, operation and update mask prove an unchanged large-object column, and an earlier live archived state establishes its value. Explicit NULL counts as a known value; a missing field or unproved marker does not. Deletes end a lifecycle. The resolver replays affected keys in source order and recomputes dependent images when an earlier event arrives late. A repeated identical batch does not change an existing derived revision or timestamp.

Each reconstructed before/after cell links to its original known-value event, source-observation hash and marker-proof hash. The operator generates proofs from a stable retained source observation and checksummed archives. The loader can read proofs but cannot certify its own evidence. A literal marker assignment, unsupported column, missing predecessor or missing/contradictory proof remains blocked.

Validation independently re-reads the original value event from its checksummed archive, checks its key, source ordering and ledger hash, and compares it with the original SQL CDC image when that image is still retained. Non-marker columns are compared directly with the target source image. A source before-image NULL that denotes an unavailable value is not required to equal a reconstructed non-NULL string: the report distinguishes this literal representation comparison from validated historical reconstruction.

Existing `mart` views expose resolved attributes with the same column names and types. An unresolved current row raises an error; it is not silently removed from results. The full-export ingestion-context check also blocks publication while current rows remain unresolved. Deleted historical marker events stay available in the raw and derived history without blocking current reads.

```sql
SELECT * FROM ops.value_resolution_status;
SELECT event_id,status,lineage,problems,resolved_at
FROM business.event_resolution ORDER BY event_id;
```

On-demand proof generation and historical reconstruction:

```bash
cd /srv/decom-sqlserver/workspace/services/warehouse
sudo python3 scripts/certify-values-host.py
sudo docker compose run --rm --no-deps backfill python verify_values.py
```

These commands use protected per-run observations under `/srv/decom-sqlserver/validation/cdc-values-<run>/`. Inputs contain business values and must not enter the repository. Proof generation is currently an operator step; raw ingestion continues when a new marker lacks proof, and protected business reads remain blocked until the evidence is available. The first version supports the captured long-text columns in Orders and Invoices. Other types/sources require reviewed mappings.

For a code/schema upgrade, build the loader and run `sudo python3 scripts/deploy-values-host.py`. It runs the warehouse and SQL-object tests in disposable databases, retains a full same-VM backup and prior image, applies checksum-ledgered migrations, replays archives, certifies values and checks real replay stability. It restarts the loader with the compatible image even when later certification reports a blocked case. Source observation, proof preparation and per-key histories have explicit bounds; see [decision 010](../../docs/decisions/010-cdc-value-reconstruction.json).

The prior code/image and full backup are protected under `/srv/decom-sqlserver/releases/cdc-values-v1-20261029/`. Keep raw capture running during investigation. The safe operational fallback is to stop downstream publication and repair/rebuild the derived projection; reverting to raw business views would expose unresolved markers. A database restore would require a coordinated maintenance window and replay of subsequent archive events, not an automatic rollback while capture is active.

The [2026-10-29 acceptance report](../../docs/validation/cdc-value-reconstruction-v1-2026-10-29.json) records 51 warehouse/proof tests and 25 SQL-object tests passed. All ten marker cells across four images of two update events were reconstructed: eight known NULLs and two non-NULL strings. Their original values were independently checked in three verified archive batches and in retained SQL CDC. Two affected batches replayed with no raw inserts or derived revision changes. Full five-table business reconciliation compared all 604,445 current rows and the declared business projections with zero differences, missing keys or orphan relationships. The raw journal hash, 604,459 events and 3,027 sales/v2 batch acknowledgements stayed unchanged; all 13 permanent services were healthy.

The historical [post-reconstruction purge audit](../../docs/validation/cdc-purge-after-reconstruction-2026-10-29.json) passed its 50 safety tests and verified all 3,033 active manifests. Its strict raw-value gate remains false, as expected: original unavailable markers have not been rewritten to reconstructed values. The subsequent [v3 audit](../purge/README.md) explicitly revalidates reconstruction and local connector resume evidence. Production transaction boundaries, retention/consumer approval and off-VM recovery remain separate requirements. The warehouse deployment itself enables no purge.

All five primary keys are **source-scoped technical keys**. Cross-application business keys have not been established. JSONB event satellites preserve source attributes; domain-specific analytical models remain a later step. The original [pilot modeling decision](../../docs/decisions/002-warehouse-pilot.json) remains historical context.

## First SQL-object migration: order updates

`Integration.GetOrderUpdates` is ported to **`app.get_order_updates(last_cutoff, new_cutoff, reference_version)`**, a read-only PostgreSQL set-returning function. The [migration decision](../../docs/decisions/004-sql-object-pilot.json), [42-procedure catalog](../../docs/migration/sql-object-catalog-2026-10-28.json) and [validation report](../../docs/validation/sql-object-pilot-2026-10-28.json) describe the contract and evidence.

It returns all 18 original output columns, including order/line amounts, customer/city keys and the latest order-or-line edit time. The cutoff window is **(last_cutoff, new_cutoff]**. `Last Modified When` uses ISO text with seven fractional digits to preserve SQL Server datetime2(7); ISO date-only cutoffs mean midnight. Timezone offsets and non-ISO date coercions are outside this API contract. NULL, equal and reversed cutoff windows return no rows.

The missing `Warehouse.PackageTypes` dependency is a protected, checksummed, immutable reference snapshot in `ref.package_types`, with provenance in `ops.reference_snapshots`. It is **not a new CDC stream**. Select and pin a specific version before calling the API:

```sql
SELECT snapshot_version, source_captured_at, row_count, refresh_mode
FROM ops.reference_snapshots
WHERE source_table = 'Warehouse.PackageTypes';

-- Replace the third argument with the chosen version from the previous query.
SELECT * FROM app.get_order_updates(
    '2016-05-30T00:00:00',
    '2016-06-01T00:00:00',
    '<chosen snapshot_version>'
)
ORDER BY "WWI Order ID"
LIMIT 20;
```

Unknown/incomplete snapshots or a missing package key fail rather than silently removing output rows. Keep existing versions; capture and validate a new reference snapshot before consumers adopt it. Reference refresh is currently explicit and is not automated. The function queries current rows: it does not emit deleted rows or historical versions, and a customer/package-only change does not advance its order/line cutoff. These limitations also matter when designing a downstream incremental export.

The source procedure executes as owner. The target uses **SECURITY INVOKER**, a fixed search path and explicit reader grants; PUBLIC execution is revoked. The validated identity is the existing full-scope `warehouse_reader`, available in CloudBeaver. Application-specific permissions and source customer territory RLS still need a separate mapping. Source schemas are not proof of actual application ownership, and no source procedure is approved for deletion.

Validation uses an isolated `warehouse_sp_test` database (25 tests), seven actual source-procedure result sets including the complete current result, twelve numeric golden cases evaluated by SQL Server, overflow checks and a CloudBeaver query. Comparison preserves duplicate rows, compares exact decimals and seven-digit dates, and treats the source's unspecified order-line tie order as a multiset within each OrderID. Source business tables and procedure definitions remain unchanged.

```bash
cd /srv/decom-sqlserver/workspace/services/warehouse
sudo python3 scripts/test-order-updates-host.py
# Initial evidence directories already exist. Use fresh directories for a new inventory/export.
sudo python3 scripts/inventory-sql-objects-host.py --output-dir /srv/decom-sqlserver/validation/sql-objects-<new-run>
sudo python3 scripts/export-order-updates-host.py --inventory-dir /srv/decom-sqlserver/validation/sql-objects-<new-run> --output-dir /srv/decom-sqlserver/validation/sql-objects-<new-run>/order-updates-results
```

`deploy-order-updates-host.py` records a protected schema recovery point, applies checksum-ledgered migrations 003 and 004 and loads the initial reference atomically. `load-package-reference-host.py` validates and loads an explicit reference bundle idempotently. `compare-order-updates.py` is the reusable bounded-memory comparison worker; `verify-order-updates-host.py` reproduces this initial lab's verification and records its evidence. Source exports hold SERIALIZABLE read locks during the quiet validation window; this is not a general online cutover protocol.

To disable the API without changing CDC ingestion or deleting any history:

```sql
REVOKE EXECUTE ON FUNCTION app.get_order_updates(text,text,text) FROM warehouse_reader;
```

Apply corrections with a subsequent numbered migration. The source procedure remains available; application switching, per-application identities, automatic reference refresh, safe purge and SQL Server retirement are not authorized by this pilot.

## Replay, ordering and checkpoint contract

1. Validate the owner-controlled consumer contract, then discover receipts in the five table partitions **and `_mixed`**. Verify the receipt contract, canonical paths, compressed and raw checksums, record counts, table counts, operation counts and capture-date bounds before loading.
2. Identify snapshot events by source instance, snapshot generation, table and key. The original snapshot's commit LSN is fixed in Compose. Accept Debezium's five initial-snapshot boundary markers; reject incremental snapshots and unexpected new snapshot positions.
3. Identify streaming events by source instance, table, key, operation, commit LSN, change LSN and event serial number. Capture timestamps and batch boundaries do not determine identity. A repeated identity with different row content fails the batch.
4. In one PostgreSQL transaction, take a writer advisory lock, COPY validated events into a temporary table, append distinct journal/vault events, update current pointers and acknowledge the batch for the current consumer version. An exception rolls everything back. Exact batch replay does nothing; a different batch containing the same events adds no duplicate events.
5. Order current state by snapshot/stream phase, binary commit LSN, binary change LSN and event serial number. Snapshot data never overwrites a streaming result; late older events remain in history without replacing current state. Deletes retain history and remove the row from current views; a later create can restore it.

This is at-least-once delivery with transactional deduplication **inside this consumer**, not a claim of end-to-end exactly-once processing. Transactions are atomic per archive batch, not necessarily per complete SQL Server transaction if that transaction spans batches. Polling has eventual consistency and does not establish a contiguous source watermark.

Current views can be partially populated during the first load. Application cutover and downstream publication must wait for explicit reconciliation. Discovery currently lists the selected manifest prefixes and keeps completed manifest keys in memory; that is suitable for this measured pilot, not a demonstrated design for millions of batches. A scalable discovery catalog and raw-file compaction remain future work.

Already acknowledged batches are skipped during routine polling. `--replay` re-reads and re-verifies them. Expanding the selected tables, changing source instances, creating a new snapshot, or changing archive generations requires a versioned backfill/checkpoint plan; do not reuse the ledger silently.

## Validation and operations

```bash
cd /srv/decom-sqlserver/workspace/services/warehouse
sudo docker compose ps
sudo docker compose logs --tail 30 loader
sudo docker compose run --rm --no-deps loader python check-access.py
sudo python3 scripts/test-host.py
```

Tests use an isolated `warehouse_test` database which is removed afterward. They cover rollback before acknowledgment, idempotent and rebatched delivery, conflicting event content, late events and deletes, changed relationships, child-first delivery, optional invoice/order links, distinct customer roles, exact money, derived invoice fields, detail-view grains, v1-to-v2 mixed-batch replay, contract drift, corrupt archives, least privilege and snapshot boundaries/generation checks. The extension suite contains 28 tests.

For full five-table lab validation (live probes and restart checks intentionally exercise the pipeline):

```bash
sudo python3 scripts/verify-sales-host.py
sudo python3 scripts/source-sales-export.py --output-dir /srv/decom-sqlserver/validation/source-sales-v2-after
sudo python3 scripts/reconcile-host.py /srv/decom-sqlserver/validation/source-sales-v2-after --sales --output /srv/decom-sqlserver/validation/warehouse-sales-v2-after.json
sudo python3 scripts/backup-restore-host.py --sales-source /srv/decom-sqlserver/validation/source-sales-v2-after
sudo python3 scripts/verify-cloudbeaver-host.py --sales
sudo python3 scripts/collect-sales-evidence-host.py
```

Use a fresh baseline directory on subsequent runs. The live probe reserves ID `2000000100` in Orders, OrderLines, Invoices and InvoiceLines; it refuses occupied source keys or existing warehouse history. Choose a new explicit probe key before repeating it. It clones isolated fixture rows without advancing sequences, checks committed create/update/delete states, changes a NULL invoice/order relationship to a real link and checks seven-digit computed delivery times. Only synthetic probe rows are removed; their CDC history stays in the vault. The probe sets the session options required by SQL Server indexes; it does not change database-wide settings.

The recovery check stops the loader, recreates PostgreSQL and requires unchanged state. Full archive replay must add zero events and change no vault counts. The backup check creates a protected `pg_dump`, restores it into temporary `warehouse_restore`, repeats all five business reconciliations and removes that temporary database. Existing cluster roles are reused; this does not test a full cluster/secret restore on another host.

The baseline SQL-object export is `/srv/decom-sqlserver/validation/source-inventory-20261028.json`. Detailed operational reports are under the same protected `validation` directory. Source projection files can contain business data and are not copied into the repository.

For controlled replay, stop the watch worker first:

```bash
sudo docker compose stop loader
sudo docker compose run --rm --no-deps backfill python load.py --replay
sudo docker compose up -d --wait loader
```

On failure, the ledger does not advance for the failed batch. The worker logs the manifest key, exception type and safe validation reason, records an error in `ops.consumer_status` when PostgreSQL is reachable and exits. Docker restarts it. A persistent invalid batch therefore remains blocking; investigate it rather than skipping or deleting it. The health check requires a recent successful scan and no current error. External alerting is not yet installed.

`source-export.py inventory` exports object definitions, table columns, declared SQL dependencies, foreign keys, sequences, synonyms and SQL Agent job steps to a protected host file. It does not discover every dynamic query, external application caller or runtime dependency. The first extraction procedure has an explicit, tested PostgreSQL implementation; no source procedure has been removed or automatically translated.

`source-sales-export.py` exports the full primary-key sets and explicit business projections to protected NDJSON files, with file hashes and a completion manifest published only after the SQL transaction commits. A SQL Server SERIALIZABLE transaction spans all five scans and can block writes. `reconcile-sales.py`, invoked by `reconcile-host.py --sales`, merges the ordered source stream with a PostgreSQL server-side cursor in a read-only REPEATABLE READ transaction. It checks every projected value, six parent references, detail-view grains and invoice totals. JSON money is decoded as Decimal. These are quiet-fixture checks, not a general online cutover algorithm. Spatial and audit columns remain outside the business projection.

## Upgrade from the two-table pilot

1. Keep the old image, a protected copy of its source and the v1 validation reports. Build image `decom-warehouse-loader:2` and run the isolated integration tests before switching the worker.
2. Create the five-table source baseline. Stop only the warehouse worker and run `scripts/cutover-sales-host.py` once to create a pre-v2 logical backup plus fingerprints of the old batch ledger and event identities. Its recovery directory is `/srv/decom-sqlserver/releases/warehouse-sales-v2-20261028`; it refuses to overwrite an existing recovery point.
3. Run `scripts/migrate-host.py`. Migration 002 copies v1 acknowledgments into the scoped ledger and adds the model and `sales/v2` contract. Migration 001 is never edited.
4. Run `docker compose run --rm --no-deps backfill`. The new scope revalidates all five table prefixes and `_mixed`. Old events deduplicate; the 542 previously ignored records in old mixed batches are now eligible. The original receipt bytes and v1 ledger rows stay unchanged.
5. Reconcile the baseline with `reconcile-host.py --sales`, then start the watch worker and run the live/recovery/restore checks. Publish the separate five-table evidence only after all checks pass.

For an incident, stop the warehouse worker and preserve capture/archive state. A full rollback restores the protected pre-v2 dump into a **separate** database and verifies it with the retained v1 image/source before any deliberate consumer switch. Do not drop the expanded warehouse or reset Debezium offsets. Archived post-backup events remain available for replay. Simply running v1 against the expanded model would leave the three added tables stale and is not a complete rollback.

Keep PostgreSQL data, secrets, archive data, connector state and migration definitions recoverable together. A container restart test does not establish disaster recovery. Backups retained on the same VM do not protect against loss of that VM or data disk. No off-VM restore, high availability, purge watermark or full application cutover has been established.

Migration 005 adds stable `delivery.invoice_details_v1`, `delivery.order_details_v1` and `delivery.ingestion_context` views for the [reusable full-export service](../export/README.md). Its dedicated PostgreSQL identity receives only these interfaces. Export jobs and S3 consumers have separate secrets and do not change the warehouse loader.

**Automatic SQL CDC cleanup remains disabled.** This consumer covers the five captured business tables, not the whole database. Neither its scoped batch ledger nor a maximum observed LSN authorizes business purge. The separate [manual technical purge pilot](../purge/README.md) is limited to the archive-only `lab.CdcProbe` fixture.
