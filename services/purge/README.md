# CDC purge: readiness audit and manual technical pilot

**The manual technical purge pilot is complete; business cleanup remains blocked.** The [pilot report](../../docs/validation/cdc-purge-pilot-v1-2026-10-29.json) records the actual cleanup and fresh capture afterward. The [v3 audit](../../docs/validation/cdc-purge-audit-v3-2026-10-29.json), [local resume exercise](../../docs/validation/cdc-local-resume-v1-2026-10-29.json) and earlier [local-only report](../../docs/validation/cdc-purge-v3-local-2026-10-29.json) remain historical evidence of the pre-purge state.

## Manual technical pilot

At the user's request to keep this a POC, `scripts/pilot-host.py` implements one fixed scope: `WideWorldImporters`, `lab.CdcProbe`, capture instance `lab_CdcProbe`. It accepts at most 200 old CDC images in complete known insert/update/delete test cycles, with a 15-minute minimum age. It cannot select a business table. The native cleanup job stays disabled with its original 4,320-minute setting. No new service, account or Airflow schedule is added.

The runner emits a fresh anchor, verifies its archive, stops Debezium gracefully, fsyncs/copies its existing checkpoint files, and binds the persisted checkpoint to the anchor's delete using retained source command IDs. The cleanup boundary must be strictly earlier than that checkpoint transaction. All retained post-bootstrap probe images are compared with checksum-verified raw batches. All technical source images are also copied to a separate `control/purge-pilot/v1/<run>/source-images.json` object and read back byte for byte. This copy includes pre-bootstrap history and is explicitly SQL-image evidence, not synthesized Debezium events.

Only after those checks, a fresh source comparison and SQL-side target/count/hash/age guards does `--execute` call `sys.sp_cdc_cleanup_change_table`. The 200-image cap is checked against the full candidate count; native `@threshold` is only a per-delete-statement batch size. The runner reconciles the same source interval afterward, verifies unchanged business captures, rechecks the archive/evidence copy, restarts Debezium with its preserved files, and verifies a new probe and advanced persisted checkpoint.

Run on the provisioned VM during a quiet lab maintenance window:

```bash
cd /srv/decom-sqlserver/workspace/services/purge
sudo python3 scripts/pilot-host.py           # plan and evidence only
sudo python3 scripts/pilot-host.py --execute # new plan, then bounded cleanup
sudo cat /srv/decom-sqlserver/purge/pilot-v1-latest.json
```

Both modes emit a technical probe and briefly pause/restart Debezium. Each invocation creates a fresh run; it refuses empty, recent, oversized or incomplete candidates. Use one invocation for the desired mode. Protected source observations, checkpoint copies, command logs and execution intent live in `/srv/decom-sqlserver/purge/pilots/<run>/`. A failed native call is never retried automatically: inspect its actual watermark and remaining images, because the watermark can advance before deletion finishes. Code rollback cannot restore deleted native CDC history; retain the evidence and current checkpoint, and do not insert directly into CDC system tables or rewind offsets.

The [2026-10-29 acceptance report](../../docs/validation/cdc-purge-pilot-v1-2026-10-29.json), run `20261029T163640Z-0f62f8ad3e8e`, records **48 images removed across 12 old probe cycles**. Before cleanup, 40 post-bootstrap images (36 candidates plus four fresh anchor images) matched their archives; 12 older images were preserved in the separate SQL-image copy. That copy contains all 52 pre-cleanup technical images. Seventeen scoped manifests were verified before and after cleanup. Four anchor images survived cleanup and four new post-purge images were captured, leaving eight technical CDC images and zero live probe rows. The five business capture histories/metadata stayed identical, native cleanup returned success, the persisted checkpoint advanced, all 88 audit/pilot tests passed locally and on the VM, and all 13 permanent services were healthy.

This POC demonstrates graceful local resume and a readable same-VM evidence copy. It does not certify crash durability, off-VM restoration, a general transaction fence or business retention/consumer approval. The old v3 historical comparison and resume proof are not automatically reusable after intentional history removal; the pilot report accounts for this transition. No production readiness gate has been changed to green. See the [scope decision](../../docs/decisions/012-cdc-technical-purge-pilot.json).

## Read-only readiness audit

This is the first operational step of the purge workstream: an **on-demand, read-only audit** of retained SQL Server CDC changes, SeaweedFS archives and PostgreSQL consumer checkpoints. It does not delete CDC rows, business rows, exports or archives. It cannot authorize cleanup, disable CDC or decommission an application.

SQL Server cleanup removes change-table entries below a low watermark. That watermark can advance even if subsequent physical deletion fails, and cleanup affects every consumer relying on those changes. A maximum archived LSN, successful export or RabbitMQ ACK therefore cannot be used as deletion permission. The current native cleanup job remains disabled.

## What is checked

1. Observe an upper source LSN, the six configured capture instances and their captured-column metadata, then enumerate retained CDC identities and all captured values up to that observation boundary. No query silently truncates: more than 100,000 rows per instance aborts this observer.
2. Read every manifest and compressed payload in `s3://cdc-raw/raw/wwi/bootstrap-v2/`, checking both checksums, envelope, record counts and table/date layout.
3. Compare post-bootstrap retained CDC identities and multiplicities with archived streaming events. An update corresponds to two source images: before and after. Identical redelivery is deduplicated by event identity; conflicting content is an error. Numeric LSN gaps are not evidence of loss.
   Compare every captured column of those images using its SQL type. Preserve exact decimals, integers, seven timestamp fractional digits, strings and binary bytes; a missing field is not SQL NULL. Column-set changes or unsupported types block the comparison. Per-table counts distinguish matched images from tables with no retained changes in this interval.
4. Check every manifest containing warehouse-scope records against the `sales/v2` batch acknowledgement, including the stored manifest hash and selected/ignored record counts. Compare the archived business streaming events' semantic hashes with the warehouse event ledger.
5. Re-read the same bounded source observation, confirm capture metadata, column metadata, identities and values stayed stable, and confirm native cleanup remains disabled.

The five business tables require warehouse acknowledgements. `lab.CdcProbe` is an archive-only technical fixture and is explicitly excluded from warehouse row processing. Probe-only batches are verified in the archive and are not required in the warehouse ledger. Mixed batches containing business rows require the warehouse's acknowledgement, including explicit ignored-record counts.

Source identity reconciliation covers table, primary key, operation, commit/change LSN and multiplicity. The separate `captured_column_values` result reports strict captured-value equality in the retained post-bootstrap interval, with canonical value hashes and mismatching column names. It does not certify transaction completeness. Archive-to-warehouse streaming comparison also uses the Debezium event serial number and the complete before/after semantic hash. Initial snapshot payloads are checksummed and their captured-column sets checked; snapshot values are not fully compared with the source by this audit.

SQL CDC can represent unavailable values as NULL, including some large-object before images and computed columns. This audit compares those captured representations; it cannot recover original base-table values that CDC did not retain. No NULL is replaced with a current source value. Temporal strings follow the existing Debezium `isostring` configuration; SQL's timezone-free dates/timestamps and the connector's corresponding `Z` representation are compared without truncating 100-nanosecond precision. Geography/geometry use SQL Server native serialization, matching the connector's bytes representation, not WKB geometry conversion. Unknown types require an explicitly reviewed mapping before the audit can continue.

Debezium 3.7 can replace a NULL in an unchanged large-object column with `__debezium_unavailable_value` for an update's before **or after** image. The audit retains `__$update_mask` and diagnoses a matching connector rule only when the SQL type/length, captured ordinal, operation, clear update-mask bit and exact marker all agree. This is a separate explanation: **a placeholder is never converted to NULL or counted as an exact value match**. `connector_representation_explains_all_differences=true` does not satisfy `FULL_SOURCE_ROW_IMAGES_RECONCILED`. Marker handling and historical reconstruction need a separate, versioned consumer contract; rewriting raw archives, substituting current source values or ignoring these fields would lose evidence.

Pre-bootstrap retained changes are reported separately. An initial snapshot reconstructs state at bootstrap; it does not reconstruct an earlier insert/update/delete history. These older changes need an explicit recovery/consumer decision before any cleanup proposal can include them.

## Decision and missing evidence

Every report contains `mode=audit-only`, `decision.purge_authorized=false`, `decision.executable_low_watermark=null` and `decision.changes_deleted=0`. A completed audit with `status=blocked` is expected while proof is missing. Unknown checks are `null`, not silently accepted.

The initial retention margin is 72 hours, matching the lab's original 4,320-minute cleanup setting. It is a project default, not a production retention or legal decision. The report's retention observation LSN is informational and must never be passed to a cleanup procedure. No executable boundary is generated, including when all implemented checks pass.

Remaining gates include crash-safe Debezium checkpoint durability, a complete transaction fence, approval of every CDC consumer and retention policy, treatment of pre-bootstrap history, and tested archive/checkpoint recovery outside the VM. New unresolved values also block future audits. Existing full-export receipts are not contiguous CDC consumer checkpoints. A successful connector-representation diagnosis alone does not close strict value reconciliation or these independent gates.

## v3 reconstruction and resume evidence

The v3 contract retains `captured_column_values.matched=false` when raw placeholders differ from captured NULLs. The new `logical_column_values` result independently verifies reconstruction. Its gate is named `RETAINED_LOGICAL_ROW_IMAGES_RECONCILED`; the historical v2 `FULL_SOURCE_ROW_IMAGES_RECONCILED` gate is not silently reinterpreted.

The operator exports sparse reconstruction inputs, proofs and derived results in one PostgreSQL read-only repeatable-read transaction. The audit service receives this protected file without gaining additional database grants. During the full verified archive scan, it gathers every event for affected keys, rechecks current SQL CDC evidence, rebuilds values in source order and compares all resolved values, lineage and revision hashes. A missing origin, delete barrier, changed mask, unsupported marker, stale revision or different value blocks the gate. The operator compares a second source observation and reconstruction bundle before finalizing the report. Limits are 10,000 targets, 100,000 proof cells and 100,000 relevant history events in total; exceeding them aborts rather than truncates.

`checkpoint-input.py` reads stable samples of the real offset and schema-history files. The bounded parser accepts only the reviewed serialized byte-array map, one expected database partition and the pinned direct/function offset fields. It never deserializes executable Java objects. Unknown formats and incremental snapshot fields remain unsupported. Fixtures were emitted by Java's `ObjectOutputStream`; the actual Debezium 3.7.0.Final image and its `connect-runtime-4.3.1.jar` were also inspected on the VM, and its real offset file decoded successfully.

The live connector uses **direct mode**, while archived source metadata omits `command_id`. The validator obtains that field from unique retained SQL CDC images matched by table, primary key, operation and commit/change LSN. Both update images must agree. It orders command IDs before change LSNs, following the pinned connector implementation, and rejects missing/ambiguous matches or a mode change. This binding verifies the tested resume points; it does not turn the warehouse's existing ordering contract into a general transaction fence.

The separate `verify-checkpoint-host.py` operator command performs a **brief capture interruption**: verify a baseline technical probe, stop Debezium, fsync/copy its existing state, emit a probe while stopped, recreate only Debezium, verify the backlog and a third probe, and compare snapshot archive counts. It attempts to restore capture in a `finally` block if stopping or copying state fails. It never stops storage or resets offsets. Three isolated `lab.CdcProbe` insert/update/delete cycles leave twelve additional CDC row images and no surviving probe rows; business tables are untouched.

The normal audit reads the latest exercise and revalidates all three probe cycles and exact checkpoint coordinates in its own checksum-verified archive scan. It requires the same connector/source containers, image, configuration and schema history, stable current state, and an exercise at most 24 hours old. This is a conservative lab freshness policy; an advanced checkpoint during the audit blocks the observation instead of inventing a stable boundary.

`CONNECTOR_PERSISTENT_RESUME_VERIFIED` can pass after that live exercise. **`CONNECTOR_DURABLE_RESUME_CHECKPOINT_CERTIFIED` remains unknown:** graceful recreation does not establish safety under abrupt power loss, interrupted offset-file writes or VM loss. Transaction completeness and off-VM recovery remain separate gates. See [ADR 011](../../docs/decisions/011-purge-logical-values-and-resume.json).

## Deployment and execution

On the VM:

```bash
cd /srv/decom-sqlserver/workspace/services/purge
sudo bash scripts/install-host.sh
sudo python3 scripts/run-host.py
sudo cat /srv/decom-sqlserver/purge/latest.json
```

The installer adds read-only identities and builds the one-off image using the existing pinned Python/archive/warehouse libraries. On first provisioning, SeaweedFS is recreated to load the added S3 identity; its data and other identities are retained. Existing CDC capture and loading recover through their retained state. No permanent container or purge schedule is added.

For an existing provisioned deployment, preserve the current purge code and image first, update the files and build `decom-cdc-purge-audit:3` with `sudo env PURGE_RUN_DIR=/srv/decom-sqlserver/purge/build-placeholder docker compose build audit`, then run the host runner. This code-only update does not require reprovisioning accounts or recreating SeaweedFS. Keep the protected `.env`, secrets and existing data directories; exclude them from code transfer bundles.

For a new image or configuration, first inspect the deployed Debezium image and bundled Kafka library version, and confirm the state parser accepts its real files. Run an exercise when establishing or renewing the bounded resume evidence, during a lab maintenance window:

```bash
cd /srv/decom-sqlserver/workspace/services/purge
sudo python3 scripts/verify-checkpoint-host.py
sudo python3 scripts/run-host.py
sudo cat /srv/decom-sqlserver/validation/cdc-purge-audit-v3-latest.json
```

Resume exercise reports and stopped-state copies are protected under `/srv/decom-sqlserver/validation/cdc-resume-<run>/`. A hash-bound `cdc-resume-v1-latest.json` pointer advances only after a successful exercise. The full audit remains useful without that evidence: it records a blocked resume gate and still evaluates values, archive integrity and warehouse acknowledgements. Copy only sanitized final reports into `docs/validation`; never copy source inputs, reconstruction bundles or checkpoint files. Retain the preceding image/code for rollback. Reverting this audit requires no database migration or state restoration, and must not overwrite historical evidence.

The host collector uses a dedicated `cdc_auditor` SQL Server login. Its password travels through standard input to the existing SQL tools container; it is not in command arguments. The operator separately reads the cleanup job's control metadata using the existing administrative helper. That helper is not exposed to the audit container.

The audit container uses `warehouse_cdc_auditor` with a default read-only transaction and grants limited to batch metadata and event identity/hash columns. It cannot read warehouse business row contents. Its S3 identity can list `cdc-raw` and read only the active archive generation; it cannot write objects or read exports. The container has no source credentials, Docker socket or source-network membership. It is limited to 512 MiB and one CPU.

Each run writes protected input observations, policy, logs and a final report under `/srv/decom-sqlserver/purge/runs/<UTC timestamp>-<random suffix>/`. Reports contain counts, hashes, column names and control metadata, not business row values. **Input observations now contain complete captured business values** and remain restricted: the run directory is root-only, the container receives its observation read-only, and final reports and observations have mode 0400. Do not copy these inputs into the repository. `latest.json` points to the last completed report and its checksum; check its timestamp before using it. A failed run retains its diagnostics and does not replace this pointer.

The isolated tests cover missing events between unchanged endpoints, multiplicity, missing update images, snapshot/history separation, malformed LSNs, source JSON table-name handling, wide-capture SQL input, mixed/archive-only consumer scope, exact value comparisons and mandatory denial even when supplied checks are true. Value failure cases include one-cent corruption, a 100-nanosecond timestamp difference, a changed before image, missing/extra columns, 38-digit decimals, large integers, Unicode, native spatial bytes and unsupported types. Marker tests verify update-mask byte ordering, missing/invalid masks, changed columns and unrelated corruption; even an explained marker remains a failed exact comparison. The runner records the actual test count rather than a fixed expected number. Live checks verify read-only database permissions and denied S3 writes to an isolated probe key. A blocked readiness report is not a failed integrity test; inspect each named check separately.

No scheduled Airflow audit has been added yet. The observer remains on demand while the missing evidence and source contract are established. A business cleanup executor requires separate implementation and validation; adding a boolean to this report cannot enable deletion. The fixed technical pilot above is a separate operator command and contract.

The historical [v1 acceptance report](../../docs/validation/cdc-purge-audit-v1-2026-10-29.json) records the identity-only milestone. The value comparison is versioned separately as `cdc-purge-audit/v2`; it does not change the archive or warehouse data contracts.

The [v2 live report](../../docs/validation/cdc-purge-audit-v2-2026-10-29.json), run `20261029T022625Z-876480074c58`, records 3,033 verified manifests/payloads, 3,027 matching warehouse batch acknowledgements and 14 matching business streaming event hashes. All 42 retained post-bootstrap CDC identities match; 344 captured cells were compared. **38 images match exactly; four update images contain ten NULL-versus-placeholder differences** in `Sales.Orders` and `Sales.Invoices`. Every such difference is explained by the pinned connector rule and actual SQL update mask; no other value difference was found. Strict full-image reconciliation remains false. `Sales.Customers` has no retained CDC images in this interval, so its 31 columns are shape-checked but not value-certified by this run. Twelve pre-bootstrap probe images remain separately unaccounted for. All 50 tests passed, source observations stayed stable, native cleanup stayed disabled, and all 13 permanent services were healthy.

The historical v2 auditor image is retained as `decom-cdc-purge-audit:validated-v2-20261029`; validation concerns the auditor and its detection behavior, not cleanup eligibility. The preceding code bundle is protected at `/srv/decom-sqlserver/purge/code-before-values-v2.tar.gz`, and the v1 image/report remain available.

The subsequent [`cdc-values/v1` reconstruction](../../docs/decisions/010-cdc-value-reconstruction.json) has [separate acceptance evidence](../../docs/validation/cdc-value-reconstruction-v1-2026-10-29.json). It reconstructs all ten marker cells from proven prior values, preserves raw archives and blocks unresolved current business reads/exports. The historical [post-deployment audit](../../docs/validation/cdc-purge-after-reconstruction-2026-10-29.json) reports 38/42 exact raw images, since reconstruction does not rewrite the raw payloads or equate a marker with NULL. Its 50 safety tests pass, all 3,033 manifests verify and native cleanup remains disabled. The v3 contract below now consumes and revalidates the derived proofs explicitly.

The [v3 live audit](../../docs/validation/cdc-purge-audit-v3-2026-10-29.json), run `20261029T160143Z-87331a9da67a`, verifies 3,036 manifests/payloads, 3,027 warehouse batch acknowledgements and 14 business streaming event hashes. All 54 retained post-bootstrap CDC identities match. Fifty images match strictly; the four marker-bearing images and all ten reconstructed cells are revalidated from six archive events across two affected keys. The three technical probe cycles added twelve retained CDC images; business rows were not changed. Source, reconstruction and connector observations remained stable, all 79 audit tests passed, and all 13 permanent services were healthy.

The [resume exercise](../../docs/validation/cdc-local-resume-v1-2026-10-29.json), run `20261029T153635Z-9119c3f1`, recreated Debezium with the same image, configuration, persistent mounts and schema history. All nine probe events were subsequently bound to their SQL CDC command IDs and verified archive positions by the audit. Snapshot records remained exactly 604,445. The namespace is `http`, the database partition is `wwi/WideWorldImporters`, and the image contains Kafka Connect runtime library 4.3.1; no Kafka service was deployed. This establishes the bounded persistent local resume check, not crash durability or transaction completeness.

The validated auditor is `decom-cdc-purge-audit:validated-v3-20261029`, image `sha256:136eb594a583a89240ef14b8833cd53bc88c292d6e4be0c27d0db8a22418b72b`. Prior and validated code, image identities and the original transfer bundle are retained under `/srv/decom-sqlserver/releases/purge-v3-20261029T152344Z/`. The predecessor image also has tag `before-v3-20261029`. All historical reports remain intact. Native cleanup remains disabled; twelve pre-bootstrap images, an aged retention candidate, crash durability, a transaction fence, consumer/retention approvals, off-VM recovery and a separately validated executor are still missing.

For rollback, stop any active audit container and retain its reports. The existing CDC/warehouse/export/messaging services are independent. Dedicated audit accounts may later be disabled after checking no audit is running. Do not reset Debezium offsets, delete raw evidence or enable native cleanup as a rollback shortcut.

See [the initial audit decision](../../docs/decisions/008-cdc-purge-audit.json) and [the captured-value decision](../../docs/decisions/009-cdc-captured-values.json). References: Microsoft [cleanup semantics and failure behavior](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-cleanup-change-table-transact-sql), [CDC image and NULL semantics](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql), and Debezium [SQL Server types, events and offsets](https://debezium.io/documentation/reference/3.7/connectors/sqlserver.html) and [the pinned 3.7.0 placeholder implementation](https://github.com/debezium/debezium/blob/v3.7.0.Final/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerChangeTablePointer.java).
