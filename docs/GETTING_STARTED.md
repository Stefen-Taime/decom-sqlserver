# Rebuild the migration lab

This is a source snapshot of a completed POC, not a newly certified clean-room deployment. The historical reports describe the original lab. Follow the component runbooks and establish new evidence for your own installation.

## 1. Provision an isolated host

The original host used Ubuntu AMD64, four vCPUs and 12 GiB RAM. SQL Server Developer is used only for development and testing. Install the Google Cloud CLI and Terraform on your workstation, then follow [infra/README.md](../infra/README.md). Set your own project and administrator in `infra/terraform.tfvars`; never commit that file or Terraform state.

Terraform creates billable GCP resources. Inspect the plan before applying it. It supplies infrastructure only; the data services run in Docker. Do not run all heavy validation jobs concurrently on this small host.

Transfer only committed source files after cloning this repository:

```bash
git archive --format=tar.gz --output=/tmp/decom-sqlserver-source.tgz HEAD
```

Use `gcloud compute scp` with your project, VM, zone and `--tunnel-through-iap`. Extract the archive under `/srv/decom-sqlserver/workspace` on the VM. Do not copy your local `.secrets`, Terraform state or generated environment files. Component installers generate credentials on the host.

## 2. Restore the source and capture its snapshot

Start with the [SQL Server runbook](../services/sqlserver/README.md). Its installer restores Microsoft's WideWorldImporters sample, configures five business CDC captures and a separate `lab.CdcProbe` fixture, and keeps native automatic cleanup disabled.

Next, follow the [CDC runbook](../services/cdc/README.md) to install SeaweedFS, the signed HTTP archive receiver and Debezium Server. There is no Kafka broker or Connect service. Wait for the initial snapshot and verify archive coverage before loading the warehouse.

Archives contain separate compressed event payloads and control receipts. Start with receipts and verify the referenced bytes; do not treat every JSON object as a business row. The archive layout and partition timestamp semantics are defined in [decision 001](decisions/001-cdc-archive-layout.json).

## 3. Establish a new warehouse identity before migrations

A fresh SQL Server restore has its own initial snapshot position. The checked-in warehouse and purge contracts identify the original validated generation. Before applying migrations to an empty PostgreSQL target, inspect verified snapshot events, confirm the initial `commit_lsn`, and align your own source identity, snapshot generation and position in these files:

| File | Contract value |
| --- | --- |
| `services/warehouse/compose.yaml` | `SNAPSHOT_COMMIT_LSN` |
| `services/warehouse/loader/sales_contract.py` | Generation, initial position and source identity |
| `services/warehouse/sql/002-five-sales-tables.sql` | Initial `ops.consumer_contracts` record |
| `services/purge/compose.yaml` | `SNAPSHOT_COMMIT_LSN` |
| `services/purge/policy.json` | Initial position and source identity |

This adjustment is for a target that has never applied the migrations. On an existing deployment, add a new migration and define the contract transition. Do not alter applied migration checksums or reset connector state to suppress a mismatch.

Follow the [warehouse runbook](../services/warehouse/README.md) to install PostgreSQL, apply migrations, backfill archives, run the loader and reconcile the declared business projections. Install [CloudBeaver](../services/cloudbeaver/README.md) for read-only inspection if needed. Host scripts under `/srv/decom-sqlserver/workspace` run on the VM, not on the Mac.

## 4. Inventory and port the SQL contract

CDC does not translate SQL objects. Use the warehouse inventory and procedure runbooks to collect definitions and dependencies, load the versioned `Warehouse.PackageTypes` reference and compare `Integration.GetOrderUpdates` with `app.get_order_updates(text,text,text)`.

Some demonstration helpers expect an inventory directory named `sql-objects-20261028`. Preserve that path for a new lab walkthrough, or update the dependent helper paths together. The inventory refuses to overwrite an existing observation. Choose an actual returned reference snapshot version when calling the migrated function.

The [42-procedure catalog](migration/sql-object-catalog-2026-10-28.json) is a starting classification from the sample database. It does not discover every application's dynamic SQL, external jobs or ownership decisions.

## 5. Deliver validated data

Install [exports](../services/export/README.md), then [RabbitMQ and the invoice application](../services/rabbitmq/README.md), and finally [Airflow](../services/airflow/README.md). Validate each component before enabling schedules or starting the continuous workers described in its runbook.

Consumers discover publication records written after file verification. Each delivery is a full snapshot; consumers replace their current model rather than append successive snapshots. Only the invoice application uses RabbitMQ. Other applications can read PostgreSQL or published SeaweedFS exports directly.

## 6. Audit before any cleanup exercise

The [purge runbook](../services/purge/README.md) separates a read-only audit from the manual `lab.CdcProbe` cleanup pilot. The audit always returns `purge_authorized=false`. A successful upload, maximum LSN or consumer receipt alone does not establish cleanup safety.

The pilot runner binds an exact Docker image name and ID through `IMAGE` and `IMAGE_ID` in `services/purge/scripts/pilot-host.py`. For a new build, run the documented tests and audit, review the result, and bind your own validated image. Do not disable the image check or assume a changed tag is sufficient. The pilot's fifteen-minute minimum age and 200-image bound apply to technical test data only.

The closure runner in `scripts/close-poc-host.py` deliberately stops SQL Server and Debezium and creates new deliveries. Run it only in a planned isolated test window after satisfying the component prerequisites. It is not an installation step or an authorization for permanent retirement.

## Access and recovery boundaries

Use `bash infra/scripts/ui-tunnel.sh` from the initialized Terraform checkout, or supply `LAB_PROJECT`, `LAB_VM` and `LAB_ZONE`. Interfaces bind to local loopback and are forwarded through IAP. Read generated credentials from the protected host files documented by each service.

The completed POC proved graceful local recovery and downstream use of already migrated data during a source outage. Power-loss durability, off-VM restore, production cutover and business retention policies require separate work. Persistent volumes on one VM are not an independent backup.
