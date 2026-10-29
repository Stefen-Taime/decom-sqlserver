# Airflow export orchestration

Apache Airflow **3.3.2**, the latest stable release verified at deployment, runs on the existing VM. The official Python 3.12 image is pinned by digest and dependencies use the release's constraints. There are three permanent containers: API/UI, scheduler with LocalExecutor, and DAG processor. No Celery broker, Redis or additional RabbitMQ consumer is introduced.

## Workflows and schedules

| DAG | Initial schedule | Steps |
| --- | --- | --- |
| `export_invoices` | Daily 06:00 America/Toronto | Check ingestion → full invoice export → independent S3 validation → durable invoice-app receipt |
| `export_orders` | Daily 06:15 America/Toronto | Check ingestion → full order export → independent S3 validation |

These are initial lab schedules. `catchup=False` prevents historical scheduled backfills; manual triggers remain available. Airflow's global parallelism and the `export_jobs` pool each allow one task at a time, and each DAG allows one active run. The two workflows can overlap in time, but their tasks execute serially.

CDC capture, warehouse loading, notification dispatch and invoice consumption remain continuous services. Airflow checks readiness and coordinates exports; it does not restart those services between tasks. Healthy services and a warehouse scan within 180 seconds do **not** certify that every source transaction has reached the warehouse. Each export has its own consistent PostgreSQL snapshot; the two datasets do not share one cross-dataset snapshot.

Invoices are complete only after the application has committed the matching publication, manifest hash, row hash and row count to its delivery ledger. A broker ACK or empty queue alone is insufficient. Orders use the existing read-only S3 consumer without RabbitMQ. This validator demonstrates that the export can be consumed; it is not proof that every future business application has consumed it.

## Task execution and retries

Airflow submits strict JSON requests over SSH using a dedicated key and pinned host key. The OS account can invoke only a root-owned forced command. That command allows the two datasets' existing exporter/validator jobs, dependency checks and invoice receipt checks. It accepts no shell command, SQL, image name or user-provided path. The scheduler has no Docker socket, warehouse business credentials, S3 keys or AMQP credentials.

The runner launches resource-limited one-off Docker containers and journals the request before starting. Detached jobs survive task or SSH interruption. Airflow polls their status; the runner records completion durably before removing its exited container. Only one managed job may be active at a time.

A request ID is derived from the dataset and Airflow DAG-run ID. It does not include the task attempt. Retrying or clearing a task in the same run reuses its completed export/validation result. A new DAG run deliberately creates a new full snapshot. The existing export request journal handles failures before or after publication.

Tasks allow two retries with exponential delay starting at 30 seconds. A failed task can leave a detached job running; inspect and resume with the same identity. Do not delete journals, reset offsets or create a new run merely to resume an uncertain result. Protected runner logs retain the one-off job's full output; Airflow task logs and XCom contain control metadata only.

The manual-run parameter `fail_after_export_once=true` intentionally fails the first export task attempt **after** publication. It is false by default and exists for the lab's retry acceptance test. The successful retry must return the original publication with one exporter attempt.

## Deployment and access

On the VM, after the warehouse, export framework and invoice messaging services are installed:

```bash
cd /srv/decom-sqlserver/workspace/services/airflow
sudo bash scripts/install-host.sh
sudo docker compose ps
sudo python3 scripts/api.py /api/v2/dags
```

The installer creates a dedicated `airflow_meta` database/role in the existing PostgreSQL cluster, generates retained file secrets, installs the forced-command SSH account, runs Airflow/FAB migrations, creates the administrator and one-slot pools, and starts the services. Newly discovered DAGs remain paused until acceptance checks enable them. Reinstallation retains secrets and metadata.

On the Mac, `bash infra/scripts/ui-tunnel.sh` exposes the UI at http://localhost:8080/. Leave the tunnel terminal open; an already running tunnel must be closed before starting another with the same local ports. The local ignored `.secrets/airflow-access.txt` contains the generated administrator login. The VM publishes port 8080 on loopback only. FAB protects the UI and API; the operator API helper reads its credentials from a root-protected file and never prints its JWT.

| Persistent location under `/srv/decom-sqlserver` | Purpose |
| --- | --- |
| `secrets/airflow/` | Metadata password, Fernet/API/JWT secrets, administrator credential, restricted SSH key and pinned host key |
| Existing PostgreSQL data volume | Dedicated `airflow_meta` database, Airflow/FAB migration ledgers, run and task history |
| `airflow/logs/` | Persistent Airflow task and component logs |
| `airflow/runner/requests/` | Durable job identity and result journal |
| `airflow/runner/logs/` | Protected exporter/validator diagnostic output |
| `export/requests/`, `export/consumer/` | Existing export recovery journal and S3 validation acknowledgements |

The API is limited to 1 GiB/1 CPU, scheduler to 2 GiB/1.5 CPU, and DAG processor to 768 MiB/0.75 CPU. Runtime SQL pools have two connections and no overflow; the role permits at most 16 connections. Schema initialization needs a larger five-connection pool. These are lab limits, not a throughput or availability guarantee.

## Operations and rollback

Use the UI to inspect failed runs, task attempts and logs. First resolve the dependency/data problem, then retry or clear the failed task in the **same** run. Compare its request ID, publication and application receipt before declaring completion. A terminal error stays visible in Airflow; email/chat notifications and escalation routing are not configured.

To suspend scheduling, pause both DAGs. Let active jobs finish and inspect `docker ps --filter label=decom.airflow.managed=true` before stopping Airflow. Stopping the scheduler does not cancel a detached exporter. Stop only the Airflow Compose project to remove orchestration; the CDC, warehouse, exports and messaging services remain independent. Retain metadata, secrets, logs and all request journals. Never use `down --volumes` as a recovery step.

Before upgrading, back up `airflow_meta` together with secrets and runner journals and validate restoration in isolation. A metadata migration may prevent simply switching to an older image; restore a compatible backup or follow the version's migration guidance. Same-VM persistence checks do not establish off-VM disaster recovery. Log/export retention, external monitoring, TLS inside the VM, credential rotation and HA remain future operational work.

**Automatic CDC cleanup remains disabled.** The separate [manual purge pilot](../purge/README.md) is limited to `lab.CdcProbe`. Neither an Airflow success state nor an application's full-export receipt authorizes business purge or SQL Server decommissioning. Business cleanup still requires reconciled CDC coverage, consumer checkpoints, a retention margin and recovery evidence.

## Acceptance evidence

On the VM, `sudo python3 scripts/verify-host.py` resumes its retained acceptance run IDs. It runs twelve boundary/recovery identity tests, checks real SSH restrictions and denied warehouse access, executes both full DAGs, verifies a post-publication Airflow retry with one export attempt, and compares the invoice application's durable receipt. The acceptance runs validated 228,265 invoice lines and 231,412 order lines, with five Parquet files each.

All three Airflow components were recreated successfully. Both completed run states and eight task-attempt logs survived. Thirteen permanent services were healthy, warehouse CDC counts were unchanged and source cleanup remained disabled. See [the recorded acceptance evidence](../../docs/validation/airflow-exports-v1-2026-10-29.json). The interrupted Docker create/start boundary is covered by isolated tests; it was not simulated with a host power loss.

See [the architecture decision](../../docs/decisions/007-airflow-export-orchestration.json). References: Apache [release directory](https://downloads.apache.org/airflow/), [supported platforms](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html), [LocalExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html), [configuration reference](https://airflow.apache.org/docs/apache-airflow/3.3.2/configurations-ref.html) and [FAB API authentication](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/token.html). The schedules, forced runner and delivery conditions are project decisions.
