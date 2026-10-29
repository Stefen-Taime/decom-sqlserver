# SQL Server source

First service of the migration lab: SQL Server 2022 Developer, WideWorldImporters Full and source-side CDC. Developer edition is for development and testing. SQL Server is the proprietary source system being migrated; the surrounding integration tools remain open source.

The original lab used `decom-sqlserver-lab` in `northamerica-northeast1-b`. Use your own GCP project. The restored sample contains 48 user tables, 42 stored procedures, 3 views and 2 SQL functions, excluding the CDC and `lab` schemas. Initial business counts: 663 customers, 73,595 orders, 231,412 order lines, 70,510 invoices and 228,265 invoice lines.

Verified on 2026-10-28 with SQL Server `16.0.4305.5`: healthy container, successful installer rerun without restoring over the existing database, data retained across container recreation, and CDC INSERT/UPDATE/DELETE captured again after restart. Docker stop completed in 0.74 seconds with exit code 0; the capture job resumed and the cleanup job remained disabled. Observed memory immediately after the final test was approximately 893 MiB against the 4 GiB ceiling; this is not a load-test result.

## Configuration

- Official Microsoft image: SQL Server 2022 CU27 GDR1 on Ubuntu 22.04, pinned by SHA-256 digest in `compose.yaml`.
- Container: 2 CPU quota, 4 GiB memory ceiling. SQL Server memory limit: 3072 MiB; `max server memory`: 2048 MiB; MAXDOP: 2. These are lab limits, not a production sizing recommendation.
- Non-root process, UID/GID 10001, matching the pinned image's `mssql` user and data-directory group. SQL Server Agent enabled.
- The secret-loading wrapper uses `exec` to make SQL Server PID 1 for graceful SIGTERM shutdown. Health checks wait for SQL Server Agent and for the existing sample database to finish recovery.
- Port 1433 is bound to `127.0.0.1` on the VM; use SSH forwarding from the Mac. Other containers on `decom-sqlserver-backend` can use `sqlserver:1433`.
- Persistent named volume: `decom-sqlserver-data`, backed by the VM's 100 GiB data disk.
- SA password: generated once on the VM, stored in `/srv/decom-sqlserver/secrets/sqlserver/sa_password` and mounted as a read-only Compose secret. The file is owned by UID 10001 with mode 0400, inside a root-only directory. It is not committed or embedded in Compose environment values. Root/Docker administrators can still access it.
- Backup: Microsoft's official `WideWorldImporters-Full.bak`, downloaded directly to the VM. The recorded SHA-256 detects subsequent local changes; it is not a publisher signature. `RESTORE VERIFYONLY` checks the backup before the initial restore. An existing database is never overwritten by the installer.

## Deploy from the Mac

From the repository root, set `LAB_PROJECT` to your GCP project and package only this service's source files:

```bash
export LAB_PROJECT="your-gcp-project-id"
COPYFILE_DISABLE=1 tar --no-xattrs -czf /tmp/decom-sqlserver-service.tgz \
  -C services/sqlserver compose.yaml bin sql scripts README.md

gcloud compute scp /tmp/decom-sqlserver-service.tgz \
  decom-sqlserver-lab:/tmp/decom-sqlserver-service.tgz \
  --project="$LAB_PROJECT" --zone=northamerica-northeast1-b --tunnel-through-iap

gcloud compute ssh decom-sqlserver-lab \
  --project="$LAB_PROJECT" --zone=northamerica-northeast1-b --tunnel-through-iap \
  --command='sudo install -d /srv/decom-sqlserver/workspace/services/sqlserver && sudo tar --no-same-owner -xzf /tmp/decom-sqlserver-service.tgz -C /srv/decom-sqlserver/workspace/services/sqlserver && sudo bash /srv/decom-sqlserver/workspace/services/sqlserver/scripts/install-host.sh'
```

The installer starts only SQL Server, restores the sample once, configures CDC and executes a real capture test. The password and data survive redeployments. Changing the secret file alone does not rotate an already initialized SA password.

## CDC scope and purge

CDC is enabled for `Sales.Customers`, `Sales.Orders`, `Sales.OrderLines`, `Sales.Invoices` and `Sales.InvoiceLines`. `lab.CdcProbe` is a separate technical test table, excluded from the planned business scope. The smoke test commits INSERT, UPDATE and DELETE operations there and asserts that all four CDC operation types arrive within 60 seconds. It leaves no business records changed.

The capture job runs under SQL Server Agent. The native cleanup job is disabled, including its schedule, because it does not wait for downstream archive and consumer checkpoints. The separate [purge service](../purge/README.md) provides an audit and a bounded technical pilot. Automatic cleanup remains disabled, so CDC history accumulates: keep test activity modest and monitor database/disk usage. No workload generator runs automatically.

This service validates capture into SQL Server CDC tables. The [CDC service](../cdc/README.md) adds Debezium Server and verified HTTP archival to SeaweedFS, without Kafka. The [PostgreSQL warehouse](../warehouse/README.md), [S3 exports](../export/README.md) and [invoice application](../rabbitmq/README.md) are deployed and validated. `cdc_reader` is used by the dedicated `debezium_cdc` login; SA is used for bootstrap and administration. [CloudBeaver](../cloudbeaver/README.md) provides browser access through a separate read-only SQL login.

## Operate on the VM

```bash
cd /srv/decom-sqlserver/workspace/services/sqlserver
sudo docker compose ps
sudo docker compose logs --tail=80 sqlserver
sudo docker compose exec -T sqlserver /bin/bash /opt/lab/bin/sqlcmd.sh \
  -i /opt/lab/sql/04-verify.sql
sudo docker compose exec -T sqlserver /bin/bash /opt/lab/bin/sqlcmd.sh \
  -t 120 -i /opt/lab/sql/05-smoke-cdc.sql
sudo docker compose stop sqlserver
sudo docker compose up -d --wait sqlserver
```

For a SQL client on the Mac, keep this tunnel open:

```bash
gcloud compute ssh decom-sqlserver-lab \
  --project="$LAB_PROJECT" --zone=northamerica-northeast1-b --tunnel-through-iap \
  -- -N -L 127.0.0.1:14330:127.0.0.1:1433
```

Connect to `127.0.0.1,14330`, database `WideWorldImporters`. The password stays in the protected file on the VM. For the initial lab certificate, clients must trust the server certificate; traffic from the Mac travels through the authenticated SSH tunnel. The bundled `sqlcmd.sh` helper already sets `-C` for local administrative queries.

`docker compose down` retains the named data volume. `docker compose down -v` deletes the database volume: do not use it for routine stops. The sample backup is retained, but it does not include later changes or instance-level jobs. Take proper backups before destructive experiments.

## References

- [Microsoft container configuration](https://learn.microsoft.com/en-us/sql/linux/containers/configure?view=sql-server-ver16)
- [SQL Server environment variables](https://learn.microsoft.com/en-us/sql/linux/configure/environment-variables?view=sql-server-ver16)
- [Restore WideWorldImporters in a container](https://learn.microsoft.com/en-us/sql/linux/migrate/tutorial-restore-backup-sql-server-container)
- [Debezium SQL Server connector](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
