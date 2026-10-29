# CloudBeaver and SeaweedFS web access

CloudBeaver Community 26.2.3 runs on the lab VM with a 1.5 GiB memory limit. Its workspace and internal H2 database persist under `/srv/decom-sqlserver/cloudbeaver`. No extra database service is required. Anonymous access is disabled.

The prepared connection, **WideWorldImporters — SQL Server (read only)**, uses the dedicated `lab_reader` SQL login. It can browse data and object definitions, including stored procedures. It cannot modify business tables or execute business procedures. WWI's territory-role memberships expose the full customer dataset without disabling its row-security policy.

The warehouse deployment also adds **Warehouse — PostgreSQL (read only)**, using `warehouse_reader`. Browse `warehouse → Schemas → mart` for current Customers and Orders, `dv` for the hubs, links and history, and `ops` for load summaries. The database role has SELECT privileges and no data-modification privileges. The PostgreSQL connection uses the internal Docker network; it does not require another browser port or tunnel.

## Open the interfaces from your Mac

From the project root, keep this command running in a terminal:

```bash
bash infra/scripts/ui-tunnel.sh
```

| Interface | Local URL |
|---|---|
| CloudBeaver | http://localhost:8978/ |
| RabbitMQ management | http://localhost:15672/ |
| SeaweedFS downstream exports | http://localhost:8888/buckets/warehouse-exports/ |
| SeaweedFS archive data | http://localhost:8888/buckets/cdc-raw/raw/wwi/bootstrap-v2/data/ |
| SeaweedFS control manifests | http://localhost:8888/buckets/cdc-raw/raw/wwi/bootstrap-v2/manifests/ |
| SeaweedFS master/storage view | http://localhost:9333/ |

CloudBeaver username: **stefen**. The generated password is in the ignored local `.secrets/cloudbeaver-access.txt` file, with owner-only permissions. The VM copy is `/srv/decom-sqlserver/secrets/cloudbeaver/access.txt`. Passwords are not embedded in Compose or documentation. Open the prepared database connection after signing in. If procedures are hidden, switch the navigator to **Advanced view**, then expand `WideWorldImporters → Schemas → <schema> → Procedures`.

Under `data/`, browse `table=Sales.Customers → batch_date=2026-10-28` to find compressed event files. Control receipts are in the corresponding partition under `manifests/`. The old `bootstrap-v1` generation remains a rollback copy; new capture writes only to v2. See [the archive reader contract](../cdc/README.md#archive-layout-and-reader-contract).

The Filer and master UIs are administrative interfaces protected here by SSH/IAP and loopback binding; the Filer can also upload or delete files. They do not use the S3 username/password. Do not expose their ports publicly. All three interfaces share the same SSH tunnel; closing it ends local access, without stopping services on the VM.

## Deployment and verification

Copy `services/cloudbeaver` plus `services/sqlserver/sql/06-lab-reader-roles.sql` into the VM workspace, then:

```bash
sudo bash /srv/decom-sqlserver/workspace/services/cloudbeaver/scripts/install-host.sh
sudo python3 /srv/decom-sqlserver/workspace/services/cloudbeaver/scripts/verify-access.py
```

The installer preserves existing secrets and workspace settings. The verifier signs in through the CloudBeaver GraphQL API, opens the saved SQL connection, executes a read query and closes its test session. It prints no passwords. `--initialize-connection` is a one-time bootstrap helper for saving the dedicated SQL reader credentials through CloudBeaver's API.

After warehouse deployment, verify the PostgreSQL connection separately:

```bash
sudo python3 /srv/decom-sqlserver/workspace/services/warehouse/scripts/verify-cloudbeaver-host.py
```

To release memory while not browsing:

```bash
cd /srv/decom-sqlserver/workspace/services/cloudbeaver
sudo docker compose stop
# Resume later:
sudo docker compose up -d --wait
```

Reference: [CloudBeaver server configuration](https://dbeaver.com/docs/cloudbeaver/Server-configuration/).
