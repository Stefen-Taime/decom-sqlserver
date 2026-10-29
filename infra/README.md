# SQL Server migration lab infrastructure

Terraform provisions one GCP VM for the Docker-based lab. Application services and the SQL Server database are deployed separately.

## Default configuration

| Setting | Value |
| --- | --- |
| Location | Montréal, `northamerica-northeast1-b` |
| Machine | `e2-custom-4-12288`: 4 vCPU, 12 GiB RAM |
| OS | Ubuntu 24.04 LTS, AMD64 |
| Boot disk | 30 GiB, balanced persistent disk |
| Data disk | 100 GiB, balanced persistent disk |
| Runtime | Docker Engine and Docker Compose plugin |
| Access | SSH through IAP with OS Login |
| Data location | `/srv/decom-sqlserver` |

The VM has an ephemeral external IP for outbound package and image downloads. The dedicated VPC permits inbound TCP/22 only from IAP (`35.235.240.0/20`). SQL Server, PostgreSQL, Airflow and other application ports have no public ingress rules. No GCP service account is attached to the VM.

Set your project and administrator in the ignored `terraform.tfvars` file. After deployment, retrieve the connection command from Terraform:

```bash
terraform output -raw ssh_command
```

Verified on 2026-10-28: SSH through IAP, completed bootstrap, Docker Engine 29.9.2, Docker Compose 5.6.1, persistent mounts for Docker/containerd, and a successful `hello-world` container. A subsequent Terraform plan reported no changes. This is historical validation of the original lab. Application services are documented in [SQL Server source](../services/sqlserver/README.md), [CDC capture](../services/cdc/README.md) and [CloudBeaver / SeaweedFS UI access](../services/cloudbeaver/README.md).

The configured administrator receives OS Admin Login and SSH-only IAP access on this VM. The administrator also needs project discovery permissions such as `compute.projects.get` for gcloud access; an existing project owner already has these. Provisioning requires permissions to enable APIs, create Compute Engine resources and set the instance/IAP IAM policies. The project must already have billing enabled.

## Deploy

For a new checkout, from this directory, using Terraform >= 1.5 and the Google Cloud CLI:

```bash
gcloud auth login
gcloud auth application-default login
cp terraform.tfvars.example terraform.tfvars
```

Set `project_id` and `admin_member` in `terraform.tfvars`. Review the location, machine size and disk size before provisioning.

For the existing deployment, retain the current `terraform.tfvars` and Terraform state; do not replace them with the example. Continue with the commands below.

```bash
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out=lab.tfplan
terraform apply lab.tfplan
terraform output -raw ssh_command
```

Run the printed SSH command. Bootstrap runs asynchronously after VM creation. Check it on the VM:

```bash
sudo journalctl -u google-startup-scripts.service --no-pager -n 80
sudo test -f /var/lib/decom-sqlserver/bootstrap-complete
sudo docker info
sudo docker compose version
findmnt --mountpoint /srv/decom-sqlserver
findmnt --mountpoint /var/lib/docker
findmnt --mountpoint /var/lib/containerd
free -h
df -h /srv/decom-sqlserver
```

For a bootstrap retry after fixing a failure:

```bash
sudo google_metadata_script_runner startup
```

Docker uses `sudo`; the bootstrap does not grant unrestricted Docker group access to every login. Place project files under `/srv/decom-sqlserver/workspace`. Docker and containerd storage are bind-mounted from the data disk. No SQL Server, Kafka, Spark or other lab workloads start during this deployment.

## Stop, restart and resize

```bash
terraform output -raw stop_command
terraform output -raw start_command
```

Run the printed command when needed. Stopping the VM stops compute usage; persistent disks continue to incur storage charges. External IPv4 addresses incur charges while assigned. The ephemeral IP may change after a restart, while IAP commands keep working by instance name. Stop application writers and complete required checkpoints before shutting down a running CDC pipeline.

To move to 16 GiB, set `machine_type = "e2-standard-4"`, then review and apply a new plan. Terraform can stop the VM to resize it. Large-volume capacity must be measured; these defaults are a development starting point.

## State and data lifecycle

Terraform state is local in this directory. Keep `terraform.tfstate` and backups private and backed up: they are needed to manage these resources. Local state, variable files and saved plans are excluded by the root `.gitignore`. Commit `.terraform.lock.hcl` once `terraform init` creates it.

The separate data disk is not automatically deleted when the VM is replaced. `prevent_destroy = true` also blocks Terraform plans that would destroy it. To intentionally remove the entire lab, back up any required data, explicitly remove that guard from `google_compute_disk.data`, and review `terraform plan -destroy` before destroying resources. A stopped VM and retained disk still consume storage budget.

The startup script only formats a blank data disk with no filesystem or partition-table signature; it refuses to overwrite an existing filesystem of another type. A missing data mount prevents Docker and containerd from starting. Existing data is reused after a VM replacement, but the disk is not a backup.

## References

- [GCP machine types](https://docs.cloud.google.com/compute/docs/general-purpose-machines)
- [SSH through IAP](https://docs.cloud.google.com/iap/docs/using-tcp-forwarding)
- [OS Login](https://docs.cloud.google.com/compute/docs/oslogin/set-up-oslogin)
- [Docker Engine installation](https://docs.docker.com/engine/install/ubuntu/)
- [Terraform Google provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
