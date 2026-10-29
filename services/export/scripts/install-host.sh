#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mountpoint -q /srv/decom-sqlserver
(cd ../warehouse && python3 scripts/migrate-host.py)
python3 scripts/prepare-host.py
docker compose config --quiet
docker compose build exporter
if [[ -f .reload-seaweed ]]; then
  cdc=/srv/decom-sqlserver/workspace/services/cdc
  # Existing combined weed-server deployment: use its already-tested restart procedure.
  docker compose --project-directory "$cdc" stop debezium
  trap 'docker compose --project-directory "$cdc" up -d debezium' EXIT
  docker compose --project-directory "$cdc" restart seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 180 seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 240 debezium
  rm .reload-seaweed
  trap - EXIT
fi
docker compose --profile tools run --rm storage-init
printf 'fs.configure -locationPrefix=/buckets/warehouse-exports/ -fsync=true -volumeGrowthCount=1 -apply\n' |
  docker exec -i decom-cdc-seaweedfs-1 weed shell -master=seaweedfs:9333 -filer=seaweedfs:8888
echo 'Export jobs ready. No permanent container or automatic schedule added.'
