#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mountpoint -q /srv/decom-sqlserver
python3 scripts/prepare-host.py
docker compose config --quiet
docker compose up -d --wait --wait-timeout 180 postgres
python3 scripts/migrate-host.py
docker compose build loader
if [[ -f .reload-seaweed ]]; then
  cdc=/srv/decom-sqlserver/workspace/services/cdc
  # Pause capture while the existing S3 service reloads the additional read-only identity.
  docker compose --project-directory "$cdc" stop debezium
  trap 'docker compose --project-directory "$cdc" up -d debezium' EXIT
  docker compose --project-directory "$cdc" restart seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 180 seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 240 debezium
  rm .reload-seaweed
  trap - EXIT
fi
echo 'Warehouse database and loader image ready. Validate and load the pilot before starting the watch service.'
