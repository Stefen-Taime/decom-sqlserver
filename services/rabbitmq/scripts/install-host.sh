#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mountpoint -q /srv/decom-sqlserver
python3 scripts/prepare-host.py
docker compose config --quiet
docker compose build dispatcher
if [[ -f .reload-seaweed ]]; then
  cdc=/srv/decom-sqlserver/workspace/services/cdc
  docker compose --project-directory "$cdc" stop debezium
  trap 'docker compose --project-directory "$cdc" up -d debezium' EXIT
  docker compose --project-directory "$cdc" restart seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 180 seaweedfs
  docker compose --project-directory "$cdc" up -d --wait --wait-timeout 240 debezium
  rm .reload-seaweed
  trap - EXIT
fi
docker compose up -d --wait --wait-timeout 240 rabbitmq
echo 'RabbitMQ ready. Validate delivery/recovery before starting dispatcher and invoice-app watch services.'
