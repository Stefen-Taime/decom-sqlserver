#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mountpoint -q /srv/decom-sqlserver
python3 scripts/prepare-host.py
# This placeholder is used only while building; the runner always creates a fresh directory.
export PURGE_RUN_DIR=/srv/decom-sqlserver/purge/build-placeholder
docker compose config --quiet
docker compose build audit
if [[ -f .reload-seaweed ]]; then
  docker compose --project-directory ../cdc up -d --force-recreate --no-deps --wait --wait-timeout 180 seaweedfs
  rm .reload-seaweed
fi
echo 'Audit job installed. No permanent service, cleanup job or purge schedule was started.'
