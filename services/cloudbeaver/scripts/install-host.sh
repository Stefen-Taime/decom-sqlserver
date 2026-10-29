#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the lab VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
root=/srv/decom-sqlserver
mountpoint -q "$root" || { echo 'Persistent disk is not mounted.' >&2; exit 1; }
docker network inspect decom-warehouse-backend >/dev/null 2>&1 || docker network create --internal decom-warehouse-backend >/dev/null
python3 scripts/prepare-host.py
printf 'CLOUDBEAVER_DATA_DIR=%s/cloudbeaver\nCLOUDBEAVER_SECRET_DIR=%s/secrets/cloudbeaver\n' "$root" "$root" > .env
chmod 0600 .env
docker compose config --quiet
docker compose up -d --wait --wait-timeout 240
docker compose ps
