#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run with sudo on the VM.' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mountpoint -q /srv/decom-sqlserver
python3 scripts/prepare-host.py
docker compose config --quiet
docker compose build api
docker compose --profile tools run --rm init
docker compose up -d --wait --wait-timeout 240 api dag-processor scheduler
echo 'Airflow ready. New DAGs remain paused until acceptance checks complete.'
