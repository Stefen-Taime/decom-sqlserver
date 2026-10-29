#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo "Run with sudo on the lab VM." >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
root=/srv/decom-sqlserver
mountpoint -q "$root" || { echo "Persistent disk is not mounted." >&2; exit 1; }
docker inspect decom-sqlserver-sqlserver-1 >/dev/null

python3 scripts/prepare-host.py
printf 'CDC_DATA_DIR=%s/cdc\nCDC_SECRET_DIR=%s/secrets/cdc\n' "$root" "$root" > .env
chmod 0600 .env
docker compose config --quiet
docker compose build archiver
docker compose up -d --wait --wait-timeout 180 seaweedfs
docker compose --profile tools run --rm storage-init
# Make the archive durable before the producer can acknowledge any events.
printf 'fs.configure -locationPrefix=/buckets/cdc-raw/ -fsync=true -volumeGrowthCount=1 -apply\nfs.configure\n' |
  docker compose exec -T seaweedfs weed shell -master=seaweedfs:9333 -filer=seaweedfs:8888
docker compose up -d --wait --wait-timeout 180 archiver
docker compose up -d --wait --wait-timeout 240 debezium
docker compose ps
echo 'CDC services started. Check snapshot completion separately. SQL CDC cleanup remains disabled.'
