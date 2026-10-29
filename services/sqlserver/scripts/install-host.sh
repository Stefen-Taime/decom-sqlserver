#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo "Run this script with sudo on the lab VM." >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."

data_root=/srv/decom-sqlserver
mountpoint -q "$data_root" || { echo "Persistent data disk is not mounted." >&2; exit 1; }
secret_dir="$data_root/secrets/sqlserver"
backup_dir="$data_root/sqlserver/backups"
install -d -m 0700 "$secret_dir"
install -d -m 0750 -o 10001 -g 0 "$backup_dir"

if [[ ! -s "$secret_dir/sa_password" ]]; then
  # Never generate a new password for an existing initialized SQL Server volume.
  if docker volume inspect decom-sqlserver-data >/dev/null 2>&1; then
    echo "SQL Server data already exists but its password secret is missing. Restore the secret." >&2
    exit 1
  fi
  python3 - "$secret_dir/sa_password" <<'PY'
import pathlib, secrets, sys
pathlib.Path(sys.argv[1]).write_text('Aa1!' + secrets.token_hex(24) + '\n')
PY
fi
chown 10001:0 "$secret_dir/sa_password"
chmod 0400 "$secret_dir/sa_password"

# These are paths only; the password is not put in the Compose environment.
printf 'SQLSERVER_SECRET_DIR=%s\nSQLSERVER_BACKUP_DIR=%s\n' "$secret_dir" "$backup_dir" > .env
chmod 0600 .env
docker compose config --quiet
docker compose up -d --wait --wait-timeout 240 sqlserver

sql() { docker compose exec -T sqlserver /bin/bash /opt/lab/bin/sqlcmd.sh "$@"; }
sql -i /opt/lab/sql/01-configure.sql

backup="$backup_dir/WideWorldImporters-Full.bak"
if [[ ! -s "$backup" ]]; then
  curl --fail --location --silent --show-error --retry 3 \
    https://github.com/microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak \
    -o "$backup.partial"
  mv "$backup.partial" "$backup"
  chown 10001:0 "$backup"
  chmod 0440 "$backup"
  (cd "$backup_dir" && sha256sum WideWorldImporters-Full.bak > WideWorldImporters-Full.bak.sha256)
fi
if [[ -f "$backup.sha256" ]]; then
  (cd "$backup_dir" && sha256sum -c WideWorldImporters-Full.bak.sha256)
fi

sql -t 600 -i /opt/lab/sql/02-restore.sql
sql -t 180 -i /opt/lab/sql/03-cdc.sql
sql -i /opt/lab/sql/04-verify.sql
sql -t 120 -i /opt/lab/sql/05-smoke-cdc.sql
docker compose ps
echo "SQL Server source is ready. Automatic CDC cleanup is disabled."
