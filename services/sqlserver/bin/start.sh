#!/usr/bin/env bash
set -Eeuo pipefail

secret=/run/secrets/sa_password
[[ -r "$secret" && -s "$secret" ]] || {
  echo "SQL Server password secret is missing or unreadable" >&2
  exit 1
}
export MSSQL_SA_PASSWORD
MSSQL_SA_PASSWORD=$(cat "$secret")
# Keep the engine as PID 1 so Docker's SIGTERM reaches it directly.
# Database initialization is handled explicitly by scripts/install-host.sh.
exec /opt/mssql/bin/sqlservr
