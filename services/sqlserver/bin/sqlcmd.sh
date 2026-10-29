#!/usr/bin/env bash
set -Eeuo pipefail

# Keep the password out of command arguments, Compose interpolation and logs.
export SQLCMDPASSWORD
SQLCMDPASSWORD=$(cat /run/secrets/sa_password)
exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -l 10 -W -s '|' "$@"
