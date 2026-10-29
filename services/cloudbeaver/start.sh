#!/usr/bin/env bash
set -Eeuo pipefail
CB_ADMIN_PASSWORD=$(cat /run/secrets/admin_password)
export CB_ADMIN_PASSWORD
cd /opt/cloudbeaver
exec ./run-cloudbeaver-server.sh
