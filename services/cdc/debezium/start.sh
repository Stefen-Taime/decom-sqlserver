#!/usr/bin/env bash
set -Eeuo pipefail
export DEBEZIUM_DB_PASSWORD ARCHIVE_WEBHOOK_SECRET
DEBEZIUM_DB_PASSWORD=$(cat /run/secrets/debezium_password)
ARCHIVE_WEBHOOK_SECRET=$(cat /run/secrets/webhook)
cd /debezium
exec /debezium/run.sh
