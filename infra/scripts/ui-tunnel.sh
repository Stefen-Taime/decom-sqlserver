#!/usr/bin/env bash
set -Eeuo pipefail

INFRA_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
terraform_value() {
  terraform -chdir="$INFRA_DIR" output -raw "$1" 2>/dev/null || {
    echo 'Set LAB_PROJECT, LAB_VM and LAB_ZONE, or use the initialized Terraform checkout.' >&2
    return 1
  }
}
LAB_PROJECT=${LAB_PROJECT:-$(terraform_value project_id)}
LAB_VM=${LAB_VM:-$(terraform_value instance_name)}
LAB_ZONE=${LAB_ZONE:-$(terraform_value zone)}

echo 'CloudBeaver: http://localhost:8978/'
echo 'RabbitMQ: http://localhost:15672/'
echo 'Airflow: http://localhost:8080/'
echo 'SeaweedFS exports: http://localhost:8888/buckets/warehouse-exports/'
echo 'SeaweedFS data: http://localhost:8888/buckets/cdc-raw/raw/wwi/bootstrap-v2/data/'
echo 'SeaweedFS manifests: http://localhost:8888/buckets/cdc-raw/raw/wwi/bootstrap-v2/manifests/'
echo 'SeaweedFS storage: http://localhost:9333/'
echo 'Leave this terminal open. Ctrl+C closes the tunnel.'
exec gcloud compute ssh "$LAB_VM" \
  --project="$LAB_PROJECT" --zone="$LAB_ZONE" \
  --tunnel-through-iap --quiet -- \
  -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L 127.0.0.1:8978:127.0.0.1:8978 \
  -L 127.0.0.1:8888:127.0.0.1:8888 \
  -L 127.0.0.1:9333:127.0.0.1:9333 \
  -L 127.0.0.1:15672:127.0.0.1:15672 \
  -L 127.0.0.1:8080:127.0.0.1:8080
