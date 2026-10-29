#!/usr/bin/env bash
set -Eeuo pipefail

exec > >(tee -a /var/log/decom-sqlserver-bootstrap.log) 2>&1
trap 'printf "Bootstrap failed at line %s\n" "$LINENO" >&2' ERR

disk=/dev/disk/by-id/google-lab-data
data_dir=/srv/decom-sqlserver
ready_file=/var/lib/decom-sqlserver/bootstrap-complete
export DEBIAN_FRONTEND=noninteractive

install -d -m 0755 /var/lib/decom-sqlserver
rm -f "$ready_file"

for attempt in {1..60}; do
  if [[ -b "$disk" ]]; then
    break
  fi
  sleep 2
done
[[ -b "$disk" ]] || { echo "Persistent data disk not found" >&2; exit 1; }

# Never reformat a disk that already has a filesystem or partition table.
if ! fs_type=$(blkid -p -s TYPE -o value "$disk"); then
  signatures=$(wipefs --no-act --noheadings --output TYPE "$disk")
  [[ -z "$signatures" ]] || { echo "Unrecognized data disk; refusing to format" >&2; exit 1; }
  mkfs.ext4 -m 0 -L lab-data "$disk"
  fs_type=ext4
fi
[[ "$fs_type" == ext4 ]] || { echo "Expected an ext4 data disk" >&2; exit 1; }

install -d -m 0755 "$data_dir"
disk_uuid=$(blkid -s UUID -o value "$disk")
if ! findmnt --fstab --mountpoint "$data_dir" >/dev/null; then
  printf 'UUID=%s %s ext4 defaults 0 2\n' "$disk_uuid" "$data_dir" >> /etc/fstab
fi
mountpoint -q "$data_dir" || mount "$data_dir"
[[ "$(findmnt -n -o UUID --target "$data_dir")" == "$disk_uuid" ]] || {
  echo "Unexpected filesystem mounted at $data_dir" >&2
  exit 1
}

install -d -m 0755 "$data_dir"/{docker,containerd,workspace,exports}

# Both Docker and containerd keep their data on the persistent disk.
# Refuse to hide existing boot-disk data underneath a new bind mount.
for runtime in docker containerd; do
  target="/var/lib/$runtime"
  install -d -m 0755 "$target"
  if ! mountpoint -q "$target"; then
    [[ -z "$(find "$target" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
      echo "Existing runtime data at $target; refusing to hide it" >&2
      exit 1
    }
    if ! findmnt --fstab --mountpoint "$target" >/dev/null; then
      printf '%s/%s %s none bind,x-systemd.requires-mounts-for=%s 0 0\n' \
        "$data_dir" "$runtime" "$target" "$data_dir" >> /etc/fstab
    fi
    mount "$target"
  fi
done

# Fail closed if the data disk is unavailable on a subsequent boot.
for unit in docker containerd; do
  install -d -m 0755 "/etc/systemd/system/$unit.service.d"
  cat > "/etc/systemd/system/$unit.service.d/lab-storage.conf" <<EOF
[Unit]
RequiresMountsFor=$data_dir /var/lib/$unit
EOF
done

install -d -m 0755 /etc/docker
if [[ ! -f /etc/docker/daemon.json ]]; then
  cat > /etc/docker/daemon.json <<'EOF'
{
  "log-driver": "local",
  "log-opts": {"max-size": "10m", "max-file": "3"},
  "live-restore": true
}
EOF
fi

if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=3 update
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=3 install -y ca-certificates curl git jq
  install -m 0755 -d /etc/apt/keyrings
  curl --fail --silent --show-error --location --retry 5 \
    https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  cat > /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $VERSION_CODENAME
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=3 update
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=3 install -y \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

systemctl daemon-reload
systemctl enable --now containerd docker
docker info >/dev/null
docker compose version
touch "$ready_file"
echo "Lab host ready. Persistent storage: $data_dir"
