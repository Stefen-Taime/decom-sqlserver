locals {
  labels = {
    project     = "decom-sqlserver"
    environment = "lab"
    managed_by  = "terraform"
  }
}

resource "google_project_service" "required" {
  for_each = toset([
    "compute.googleapis.com",
    "iap.googleapis.com",
    "oslogin.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_compute_network" "lab" {
  name                    = "${var.name}-vpc"
  auto_create_subnetworks = false

  depends_on = [google_project_service.required]
}

resource "google_compute_subnetwork" "lab" {
  name          = "${var.name}-subnet"
  region        = var.region
  network       = google_compute_network.lab.id
  ip_cidr_range = "10.80.0.0/24"
}

resource "google_compute_firewall" "iap_ssh" {
  name          = "${var.name}-iap-ssh"
  network       = google_compute_network.lab.name
  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["${var.name}-ssh"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_disk" "data" {
  name   = "${var.name}-data"
  type   = "pd-balanced"
  zone   = var.zone
  size   = var.data_disk_size_gb
  labels = local.labels

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.required]
}

resource "google_compute_instance" "lab" {
  name                      = var.name
  machine_type              = var.machine_type
  zone                      = var.zone
  allow_stopping_for_update = true
  labels                    = local.labels
  tags                      = ["${var.name}-ssh"]

  boot_disk {
    auto_delete = true

    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = 30
      type  = "pd-balanced"
    }
  }

  attached_disk {
    source      = google_compute_disk.data.id
    device_name = "lab-data"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.lab.id

    # Outbound package/image downloads; ingress is restricted to SSH via IAP.
    # An ephemeral address avoids adding a Cloud NAT gateway to this one-VM lab.
    access_config {}
  }

  # No service account is attached: these Docker workloads need no GCP API access.
  metadata = {
    enable-oslogin         = "TRUE"
    block-project-ssh-keys = "TRUE"
    startup-script         = file("${path.module}/scripts/bootstrap.sh")
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  lifecycle {
    precondition {
      condition     = startswith(var.zone, "${var.region}-")
      error_message = "The zone must belong to the selected region."
    }
  }
}

resource "google_compute_instance_iam_member" "admin" {
  project       = var.project_id
  zone          = var.zone
  instance_name = google_compute_instance.lab.name
  role          = "roles/compute.osAdminLogin"
  member        = var.admin_member
}

resource "google_iap_tunnel_instance_iam_member" "admin" {
  project  = var.project_id
  zone     = var.zone
  instance = google_compute_instance.lab.name
  role     = "roles/iap.tunnelResourceAccessor"
  member   = var.admin_member

  condition {
    title      = "ssh-only"
    expression = "destination.port == 22"
  }
}
