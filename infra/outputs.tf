output "instance_name" {
  value = google_compute_instance.lab.name
}

output "zone" {
  value = var.zone
}

output "project_id" {
  value = var.project_id
}

output "external_ip" {
  description = "Ephemeral address for outbound access; direct inbound connections are blocked."
  value       = google_compute_instance.lab.network_interface[0].access_config[0].nat_ip
}

output "ssh_command" {
  value = "gcloud compute ssh ${google_compute_instance.lab.name} --project=${var.project_id} --zone=${var.zone} --tunnel-through-iap"
}

output "stop_command" {
  value = "gcloud compute instances stop ${google_compute_instance.lab.name} --project=${var.project_id} --zone=${var.zone}"
}

output "start_command" {
  value = "gcloud compute instances start ${google_compute_instance.lab.name} --project=${var.project_id} --zone=${var.zone}"
}

output "data_directory" {
  value = "/srv/decom-sqlserver"
}
