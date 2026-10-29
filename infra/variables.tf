variable "project_id" {
  description = "Existing GCP project with billing enabled."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Use the GCP project ID, not its display name or project number."
  }
}

variable "region" {
  description = "Region for the lab network and VM."
  type        = string
  default     = "northamerica-northeast1"
}

variable "zone" {
  description = "Zone in the selected region."
  type        = string
  default     = "northamerica-northeast1-b"
}

variable "name" {
  description = "Prefix for resources belonging to this lab."
  type        = string
  default     = "decom-sqlserver-lab"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,39}[a-z0-9]$", var.name))
    error_message = "Use 2–41 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit."
  }
}

variable "machine_type" {
  description = "12 GiB / 4 vCPU by default; use e2-standard-4 for 16 GiB."
  type        = string
  default     = "e2-custom-4-12288"
}

variable "data_disk_size_gb" {
  description = "Persistent storage for Docker images, volumes and lab files."
  type        = number
  default     = 100

  validation {
    condition     = var.data_disk_size_gb >= 50 && floor(var.data_disk_size_gb) == var.data_disk_size_gb
    error_message = "Provide a whole number of GiB, at least 50."
  }
}

variable "admin_member" {
  description = "IAM member receiving OS Admin Login and IAP access to this VM, e.g. user:you@example.com."
  type        = string

  validation {
    condition     = can(regex("^(user|group):[^ ]+@[^ ]+$", var.admin_member))
    error_message = "Use user:email@example.com or group:email@example.com."
  }
}
