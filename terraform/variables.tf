variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "locality" {
  description = "Weather locality name"
  type        = string
  default     = "Toronto"
}

variable "latitude" {
  description = "Weather station latitude"
  type        = string
  default     = "43.65"
}

variable "longitude" {
  description = "Weather station longitude"
  type        = string
  default     = "-79.38"
}

variable "timezone" {
  description = "Location timezone"
  type        = string
  default     = "America/Toronto"
}

variable "schedule" {
  description = "Pipeline execution schedule (Cloud Scheduler cron)"
  type        = string
  default     = "0 */6 * * *" # Every 6 hours
}
