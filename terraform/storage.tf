# GCS bucket for weather data
resource "google_storage_bucket" "data" {
  name          = "${var.project_id}-arqtic-data"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# GCS bucket for Terraform state (created manually before first apply)
# See: gcloud storage buckets create gs://arqtic-tf-state --location=us-central1
