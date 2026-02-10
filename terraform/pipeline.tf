# Cloud Run Job — executes the data pipeline
resource "google_cloud_run_v2_job" "pipeline" {
  name     = "arqtic-pipeline"
  location = var.region

  template {
    template {
      service_account = google_service_account.arqtic.email

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/arqtic/arqtic:latest"

        command = ["python", "-m", "pipeline.run"]

        env {
          name  = "DATA_PATH"
          value = "gs://${google_storage_bucket.data.name}"
        }
        env {
          name  = "LOCALITY"
          value = var.locality
        }
        env {
          name  = "LATITUDE"
          value = var.latitude
        }
        env {
          name  = "LONGITUDE"
          value = var.longitude
        }
        env {
          name  = "TIMEZONE"
          value = var.timezone
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }

      timeout     = "300s"
      max_retries = 1
    }
  }
}

# Cloud Scheduler — triggers the pipeline on schedule
resource "google_cloud_scheduler_job" "pipeline_trigger" {
  name        = "arqtic-pipeline-trigger"
  region      = var.region
  description = "Triggers the Arqtic weather pipeline every 6 hours"
  schedule    = var.schedule
  time_zone   = var.timezone

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.pipeline.name}:run"

    oauth_token {
      service_account_email = google_service_account.arqtic.email
    }
  }
}
