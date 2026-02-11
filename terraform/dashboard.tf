# Cloud Run Service — serves the Streamlit dashboard
resource "google_cloud_run_v2_service" "dashboard" {
  name     = "arqtic-dashboard"
  location = var.region

  template {
    service_account = google_service_account.arqtic.email

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/arqtic/arqtic:latest"

      command = ["streamlit", "run", "dashboard/app.py",
        "--server.port=8080",
        "--server.address=0.0.0.0",
        "--server.headless=true",
      ]

      ports {
        container_port = 8080
      }

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
          memory = "2Gi"
        }
      }

      startup_probe {
        tcp_socket {
          port = 8080
        }
        initial_delay_seconds = 10
        failure_threshold     = 5
        period_seconds        = 15
        timeout_seconds       = 5
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

# Allow unauthenticated access to the dashboard
resource "google_cloud_run_v2_service_iam_member" "public" {
  project  = google_cloud_run_v2_service.dashboard.project
  location = google_cloud_run_v2_service.dashboard.location
  name     = google_cloud_run_v2_service.dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
