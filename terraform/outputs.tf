output "dashboard_url" {
  description = "Public URL for the weather dashboard"
  value       = google_cloud_run_v2_service.dashboard.uri
}

output "data_bucket" {
  description = "GCS bucket where weather data is stored"
  value       = google_storage_bucket.data.name
}

output "pipeline_job" {
  description = "Cloud Run Job for the pipeline"
  value       = google_cloud_run_v2_job.pipeline.name
}
