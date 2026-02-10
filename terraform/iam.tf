# Service account for pipeline and dashboard (least privilege)
resource "google_service_account" "arqtic" {
  account_id   = "arqtic-runner"
  display_name = "Arqtic Pipeline & Dashboard"
}

# Pipeline needs to write weather data to GCS
resource "google_storage_bucket_iam_member" "data_writer" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.arqtic.email}"
}

# Cloud Run invoker for Scheduler to trigger the pipeline job
resource "google_cloud_run_v2_job_iam_member" "scheduler_invoke" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.pipeline.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.arqtic.email}"
}
