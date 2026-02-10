variables {
  project_id = "test-project"
  region     = "us-central1"
}

run "storage_bucket_name" {
  command = plan

  assert {
    condition     = google_storage_bucket.data.name == "test-project-arqtic-data"
    error_message = "Data bucket name should follow project-arqtic-data convention"
  }
}

run "pipeline_job_has_environment" {
  command = plan

  assert {
    condition     = google_cloud_run_v2_job.pipeline.template[0].template[0].containers[0].env[0].name == "DATA_PATH"
    error_message = "Pipeline job must have DATA_PATH environment variable"
  }
}

run "dashboard_is_public" {
  command = plan

  assert {
    condition     = google_cloud_run_v2_service_iam_member.public.member == "allUsers"
    error_message = "Dashboard must allow unauthenticated access"
  }
}
