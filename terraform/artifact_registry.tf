# Artifact Registry for Docker images
resource "google_artifact_registry_repository" "arqtic" {
  location      = var.region
  repository_id = "arqtic"
  format        = "DOCKER"
  description   = "Docker images for Arqtic weather pipeline and dashboard"
}
