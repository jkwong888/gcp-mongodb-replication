resource "google_pubsub_topic" "example" {
  project    = module.service_project.project_id
  name = "mongodb-changestream"

  message_retention_duration = "86600s"
}