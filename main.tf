provider "google" {
  project = "your-gcp-project-id"
  region  = "us-central1"
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "your_bigquery_dataset"
  location   = "US"
}