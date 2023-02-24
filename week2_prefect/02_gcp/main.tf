terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "focus-poet-376519"
}

#resource "google_storage_bucket" "data-lake-bucket-maria"{
#  name     = "random-bucket-maria-123"
#  location = "EU"
#}

resource "google_bigquery_dataset" "default" {
  dataset_id = "dezoomcamp2"
  location   = "US"
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "rides"
}
