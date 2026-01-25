terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0" # It is recommended to use a version constraint
    }
  }
}
provider "google" {
  credentials = "./keys/my-creds.json"
  project     = "authentic-host-485219-t5"
  region      = "us-central1"
}

resource "google_storage_bucket" "auto-expire" {
  name          = "authentic-host-485219-t5-terra-bucket"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "demo_dataset"
}
