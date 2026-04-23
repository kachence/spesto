# infra/terraform/

GCP infrastructure as code. Nothing provisioned yet.

Expected resources at v1:

- GCS bucket: `spesto-landed` (landed zone; object lifecycle retains raw zips forever).
- BigQuery datasets: `landed_raw`, `staged`, `prod`, `views`.
- Cloud SQL Postgres instance: small, operational.
- Cloud Run Jobs: one per `jobs/*/` directory.
- Cloud Run Service: `apps/api/`.
- Cloud Scheduler jobs: daily triggers for each Cloud Run Job.
- Artifact Registry: container images for jobs + API.
- Service accounts + IAM: one per Cloud Run workload, least-privilege.
- Secret Manager: API keys (Anthropic, Google Places), DB passwords.

Staging and production environments configured as separate workspaces or separate
projects (TBD).
