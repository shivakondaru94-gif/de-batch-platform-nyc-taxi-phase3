# Capturing deployment evidence

Seven screenshots demonstrate that the platform runs unattended on a target
machine. Take them in this order; each corresponds to a numbered slot in
Appendix A of the finalisation report. Save them into `phase3_report/screenshots/`
with exactly these filenames — the report picks them up automatically and
falls back to a placeholder box if a file is missing.

## Before you start

Install Docker Desktop (WSL2 backend on Windows) and confirm:

```bash
docker compose version
```

Then, from the repository root:

```bash
make up
```

## The seven captures

| File | What to capture | Shows |
|---|---|---|
| `01-make-up.png` | The terminal after `make up` finishes, showing "all services healthy" and the printed URLs | One command brings the platform up with no manual editing |
| `02-compose-ps.png` | Output of `docker compose ps` | All nine services running, health status, no ports on `0.0.0.0` |
| `03-airflow-dags.png` | Airflow at `http://127.0.0.1:8080`, DAG list | Both DAGs present and **already unpaused** — no manual intervention |
| `04-airflow-graph.png` | The `quarterly_features` graph view | The task graph matches the design; green = succeeded |
| `05-minio-buckets.png` | MinIO console at `http://127.0.0.1:9001`, bucket list, then inside `silver/trips/` | The four medallion zones and the partitioned key layout |
| `06-spark-ui.png` | Spark master at `http://127.0.0.1:8081` during or after a job | Workers registered; the submitted application |
| `07-make-smoke.png` | The terminal after `make smoke`, including the final row-count table | End-to-end proof: data ingested, validated, aggregated, served |
| `08-features.png` | The top-10 busiest-windows query against `ml.features_demand_hourly` | The delivered features themselves, read back from the serving store |

## Four more that materially strengthen the report

These are worth the extra few minutes: each one turns a claim the report makes
into something visible.

| File | Command / place | Why it matters |
|---|---|---|
| `09-scale.png` | `docker compose up -d --scale spark-worker=4`, then Spark UI (8081) | The report claims scalability is horizontal and needs no code change. This shows four workers registered after one flag. |
| `10-scan.png` | `make scan` | Supply-chain evidence, and the conception-phase feedback asked specifically about security. |
| `11-secrets.png` | `make secrets-check` | Shows `.env` is neither tracked nor anywhere in git history. |
| `12-quarantine.png` | MinIO console (9001) -> `quarantine` bucket -> `year=2024/month=01/` | Proves rejected records are retained with reasons rather than silently dropped -- the design decision the report argues hardest for. |

For `09-scale.png`, scale back afterwards:

```bash
docker compose up -d --scale spark-worker=1
```

Note: the lineage and quality tables are written by the **Airflow DAG**, not by
`make smoke`. They stay empty after a smoke run alone. To evidence them,
trigger the quarterly graph from the Airflow interface and screenshot the
tables afterwards; otherwise omit that capture rather than showing empty ones.

## Running the end-to-end check

```bash
make smoke
```

This ingests one month, validates it, aggregates the quarter and prints what
reached the feature store. The final table in that output is the single most
useful piece of evidence in the whole appendix — it is the moment the pipeline
demonstrably works.

If a step fails, capture the failure too. A report that shows a real problem
and how it was resolved is stronger than one that shows only success, and the
finalisation phase explicitly asks what went wrong and why.

## Cropping

Crop to the relevant region and keep text legible at page width. Screenshots
are embedded at 300 dpi in the report, so a capture around 1600 px wide is
ample.
