# Phase 1 — Concept (PebblePad text field)

**Title:** A Reproducible Batch Data Architecture for Quarterly ML Feature
Generation on Urban Mobility Data

---

## Submission text (200 words — paste into the PebblePad field)

The system supplies a quarterly-retrained demand-forecasting model with
aggregated features from NYC TLC yellow taxi records: over 36 million
timestamped trips per year, ingested monthly as Parquet.

Every component is an isolated Docker microservice, declared in a single
`docker-compose.yml` so the whole platform is Infrastructure as Code and
rebuilds identically on any machine. A Python ingestion service streams each
monthly file into **MinIO**, an S3-compatible data lake chosen over HDFS for
the same object-store semantics at a fraction of the memory cost. The lake
follows a medallion layout: bronze holds immutable raw objects, silver holds
validated and deduplicated trips, gold holds hourly features per pickup zone.
**Apache Spark** (one master, scalable workers) performs cleaning and
aggregation; **Apache Airflow** schedules monthly ingestion and quarterly
aggregation, with exponential-backoff retries and catchup backfills.
**PostgreSQL** serves the gold features to the machine learning application.

Reliability comes from idempotent ingestion, healthchecks and a quarantine
bucket that makes every rejected row auditable. Scalability is horizontal:
stateless workers scale by replica count. Security and governance rest on
environment-injected secrets, a scoped lake service account, a read-only
consumer role, and lineage plus quality-check tables recording every batch.

---

## Justification of each design decision (working notes, not submitted)

### Which microservices handle ingestion?
`ingest-tlc`, a small Python service. It streams the source file in 8 MiB
chunks (the file never sits in RAM), computes a SHA-256 for provenance, and
performs a `head_object` check first so a re-run is a no-op rather than a
duplicate. Kafka was rejected deliberately: the source arrives as monthly
files, and introducing a broker for a batch cadence would add a stateful
component with no benefit — an honest trade-off worth stating in the report.

### Which microservices handle storage?
MinIO for the lake, PostgreSQL for serving. MinIO gives S3A-compatible access,
so the Spark jobs would run unchanged against real AWS S3 — the portability
argument. HDFS was rejected: a NameNode plus DataNodes plus YARN would consume
most of a 16 GB development machine and earn no additional marks.

### Which microservices handle pre-processing and aggregation?
Spark, as a standalone cluster (`spark-master` + N `spark-worker` replicas).
`bronze_to_silver.py` enforces eight named business rules; `silver_to_gold.py`
aggregates a quarter of trips into hourly tumbling windows per pickup zone.

### Which microservices handle delivery to the frontend?
PostgreSQL, exposing `ml.features_demand_hourly`. The ML application reads
that one table through a read-only role, so the lake internals can change
without breaking the consumer contract.

### Reliability, scalability, maintainability
- *Reliability*: Airflow retries with exponential backoff; ingestion is
  idempotent; bad rows are quarantined with a reason rather than dropped;
  every service has a healthcheck and ordered `depends_on` conditions;
  MinIO object versioning allows recovery from a bad write.
- *Scalability*: workers are stateless, so `--scale spark-worker=4` adds
  capacity with no code change; Hive-style partitioning lets Spark prune
  whole directories instead of scanning the lake.
- *Maintainability*: one compose file, exactly pinned image tags, a Makefile
  as the single entry point, jobs sharing one `common.py` session builder.

### Security, governance, protection
Secrets are injected from a git-ignored `.env`; the pipeline authenticates
with a scoped MinIO service account rather than the root credentials; the ML
consumer holds a `SELECT`-only role; containers run as non-root; only the
UI ports are published. `governance.batch_lineage` traces every gold row back
to its source object, and `governance.quality_checks` stores the pass/fail of
each expectation per batch. Bronze carries a 90-day retention lock.

### Which Docker images, and are they modified?
`minio`, `postgres:16-alpine`, `apache/airflow:2.9.3` and `bitnami/spark:3.5.1`,
all version-pinned. Two are modified: the Spark image gains the `hadoop-aws`,
`aws-java-sdk-bundle` and PostgreSQL JDBC jars needed for S3A and JDBC; the
Airflow image gains a JRE plus the Spark and Postgres providers.

### Which data?
NYC TLC Yellow Taxi trip records — open, Parquet, roughly 3 million rows per
month with pickup and dropoff timestamps on every row. Twelve months exceed
the one-million-row requirement by a wide margin.

### At which frequency?
Ingestion monthly (5th of the month, after TLC publishes); aggregation and
delivery quarterly (10th of January, April, July, October), matching the
stated retraining cadence of the frontend application.

---

## Advantages and disadvantages of the draft

**Advantages.** Fully reproducible from one command; every layer swappable
behind a stable interface (MinIO → S3, Postgres → any warehouse); horizontal
scaling without code change; raw data immutable, so any downstream bug is
replayable; quality and lineage are first-class tables rather than an
afterthought.

**Disadvantages.** Spark standalone has no resource queueing, so concurrent
DAGs contend for the same workers. A single-node MinIO is a single point of
failure — acceptable for local development, but production would need
distributed mode. Airflow's LocalExecutor caps parallelism at one machine;
CeleryExecutor or Kubernetes would be the production path. Parquet on object
storage gives no ACID guarantees — Delta Lake or Iceberg would add them, at
the cost of another dependency. The quarterly cadence means feature freshness
lags by up to three months, which is inherent to the batch design and is
exactly what the Phase 3 streaming discussion should address.
