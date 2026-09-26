# Threat model and security controls

Phase 1 listed controls. This document states what each control *defends
against*, which is the question the Phase 1 feedback raised: how the design
copes with vulnerabilities in a deployed environment.

## Trust boundaries

1. **Host ↔ platform** — the Docker host and everything outside it.
2. **Platform ↔ internet** — the ingestion service reaches a public publisher.
3. **Service ↔ service** — containers on the `platform` bridge network.
4. **Platform ↔ consumer** — the machine learning application reading features.

## Threats and the control that answers each

| # | Threat | Control implemented | Residual risk |
|---|---|---|---|
| T1 | Another device on the LAN reaches the lake or warehouse | No service binds `0.0.0.0`; operator UIs bound to `127.0.0.1`; data stores publish **no** host port | Anything running on the host itself can still reach the loopback ports |
| T2 | A compromised container escalates to root on the host | `no-new-privileges:true` and `cap_drop: ALL` on every service; Postgres keeps only the five capabilities `initdb` requires | Container runtime escapes remain out of scope for a bridge network |
| T3 | A compromised processing container reads or destroys the whole lake | Spark authenticates with a scoped MinIO service account, never the root credentials | The scoped account currently holds `readwrite` across buckets; per-bucket policies would narrow it further |
| T4 | The consuming application corrupts the feature store | `ml_reader` role holds `SELECT` only; writes are structurally impossible | The role's password is a development placeholder |
| T5 | Credentials leak through the repository, or placeholder passwords survive into use | `.env` is git-ignored and never committed; `make init` **generates** a fresh random secret per credential, so no default password can be left in place; all are injected as environment variables | Environment variables are visible via `docker inspect`; Docker secrets or a vault is the production answer |
| T6 | A malicious or vulnerable base image enters the platform | Every image pinned to an exact release tag, never `latest`; `make scan` runs Trivy against each | **This risk materialised twice.** Bitnami withdrew every versioned `bitnami/spark` tag from Docker Hub (substituted: `bitnamilegacy/spark`), and later MinIO withdrew anonymous pulls of `minio/minio` and `minio/mc` (substituted: the vendor's `quay.io` mirror, same release tags). A tag can be *deleted* as well as moved: a digest guarantees integrity, but only a mirror or vendored copy guarantees availability |
| T7 | A runaway or hostile job exhausts the host | CPU and memory limits on all nine services | Limits are tuned for a 16 GB development machine, not a production host |
| T8 | Data is altered and the change goes unnoticed | Bronze is immutable with a 90-day retention lock; object versioning enabled; every batch recorded in `governance.batch_lineage` | Lineage lives in the same database it describes |
| T9 | A breach is never detected | MinIO access logging; quality and lineage tables give an auditable record per batch | No alerting or log shipping is configured |
| T10 | Traffic between services is intercepted | Services share a private bridge network not reachable off-host | Traffic is **not** TLS-encrypted in transit; this is the largest deliberate gap |

## Deliberate gaps, and why

- **No TLS between services.** Terminating TLS on MinIO, Postgres and Spark
  needs a certificate authority and per-service certificates. On a
  single-host development platform whose network is not reachable off-host,
  the added complexity outweighs the benefit. In production this is the first
  thing to add.
- **No read-only root filesystems.** `read_only: true` is valuable but each
  image needs its writable paths mapped to `tmpfs` first. Declaring it
  untested would be worse than declaring it absent.
- **Secrets as environment variables.** Correct for local development,
  insufficient for production, where a vault should issue short-lived
  credentials. Generation removes the weaker failure of a shipped default,
  but not the exposure of the variable itself.

## Verification

```bash
make scan          # Trivy vulnerability scan of every image
make secrets-check # fail if a credential was ever committed
make config        # render the resolved compose configuration
```
