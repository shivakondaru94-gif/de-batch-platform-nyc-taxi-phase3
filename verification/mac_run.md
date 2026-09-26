# Second machine: macOS laptop

The platform was built from a fresh clone with `make up-lite` and no manual
configuration. Screenshots are embedded in Appendix A of the finalisation
report.

## What the captures show

- **Start-up** — every service reported healthy; the MinIO image came from
  `quay.io/minio/minio` (the Docker Hub withdrawal fix working); every
  published port bound to `127.0.0.1`.
- **Orchestration** — both DAGs active on start ("Paused 0") and catching up
  unattended. Spark applications were submitted by user `airflow`, confirming
  the orchestrated path works end to end, not only `make smoke`.
- **Lake** — bronze 5 objects (263.5 MiB), silver 14 (279.2 MiB), gold 7,381
  (29.2 MiB), quarantine 10 (12.0 MiB).

## Verification target

```
removed 73267 previously delivered rows for 2024Q1
BATCH_METRICS batch_id=smoke-2024Q1 rows_out=158105
 feature_rows | zones |    first_window     |     last_window
--------------+-------+---------------------+---------------------
       158105 |   260 | 2024-01-01 00:00:00 | 2024-03-31 23:00:00
```

January and March had been validated by then (February's run had failed), so
the quarter held two months. The first line is the idempotent reload: the
earlier January-only delivery is removed before the new one is written.

## Cross-validation

The independent pandas reference (`verify_logic.py` / `full_year.py`), given
the same months:

| Months in Q1 | feature rows | zones | window span |
|---|---|---|---|
| Jan only | 73,267 | 257 | 01-01 .. 01-31 |
| Jan + Feb | 143,732 | 260 | 01-01 .. 02-29 |
| **Jan + Mar** | **158,105** | **260** | **01-01 .. 03-31** |
| Jan + Feb + Mar | 228,570 | 261 | 01-01 .. 03-31 |

Jan + Mar matches the Mac output exactly, and identifies which months were
present. The top-10 windows also agree row for row (trip counts, distances and
fares identical; p90 duration differs at the second decimal because Spark uses
`percentile_approx`).

## Defect found from these captures

The Spark UI listed an application named `bronze_to_silver_2023-12` — a month
before the platform's start date. Both DAGs subtracted a month from
`data_interval_start`, not accounting for the fact that an Airflow run fires at
the *end* of its interval. Fixed: each DAG now targets the period of
`data_interval_start` itself, and the quarterly start date moved to January so
Q1 receives its own run.

## Open item

Two monthly runs failed. The cause was not confirmed from task logs; the single
Spark worker was saturated at the time (one application shown WAITING with 0
cores), which is plausible but unverified.
