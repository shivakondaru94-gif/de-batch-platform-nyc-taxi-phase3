# End-to-end run on the container platform

Recorded from `make smoke` executed in a GitHub Codespace (4-core, 16 GB,
`make up-lite` profile) on 7 September 2026, after the platform was brought up
from the repository with no manual configuration.

## Platform state

All nine services reported healthy. `docker compose ps` showed no port bound
to `0.0.0.0`; operator interfaces on `127.0.0.1` only; the data stores
published no host port.

## Lake bootstrap (`minio-init`)

```
Bucket created successfully `local/bronze`.   (with object lock)
Bucket created successfully `local/silver`.
Bucket created successfully `local/gold`.
Bucket created successfully `local/quarantine`.
versioning enabled on all four
Object locking 'GOVERNANCE' is configured for 90DAYS.
Added user `platform` successfully.   policy: readwrite
```

## Step 1 — ingestion

```
downloaded 47.6 MiB, sha256=c4d59da7bbc8abae
landed s3a://bronze/yellow_tripdata/year=2024/month=01/yellow_tripdata_2024-01.parquet
```

The SHA-256 prefix is identical to the independent local download
(`verification/logic_verification_output.txt`), confirming the same source
bytes were processed by both implementations.

## Step 3 — aggregation (silver → gold)

```
WARNING no validated data for month(s): 02, 03     (expected: only January ingested)
removed 0 previously delivered rows for 2024Q1
BATCH_METRICS batch_id=smoke-2024Q1 rows_out=73267
```

## Step 4 — what reached the feature store

```
 feature_rows | zones |    first_window     |     last_window
--------------+-------+---------------------+---------------------
        73267 |   257 | 2024-01-01 00:00:00 | 2024-01-31 23:00:00
```

## Top windows by demand (queried from `ml.features_demand_hourly`)

```
 pickup_zone_id |    window_start     | trip_count | avg_fare_amount | p90_duration_min | is_weekend
----------------+---------------------+------------+-----------------+------------------+------------
            161 | 2024-01-24 18:00:00 |        720 |           14.00 |            23.45 | f
            161 | 2024-01-18 18:00:00 |        701 |           14.85 |            23.72 | f
            161 | 2024-01-17 19:00:00 |        680 |           13.79 |            20.10 | f
             79 | 2024-01-28 01:00:00 |        668 |           14.73 |            21.48 | t
            142 | 2024-01-04 22:00:00 |        662 |           14.29 |            19.52 | f
            161 | 2024-01-30 20:00:00 |        657 |           13.71 |            20.28 | f
            161 | 2024-01-09 18:00:00 |        653 |           13.61 |            19.95 | f
            161 | 2024-01-17 18:00:00 |        651 |           13.93 |            21.43 | f
            142 | 2024-01-04 21:00:00 |        647 |           13.43 |            18.55 | f
            161 | 2024-01-30 18:00:00 |        647 |           14.14 |            22.13 | f
```

## Cross-validation against the independent pandas reference

| Quantity | Spark (platform) | pandas (reference) |
|---|---|---|
| feature rows | 73,267 | 73,267 |
| distinct zones | 257 | 257 |
| window span | 01 Jan 00:00 – 31 Jan 23:00 | identical |
| zone 161, 24 Jan 18:00 — trips / mean fare / p90 | 720 / 14.00 / 23.45 | 720 / 14.00 / 23.46 |
| zone 79, 28 Jan 01:00 — trips / weekend | 668 / true | 668 / true |

Counts and means identical. p90 differs at the second decimal because Spark
uses `percentile_approx` while the reference computed the exact quantile.

## Problems encountered and fixed during this run

1. `bitnami/spark:3.5.1` no longer resolves — vendor withdrew all versioned
   tags; switched to `bitnamilegacy/spark:3.5.6`.
2. Bootstrap script had no execute bit after a Windows clone — invoked via `sh`.
3. `mc` could not write `/root/.mc` — config directory passed explicitly.
4. Spark uid 1001 had no `/etc/passwd` entry — Ivy built `?/.ivy2`, then the
   JVM login module threw on a null user name; entry added at image build.
5. `TIMESTAMP_NTZ` cannot be cast to `BIGINT` — normalised to `TIMESTAMP`
   with the session zone pinned to UTC.
6. `array_remove(arr, NULL)` returned NULL, so validation kept zero rows and
   completed "successfully" — caught by the post-load check; replaced with
   `array_compact` and a row-conservation assertion.
7. `FileSystem.get(conf)` returned the local scheme — resolved from the path.
