"""Local verification of the pipeline's TRANSFORMATION LOGIC only.

This is NOT the containerised platform. It reimplements, in pandas, exactly
the rules and aggregation that bronze_to_silver.py and silver_to_gold.py
apply in Spark, against the real source file, so the logic can be checked
before Docker is available. Any divergence in results between this and the
Spark jobs would be a bug in one of them.
"""

import hashlib
import os
import time

import pandas as pd
import requests

URL = ("https://d37ci6vzurychx.cloudfront.net/trip-data/"
       "yellow_tripdata_2024-01.parquet")
LOCAL = "yellow_tripdata_2024-01.parquet"
YEAR, MONTH = 2024, 1

MIN_DISTANCE_MI, MAX_DISTANCE_MI = 0.1, 100.0
MIN_DURATION_MIN, MAX_DURATION_MIN = 1.0, 300.0
MAX_FARE = 1000.0


def download():
    """Mirrors ingest_tlc.py: streamed, checksummed, idempotent."""
    if os.path.exists(LOCAL):
        print(f"[ingest] {LOCAL} already present - skipping (idempotent)")
        return None
    digest = hashlib.sha256()
    t0 = time.time()
    with requests.get(URL, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(LOCAL, "wb") as fh:
            for chunk in r.iter_content(8 * 1024 * 1024):
                fh.write(chunk)
                digest.update(chunk)
    mb = os.path.getsize(LOCAL) / 1024 / 1024
    print(f"[ingest] {mb:.1f} MiB in {time.time()-t0:.1f}s  "
          f"sha256={digest.hexdigest()[:16]}")
    return digest.hexdigest()


def main():
    download()

    print("\n[bronze->silver] reading raw month")
    raw = pd.read_parquet(LOCAL, columns=[
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID", "trip_distance",
        "fare_amount", "total_amount", "passenger_count"])
    rows_in = len(raw)
    print(f"  rows_in = {rows_in:,}")

    t = pd.DataFrame({
        "pickup_ts": raw["tpep_pickup_datetime"],
        "dropoff_ts": raw["tpep_dropoff_datetime"],
        "pickup_zone_id": raw["PULocationID"].astype("Int32"),
        "trip_distance": raw["trip_distance"].astype("float64"),
        "fare_amount": raw["fare_amount"].astype("float64"),
        "total_amount": raw["total_amount"].astype("float64"),
    })
    t["duration_min"] = (
        t["dropoff_ts"] - t["pickup_ts"]).dt.total_seconds() / 60.0

    # The eight rules, in the same order as bronze_to_silver.py
    rules = {
        "null_timestamp": t["pickup_ts"].isna() | t["dropoff_ts"].isna(),
        "month_mismatch": (t["pickup_ts"].dt.year != YEAR)
                          | (t["pickup_ts"].dt.month != MONTH),
        "nonpositive_duration": t["duration_min"] < MIN_DURATION_MIN,
        "implausible_duration": t["duration_min"] > MAX_DURATION_MIN,
        "implausible_distance": (t["trip_distance"] < MIN_DISTANCE_MI)
                                | (t["trip_distance"] > MAX_DISTANCE_MI),
        "negative_fare": t["fare_amount"] < 0,
        "implausible_fare": t["fare_amount"] > MAX_FARE,
        "invalid_zone": t["pickup_zone_id"].isna()
                        | (t["pickup_zone_id"] < 1)
                        | (t["pickup_zone_id"] > 265),
    }

    print("\n  rejections per rule (a row may violate several):")
    reject_any = pd.Series(False, index=t.index)
    for name, cond in rules.items():
        cond = cond.fillna(False)
        n = int(cond.sum())
        print(f"    {name:24} {n:>9,}  ({n/rows_in*100:5.2f}%)")
        reject_any |= cond

    rows_quarantined = int(reject_any.sum())
    clean = t[~reject_any]

    before_dedup = len(clean)
    silver = clean.drop_duplicates(
        subset=["pickup_ts", "dropoff_ts", "pickup_zone_id", "total_amount"])
    dupes = before_dedup - len(silver)
    rows_out = len(silver)

    print(f"\n  rows_quarantined = {rows_quarantined:,} "
          f"({rows_quarantined/rows_in*100:.2f}%)")
    print(f"  duplicates removed = {dupes:,}")
    print(f"  rows_out (silver)  = {rows_out:,}")

    print("\n[silver->gold] hourly tumbling windows per pickup zone")
    g = silver.copy()
    g["window_start"] = g["pickup_ts"].dt.floor("h")
    feat = g.groupby(["pickup_zone_id", "window_start"], observed=True).agg(
        trip_count=("pickup_zone_id", "size"),
        avg_trip_distance=("trip_distance", "mean"),
        avg_fare_amount=("fare_amount", "mean"),
        avg_duration_min=("duration_min", "mean"),
        p90_duration_min=("duration_min", lambda s: s.quantile(0.9)),
    ).reset_index()

    feat["hour_of_day"] = feat["window_start"].dt.hour
    # Spark dayofweek: 1=Sunday .. 7=Saturday
    feat["day_of_week"] = (feat["window_start"].dt.dayofweek + 1) % 7 + 1
    feat["is_weekend"] = feat["day_of_week"].isin([1, 7])

    print(f"\n  feature_rows = {len(feat):,}")
    print(f"  zones        = {feat['pickup_zone_id'].nunique()}")
    print(f"  first_window = {feat['window_start'].min()}")
    print(f"  last_window  = {feat['window_start'].max()}")
    print(f"  weekend rows = {int(feat['is_weekend'].sum()):,}")

    print("\n  sample of the delivered feature table:")
    cols = ["pickup_zone_id", "window_start", "trip_count",
            "avg_trip_distance", "avg_fare_amount", "avg_duration_min",
            "p90_duration_min", "hour_of_day", "day_of_week", "is_weekend"]
    with pd.option_context("display.width", 200,
                           "display.max_columns", 20):
        print(feat.sort_values("trip_count", ascending=False)
                  .head(8)[cols].to_string(index=False))

    # Invariants the quarterly DAG's verify task asserts
    print("\n[verify] post-load assertions")
    checks = {
        "row_count_positive": len(feat) > 0,
        "no_null_zones": feat["pickup_zone_id"].notna().all(),
        "no_negative_counts": (feat["trip_count"] >= 0).all(),
        "unique_zone_window": not feat.duplicated(
            ["pickup_zone_id", "window_start"]).any(),
    }
    for name, ok in checks.items():
        print(f"    {name:22} {'PASS' if ok else 'FAIL'}")

    feat.to_csv("gold_features_sample.csv", index=False)
    print(f"\nwrote gold_features_sample.csv ({len(feat):,} rows)")


if __name__ == "__main__":
    main()
