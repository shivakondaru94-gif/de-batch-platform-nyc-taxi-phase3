"""Full-year reference run: all twelve months of 2024, quarter by quarter.

Same rules and same aggregation as the Spark jobs, computed independently so
the platform's own full-year run can be checked against it. Processes one
quarter at a time so memory stays bounded.
"""

import os
import sys
import time

import pandas as pd
import requests

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
QUARTERS = {"Q1": ["01", "02", "03"], "Q2": ["04", "05", "06"],
            "Q3": ["07", "08", "09"], "Q4": ["10", "11", "12"]}
YEAR = 2024

MIN_DIST, MAX_DIST = 0.1, 100.0
MIN_DUR, MAX_DUR = 1.0, 300.0
MAX_FARE = 1000.0

COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
        "trip_distance", "fare_amount", "total_amount"]


def fetch(month):
    fn = f"yellow_tripdata_{YEAR}-{month}.parquet"
    if not os.path.exists(fn):
        with requests.get(f"{BASE}/{fn}", stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(fn, "wb") as fh:
                for chunk in r.iter_content(8 << 20):
                    fh.write(chunk)
    return fn, os.path.getsize(fn) / 1024 / 1024


def validate(month):
    """Return (silver_df, rows_in, per_rule_counts, quarantined, deduped)."""
    fn, mb = fetch(month)
    raw = pd.read_parquet(fn, columns=COLS)
    rows_in = len(raw)

    t = pd.DataFrame({
        "pickup_ts": raw["tpep_pickup_datetime"],
        "dropoff_ts": raw["tpep_dropoff_datetime"],
        "pickup_zone_id": raw["PULocationID"].astype("Int32"),
        "trip_distance": raw["trip_distance"].astype("float64"),
        "fare_amount": raw["fare_amount"].astype("float64"),
        "total_amount": raw["total_amount"].astype("float64"),
    })
    del raw
    t["duration_min"] = (
        t["dropoff_ts"] - t["pickup_ts"]).dt.total_seconds() / 60.0

    rules = {
        "null_timestamp": t["pickup_ts"].isna() | t["dropoff_ts"].isna(),
        "month_mismatch": (t["pickup_ts"].dt.year != YEAR)
                          | (t["pickup_ts"].dt.month != int(month)),
        "nonpositive_duration": t["duration_min"] < MIN_DUR,
        "implausible_duration": t["duration_min"] > MAX_DUR,
        "implausible_distance": (t["trip_distance"] < MIN_DIST)
                                | (t["trip_distance"] > MAX_DIST),
        "negative_fare": t["fare_amount"] < 0,
        "implausible_fare": t["fare_amount"] > MAX_FARE,
        "invalid_zone": t["pickup_zone_id"].isna()
                        | (t["pickup_zone_id"] < 1)
                        | (t["pickup_zone_id"] > 265),
    }
    per_rule, reject_any = {}, pd.Series(False, index=t.index)
    for name, cond in rules.items():
        cond = cond.fillna(False)
        per_rule[name] = int(cond.sum())
        reject_any |= cond

    quarantined = int(reject_any.sum())
    clean = t[~reject_any]
    before = len(clean)
    silver = clean.drop_duplicates(
        subset=["pickup_ts", "dropoff_ts", "pickup_zone_id", "total_amount"])
    return silver, rows_in, per_rule, quarantined, before - len(silver), mb


def aggregate(silver):
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
    feat["day_of_week"] = (feat["window_start"].dt.dayofweek + 1) % 7 + 1
    feat["is_weekend"] = feat["day_of_week"].isin([1, 7])
    return feat


def main():
    t0 = time.time()
    month_rows, rule_totals, quarter_rows = [], {}, []
    total_in = total_q = total_out = total_mb = 0

    for qname, months in QUARTERS.items():
        silvers = []
        for m in months:
            silver, rin, per_rule, quar, dedup, mb = validate(m)
            month_rows.append((f"{YEAR}-{m}", rin, quar, dedup, len(silver)))
            for k, v in per_rule.items():
                rule_totals[k] = rule_totals.get(k, 0) + v
            total_in += rin
            total_q += quar
            total_out += len(silver)
            total_mb += mb
            silvers.append(silver)
            print(f"  {YEAR}-{m}: in={rin:,} quarantined={quar:,} "
                  f"out={len(silver):,}", flush=True)

        feat = aggregate(pd.concat(silvers, ignore_index=True))
        del silvers
        quarter_rows.append((qname, len(feat), feat["pickup_zone_id"].nunique(),
                             feat["window_start"].min(),
                             feat["window_start"].max(),
                             int(feat["trip_count"].sum())))
        print(f"{qname}: {len(feat):,} feature rows, "
              f"{feat['pickup_zone_id'].nunique()} zones", flush=True)
        del feat

    print("\n" + "=" * 78)
    print("FULL YEAR 2024 - REFERENCE RUN")
    print("=" * 78)
    print(f"\nSource volume: {total_mb:,.0f} MiB across 12 monthly files\n")

    print(f"{'Month':<9}{'rows in':>14}{'quarantined':>14}{'dupes':>9}"
          f"{'rows out':>14}{'reject %':>10}")
    print("-" * 70)
    for mth, rin, quar, dup, out in month_rows:
        print(f"{mth:<9}{rin:>14,}{quar:>14,}{dup:>9,}{out:>14,}"
              f"{quar / rin * 100:>9.2f}%")
    print("-" * 70)
    print(f"{'TOTAL':<9}{total_in:>14,}{total_q:>14,}{'':>9}{total_out:>14,}"
          f"{total_q / total_in * 100:>9.2f}%")

    print(f"\n{'Rule':<24}{'rejections':>14}{'% of input':>12}")
    print("-" * 50)
    for name, n in sorted(rule_totals.items(), key=lambda kv: -kv[1]):
        print(f"{name:<24}{n:>14,}{n / total_in * 100:>11.2f}%")

    print(f"\n{'Quarter':<9}{'feature rows':>14}{'zones':>8}"
          f"{'trips covered':>16}  window span")
    print("-" * 78)
    tot_feat = 0
    for q, nrows, zones, w0, w1, trips in quarter_rows:
        tot_feat += nrows
        print(f"{q:<9}{nrows:>14,}{zones:>8}{trips:>16,}  {w0} .. {w1}")
    print("-" * 78)
    print(f"{'TOTAL':<9}{tot_feat:>14,}")

    print(f"\nCompression: {total_in:,} raw trips -> {tot_feat:,} feature rows "
          f"({total_in / tot_feat:.0f}x reduction)")
    print(f"Elapsed: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    sys.exit(main())
