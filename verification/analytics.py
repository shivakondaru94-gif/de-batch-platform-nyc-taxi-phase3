"""Extra full-year aggregates for the results chapter.

Uses exactly the same validation as the pipeline (full_year.validate), then
computes: hourly demand profile (weekday vs weekend), trips per day across the
year, and the busiest pickup zones. Output: analytics.json
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)

import pandas as pd
from full_year import validate

MONTHS = [f"{m:02d}" for m in range(1, 13)]

hour_counts = {"weekday": [0] * 24, "weekend": [0] * 24}
daily = {}
zones = {}
fare_by_hour_sum = [0.0] * 24
fare_by_hour_n = [0] * 24

for m in MONTHS:
    silver = validate(m)[0]
    ts = silver["pickup_ts"]
    wk = ts.dt.dayofweek >= 5                     # Sat/Sun
    hrs = ts.dt.hour
    for is_wkend, grp in silver.groupby(wk):
        c = grp["pickup_ts"].dt.hour.value_counts()
        key = "weekend" if is_wkend else "weekday"
        for h, n in c.items():
            hour_counts[key][int(h)] += int(n)
    for d, n in ts.dt.date.value_counts().items():
        daily[str(d)] = daily.get(str(d), 0) + int(n)
    for z, n in silver["pickup_zone_id"].value_counts().items():
        zones[int(z)] = zones.get(int(z), 0) + int(n)
    g = silver.groupby(hrs)["fare_amount"].agg(["sum", "count"])
    for h, row in g.iterrows():
        fare_by_hour_sum[int(h)] += float(row["sum"])
        fare_by_hour_n[int(h)] += int(row["count"])
    print("done", m, flush=True)

days = pd.to_datetime(pd.Series(sorted(daily)))
n_weekend_days = int((days.dt.dayofweek >= 5).sum())
n_weekday_days = len(days) - n_weekend_days

out = {
    "hour_avg_per_day": {
        "weekday": [c / n_weekday_days for c in hour_counts["weekday"]],
        "weekend": [c / n_weekend_days for c in hour_counts["weekend"]],
    },
    "n_weekday_days": n_weekday_days,
    "n_weekend_days": n_weekend_days,
    "daily": daily,
    "top_zones": sorted(zones.items(), key=lambda kv: -kv[1])[:10],
    "n_zones": len(zones),
    "avg_fare_by_hour": [s / n if n else 0 for s, n in zip(fare_by_hour_sum, fare_by_hour_n)],
}
with open("analytics.json", "w") as fh:
    json.dump(out, fh, indent=1, default=str)
print("wrote analytics.json")
