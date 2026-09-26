"""Silver -> Gold: aggregate a quarter of trips into ML-ready features.

Runs once per quarter, reads the three months of validated trips, and
produces one row per pickup zone per hour. The result is written both to
the gold bucket (Parquet, for reproducibility) and to the serving warehouse
(Postgres, for the machine learning application to query).

Usage:
    spark-submit silver_to_gold.py --quarter 2024Q1 --batch-id <id>
"""

from __future__ import annotations

import argparse

from common import build_session, warehouse_jdbc
from pyspark.sql import functions as F

QUARTER_MONTHS = {
    "Q1": ["01", "02", "03"],
    "Q2": ["04", "05", "06"],
    "Q3": ["07", "08", "09"],
    "Q4": ["10", "11", "12"],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quarter", required=True, help="e.g. 2024Q1")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    year, quarter = args.quarter[:4], args.quarter[4:]

    spark = build_session(f"silver_to_gold_{args.quarter}")
    spark.sparkContext.setLogLevel("WARN")

    # A quarter is aggregated from whichever of its months have actually been
    # validated. Reading a non-existent path fails the whole job, so absent
    # months are skipped explicitly and reported, which lets the pipeline be
    # exercised end to end from a single ingested month.
    # The filesystem must be resolved FROM the path: FileSystem.get(conf)
    # alone returns the default scheme (file:///) and then rejects s3a URIs.
    hadoop = spark.sparkContext._jvm.org.apache.hadoop
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    paths, missing = [], []
    for m in QUARTER_MONTHS[quarter]:
        path = f"s3a://silver/trips/year={year}/month={m}/"
        jpath = hadoop.fs.Path(path)
        if jpath.getFileSystem(conf).exists(jpath):
            paths.append(path)
        else:
            missing.append(m)

    if missing:
        print(f"WARNING no validated data for month(s): {', '.join(missing)}")
    if not paths:
        raise SystemExit(
            f"no validated data for any month of {args.quarter}; "
            "run the validation job first"
        )

    trips = spark.read.parquet(*paths)

    # Hourly tumbling windows keyed by pickup zone: the grain the demand
    # forecasting model is trained on.
    features = (
        trips.withColumn("window_start", F.date_trunc("hour", "pickup_ts"))
        .groupBy("pickup_zone_id", "window_start")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(
                F.percentile_approx("duration_min", 0.9), 2
            ).alias("p90_duration_min"),
        )
        # Calendar features the model would otherwise have to derive itself.
        .withColumn("hour_of_day", F.hour("window_start"))
        .withColumn("day_of_week", F.dayofweek("window_start"))
        .withColumn("is_weekend", F.dayofweek("window_start").isin(1, 7))
        .withColumn("batch_id", F.lit(args.batch_id))
        .withColumn("generated_at", F.current_timestamp())
    )

    features.cache()
    rows_out = features.count()

    # Gold lake copy: the immutable, versioned record of what was served.
    (
        features.write.mode("overwrite")
        .partitionBy("pickup_zone_id")
        .parquet(f"s3a://gold/features_demand_hourly/quarter={args.quarter}/")
    )

    # Serving copy. `append` keeps history across quarters, but a re-run of
    # the same quarter must replace its rows rather than collide with the
    # primary key, so the quarter's window range is cleared first. The whole
    # load is therefore idempotent: running it twice leaves one copy.
    url, props = warehouse_jdbc()
    months = QUARTER_MONTHS[quarter]
    q_start = f"{year}-{months[0]}-01"
    end_month = int(months[-1]) + 1
    q_end = (f"{int(year) + 1}-01-01" if end_month == 13
             else f"{year}-{end_month:02d}-01")

    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        url, props["user"], props["password"]
    )
    try:
        removed = conn.createStatement().executeUpdate(
            "DELETE FROM ml.features_demand_hourly "
            f"WHERE window_start >= '{q_start}' AND window_start < '{q_end}'"
        )
        print(f"removed {removed} previously delivered rows for {args.quarter}")
    finally:
        conn.close()

    (
        features.write.mode("append")
        .option("batchsize", 10_000)
        .jdbc(url, "ml.features_demand_hourly", properties=props)
    )

    print(f"BATCH_METRICS batch_id={args.batch_id} rows_out={rows_out}")
    spark.stop()


if __name__ == "__main__":
    main()
