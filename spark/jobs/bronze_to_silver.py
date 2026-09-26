"""Bronze -> Silver: validate, clean and conform raw trip records.

Rows that violate an expectation are not dropped silently: they are written
to the quarantine bucket with the reason attached, so data loss is always
auditable. This is the data-quality gate of the platform.

Usage:
    spark-submit bronze_to_silver.py --month 2024-01 --batch-id <id>
"""

from __future__ import annotations

import argparse

from common import build_session
from pyspark.sql import functions as F

# Business rules for a plausible taxi trip. Anything outside these bounds is
# a sensor/meter error and would poison the model's features.
MIN_DISTANCE_MI = 0.1
MAX_DISTANCE_MI = 100.0
MIN_DURATION_MIN = 1.0
MAX_DURATION_MIN = 300.0
MAX_FARE = 1000.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    year, month = args.month.split("-")
    spark = build_session(f"bronze_to_silver_{args.month}")
    spark.sparkContext.setLogLevel("WARN")

    src = f"s3a://bronze/yellow_tripdata/year={year}/month={month}/"
    raw = spark.read.parquet(src)
    rows_in = raw.count()

    # The publisher stores timestamps as TIMESTAMP_NTZ (no time zone). Spark
    # refuses to cast that type to an integer, so both are normalised to a
    # plain TIMESTAMP first; with the session fixed to UTC in common.py the
    # wall-clock values are preserved exactly.
    trips = raw.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
    ).withColumn(
        "duration_min",
        (
            F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")
        )
        / 60.0,
    )

    # Each rule is named so the quarantine record explains *why* it failed.
    rules = {
        "null_timestamp": F.col("pickup_ts").isNull()
        | F.col("dropoff_ts").isNull(),
        "month_mismatch": (F.year("pickup_ts") != int(year))
        | (F.month("pickup_ts") != int(month)),
        "nonpositive_duration": F.col("duration_min") < MIN_DURATION_MIN,
        "implausible_duration": F.col("duration_min") > MAX_DURATION_MIN,
        "implausible_distance": (F.col("trip_distance") < MIN_DISTANCE_MI)
        | (F.col("trip_distance") > MAX_DISTANCE_MI),
        "negative_fare": F.col("fare_amount") < 0,
        "implausible_fare": F.col("fare_amount") > MAX_FARE,
        "invalid_zone": F.col("pickup_zone_id").isNull()
        | (F.col("pickup_zone_id") < 1)
        | (F.col("pickup_zone_id") > 265),
    }

    # Each rule contributes its name when it fires and NULL otherwise;
    # array_compact drops the NULLs. This must NOT be array_remove(arr, NULL):
    # under SQL semantics that returns NULL for every row, size(NULL) is -1,
    # and both filters below then exclude everything - the pipeline completes
    # "successfully" having written zero rows. That is precisely the silent
    # failure the post-load verification exists to catch, and it did.
    reason = F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    )
    tagged = trips.withColumn("reject_reasons", reason).cache()

    rejected = tagged.filter(F.size("reject_reasons") > 0)
    clean = tagged.filter(F.size("reject_reasons") == 0).drop("reject_reasons")

    rows_quarantined = rejected.count()
    rows_clean = clean.count()

    # Row conservation: every input row is either kept or quarantined. A
    # violation means a rule produced NULL instead of true/false and rows
    # vanished without being counted anywhere.
    if rows_clean + rows_quarantined != rows_in:
        raise SystemExit(
            f"row conservation violated: {rows_in} in, {rows_clean} clean, "
            f"{rows_quarantined} quarantined"
        )
    if rows_quarantined:
        (
            rejected.withColumn("batch_id", F.lit(args.batch_id))
            .write.mode("overwrite")
            .parquet(f"s3a://quarantine/year={year}/month={month}/")
        )

    # Deduplicate: a re-delivered source file must not double-count trips.
    silver = clean.dropDuplicates(
        ["pickup_ts", "dropoff_ts", "pickup_zone_id", "total_amount"]
    ).withColumn("batch_id", F.lit(args.batch_id))

    rows_out = silver.count()
    rows_deduplicated = rows_clean - rows_out

    (
        silver.repartition("pickup_zone_id")
        .write.mode("overwrite")
        .parquet(f"s3a://silver/trips/year={year}/month={month}/")
    )

    print(
        f"BATCH_METRICS batch_id={args.batch_id} rows_in={rows_in} "
        f"rows_out={rows_out} rows_quarantined={rows_quarantined} "
        f"rows_deduplicated={rows_deduplicated}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
