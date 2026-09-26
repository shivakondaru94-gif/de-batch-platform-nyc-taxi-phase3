"""Shared Spark session + lake configuration for every job in the platform."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_session(app_name: str) -> SparkSession:
    """Spark session wired to the MinIO lake over S3A.

    Credentials come from the environment, never from source, so the same
    code runs against MinIO locally and against real S3 unchanged.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["LAKE_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["LAKE_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["LAKE_SECRET_KEY"])
        # MinIO speaks path-style addressing, not virtual-host style.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # Skip the _SUCCESS/_temporary dance that is slow on object stores.
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )
        # Pin the session zone so TIMESTAMP_NTZ -> TIMESTAMP casts are
        # deterministic on every machine and hourly windows never shift.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def warehouse_jdbc() -> tuple[str, dict[str, str]]:
    """JDBC URL and properties for the gold-layer serving warehouse."""
    url = (
        f"jdbc:postgresql://warehouse:5432/{os.environ['WAREHOUSE_DB']}"
    )
    props = {
        "user": os.environ["WAREHOUSE_USER"],
        "password": os.environ["WAREHOUSE_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    return url, props
