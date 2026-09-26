"""Monthly ingestion microservice: NYC TLC trip records -> bronze lake.

Downloads one month of Parquet trip data and lands it, byte-for-byte
unmodified, in the bronze bucket. Bronze is immutable: nothing is cleaned,
filtered or reshaped here, so a bad transformation downstream can always be
replayed from the original source.

Usage:
    python ingest_tlc.py --month 2024-01
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import tempfile

import boto3
import requests
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest")

CHUNK = 8 * 1024 * 1024  # 8 MiB streaming chunks; the file never sits in RAM


def s3_client():
    """S3 client pointed at MinIO. Same API as real AWS S3."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["LAKE_ENDPOINT"],
        aws_access_key_id=os.environ["LAKE_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LAKE_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def already_ingested(s3, bucket: str, key: str) -> bool:
    """Idempotency guard: a re-run of the same month is a no-op, not a duplicate."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def download(url: str, dest: str) -> str:
    """Stream the source file to disk, returning its SHA-256 for the lineage log."""
    digest = hashlib.sha256()
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)
                digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    dataset = os.environ.get("TLC_DATASET", "yellow_tripdata")
    base = os.environ.get(
        "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    filename = f"{dataset}_{args.month}.parquet"
    url = f"{base}/{filename}"

    year, month = args.month.split("-")
    # Hive-style partitioning: Spark prunes whole directories at read time.
    key = f"{dataset}/year={year}/month={month}/{filename}"

    s3 = s3_client()
    if already_ingested(s3, "bronze", key) and not args.overwrite:
        log.info("s3a://bronze/%s already present - skipping", key)
        return 0

    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        log.info("downloading %s", url)
        checksum = download(url, local)
        size_mb = os.path.getsize(local) / 1024 / 1024
        log.info("downloaded %.1f MiB, sha256=%s", size_mb, checksum[:16])

        s3.upload_file(
            local,
            "bronze",
            key,
            ExtraArgs={
                # Provenance travels with the object itself.
                "Metadata": {
                    "source-url": url,
                    "sha256": checksum,
                    "ingest-month": args.month,
                }
            },
        )

    log.info("landed s3a://bronze/%s", key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
