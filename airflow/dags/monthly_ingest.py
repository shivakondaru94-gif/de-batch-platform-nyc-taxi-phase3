"""Monthly ingestion DAG: land one month of raw trip data, then validate it.

Schedule mirrors the business cadence stated in the task: data arrives
monthly, features are rebuilt quarterly. Catchup is enabled so a year of
history can be backfilled with a single command.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

DEFAULT_ARGS = {
    # Reliability: transient network/S3 errors retry with backoff rather
    # than failing the whole pipeline.
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "email_on_failure": False,
}


@dag(
    dag_id="monthly_ingest",
    description="NYC TLC monthly trip data -> bronze -> silver",
    schedule="0 3 5 * *",  # 5th of each month, after TLC publishes
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "batch"],
)
def monthly_ingest():

    @task
    def target_month(data_interval_start=None) -> str:
        """The month this run is responsible for, as YYYY-MM."""
        # A run executes at the END of its interval, so the run covering
        # [5 Jan, 5 Feb) fires on 5 Feb and owns January: the month of
        # data_interval_start itself. Subtracting a month here (as an earlier
        # version did) made the first run process December 2023, a month
        # before the platform's own start date - visible in the Spark UI as
        # bronze_to_silver_2023-12.
        return data_interval_start.format("YYYY-MM")

    @task
    def batch_id(month: str, run_id=None) -> str:
        """Stable identifier tying every artefact of this run together."""
        return f"ingest-{month}-{run_id}"

    @task
    def ingest(month: str) -> str:
        """Download + land in bronze. Idempotent: safe to retry."""
        import subprocess
        import sys

        subprocess.run(
            [sys.executable, "/opt/ingestion/ingest_tlc.py", "--month", month],
            check=True,
        )
        return month

    month = target_month()
    bid = batch_id(month)
    landed = ingest(month)

    validate = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/jobs/bronze_to_silver.py",
        py_files="/opt/jobs/common.py",
        conn_id="spark_default",
        application_args=[
            "--month", "{{ ti.xcom_pull(task_ids='target_month') }}",
            "--batch-id", "{{ ti.xcom_pull(task_ids='batch_id') }}",
        ],
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        verbose=False,
    )

    landed >> validate
    bid >> validate


monthly_ingest()
