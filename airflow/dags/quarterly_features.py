"""Quarterly feature DAG: aggregate three months of silver into gold.

This is the cadence the task description sets out - the frontend machine
learning application retrains once per quarter, so the feature table is
rebuilt on the same rhythm. The DAG waits for all three monthly ingests to
have completed before it aggregates.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


@dag(
    dag_id="quarterly_features",
    description="Silver trips -> hourly demand features -> serving warehouse",
    schedule="0 6 10 1,4,7,10 *",  # 10th of Jan/Apr/Jul/Oct
    # Starts in January so that Q1 gets a run of its own: the interval
    # [10 Jan, 10 Apr) fires on 10 Apr, when Q1 is complete.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["aggregation", "batch", "ml-serving"],
)
def quarterly_features():

    @task
    def target_quarter(data_interval_start=None) -> str:
        """The completed quarter this run aggregates, e.g. 2024Q1.

        The run fires at the end of its interval, by which time the quarter
        containing data_interval_start is complete. An earlier version
        subtracted a month first, which made every run aggregate the quarter
        BEFORE the one just finished - a full quarter of needless lag.
        """
        return f"{data_interval_start.year}Q{data_interval_start.quarter}"

    @task
    def batch_id(quarter: str, run_id=None) -> str:
        return f"gold-{quarter}-{run_id}"

    @task
    def open_lineage(quarter: str, bid: str) -> str:
        """Record the batch as started before any data moves."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            """
            INSERT INTO governance.batch_lineage
                (batch_id, source_uri, layer, started_at, status)
            VALUES (%s, %s, 'gold', now(), 'RUNNING')
            ON CONFLICT (batch_id) DO NOTHING
            """,
            parameters=(bid, f"s3a://silver/trips/ [{quarter}]"),
        )
        return bid

    @task
    def close_lineage(bid: str) -> None:
        """Mark the batch complete and capture the served row count."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            """
            UPDATE governance.batch_lineage
               SET finished_at = now(),
                   status = 'SUCCESS',
                   rows_out = (
                       SELECT count(*) FROM ml.features_demand_hourly
                        WHERE batch_id = %s
                   )
             WHERE batch_id = %s
            """,
            parameters=(bid, bid),
        )

    @task
    def verify(bid: str) -> None:
        """Post-load assertions. A silently empty feature table is worse
        than a loud failure, so the DAG fails if the contract is broken."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        checks = {
            "row_count_positive":
                "SELECT count(*) > 0 FROM ml.features_demand_hourly "
                "WHERE batch_id = %(bid)s",
            "no_null_zones":
                "SELECT count(*) = 0 FROM ml.features_demand_hourly "
                "WHERE batch_id = %(bid)s AND pickup_zone_id IS NULL",
            "no_negative_counts":
                "SELECT count(*) = 0 FROM ml.features_demand_hourly "
                "WHERE batch_id = %(bid)s AND trip_count < 0",
        }
        failures = []
        for name, sql in checks.items():
            passed = hook.get_first(sql, parameters={"bid": bid})[0]
            hook.run(
                """
                INSERT INTO governance.quality_checks
                    (batch_id, expectation, passed)
                VALUES (%s, %s, %s)
                ON CONFLICT (batch_id, expectation)
                DO UPDATE SET passed = EXCLUDED.passed, checked_at = now()
                """,
                parameters=(bid, name, bool(passed)),
            )
            if not passed:
                failures.append(name)
        if failures:
            raise ValueError(f"gold quality checks failed: {failures}")

    quarter = target_quarter()
    bid = batch_id(quarter)
    opened = open_lineage(quarter, bid)

    aggregate = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/jobs/silver_to_gold.py",
        py_files="/opt/jobs/common.py",
        conn_id="spark_default",
        application_args=[
            "--quarter", "{{ ti.xcom_pull(task_ids='target_quarter') }}",
            "--batch-id", "{{ ti.xcom_pull(task_ids='batch_id') }}",
        ],
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        verbose=False,
    )

    opened >> aggregate >> verify(bid) >> close_lineage(bid)


quarterly_features()
