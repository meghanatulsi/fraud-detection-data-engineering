"""
Airflow DAG — Fraud Detection Batch Pipeline
=============================================

Two schedules in one DAG file:

  1. @monthly   → trigger Kafka Producer (ingest monthly CSV slice)
  2. quarterly  → trigger Spark feature engineering
                  cron: 0 0 1 1,4,7,10 *  (Jan/Apr/Jul/Oct)

Retries: 3 attempts, 5-minute back-off between retries.
Alerts : on_failure_callback logs error (hook up email/Slack in production).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

SPARK_HOST  = "spark"
SPARK_JOB   = "/opt/spark_jobs/feature_engineering.py"

DEFAULT_ARGS = {
    "owner":            "fraud_pipeline",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,   # set to True + configure SMTP in production
}


# ── DAG 1 : Monthly Ingestion ─────────────────────────────────────────────────
with DAG(
    dag_id="monthly_ingestion",
    default_args=DEFAULT_ARGS,
    description="Monthly: run Kafka producer to ingest CSV slice into raw_transactions",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "kafka"],
) as monthly_dag:

    wait_for_kafka = BashOperator(
        task_id="wait_for_kafka",
        bash_command=(
            "until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; "
            "do echo 'Waiting for Kafka…'; sleep 5; done"
        ),
    )

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="docker exec kafka_producer python /app/producer.py",
    )

    wait_for_kafka >> run_producer


# ── DAG 2 : Quarterly Batch Processing ────────────────────────────────────────
with DAG(
    dag_id="quarterly_feature_engineering",
    default_args=DEFAULT_ARGS,
    description="Quarterly: Spark reads raw_transactions, writes aggregated_features",
    schedule_interval="0 0 1 1,4,7,10 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["processing", "spark", "features"],
) as quarterly_dag:

    def log_start(**context):
        log.info(
            "Quarterly Spark job starting. Execution date: %s",
            context["execution_date"]
        )

    notify_start = PythonOperator(
        task_id="notify_start",
        python_callable=log_start,
    )

    run_spark_job = BashOperator(
        task_id="run_spark_feature_engineering",
        bash_command=(
            f"docker exec spark spark-submit "
            f"--master local[*] "
            f"--packages org.postgresql:postgresql:42.6.0 "
            f"{SPARK_JOB}"
        ),
        execution_timeout=timedelta(hours=2),
    )

    def log_complete(**context):
        log.info("Quarterly feature engineering complete. FastAPI now serves updated features.")

    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=log_complete,
    )

    notify_start >> run_spark_job >> notify_complete
