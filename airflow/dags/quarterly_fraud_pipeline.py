"""
Airflow DAG — Quarterly Fraud Feature Engineering Pipeline
Schedule: 0 0 1 1,4,7,10 *  (Jan/Apr/Jul/Oct 1st at midnight)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2, os, logging

log = logging.getLogger(__name__)

default_args = {
    "owner": "meghana_tulsi",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["meghana@example.com"],
}

dag = DAG(
    dag_id="quarterly_fraud_feature_pipeline",
    default_args=default_args,
    description="Quarterly batch: Spark feature engineering for fraud ML model",
    schedule_interval="0 0 1 1,4,7,10 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "batch", "quarterly"],
)

def check_raw_data(**ctx):
    """Verify raw_transactions has data before triggering Spark."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        dbname=os.getenv("DB_NAME", "fraud_db"),
        user=os.getenv("DB_USER", "fraud_user"),
        password=os.getenv("DB_PASS", "fraud_pass")
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_transactions;")
        count = cur.fetchone()[0]
    conn.close()
    log.info(f"Raw transaction count: {count}")
    if count == 0:
        raise ValueError("raw_transactions is empty — aborting pipeline.")
    return count

check_data = PythonOperator(
    task_id="check_raw_data_exists",
    python_callable=check_raw_data,
    dag=dag,
)

run_spark = BashOperator(
    task_id="run_spark_batch_job",
    bash_command=(
        "docker exec spark spark-submit "
        "--packages org.postgresql:postgresql:42.6.0 "
        "/opt/spark_jobs/batch_feature_engineering.py"
    ),
    dag=dag,
)

def verify_output(**ctx):
    """Confirm Spark wrote at least one aggregated row for this quarter."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        dbname=os.getenv("DB_NAME", "fraud_db"),
        user=os.getenv("DB_USER", "fraud_user"),
        password=os.getenv("DB_PASS", "fraud_pass")
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM aggregated_features WHERE batch_run_ts >= NOW() - INTERVAL '1 day';")
        count = cur.fetchone()[0]
    conn.close()
    if count == 0:
        raise ValueError("No aggregated features written — Spark job may have failed.")
    log.info(f"Verified {count} aggregated rows written.")

verify = PythonOperator(
    task_id="verify_aggregated_output",
    python_callable=verify_output,
    dag=dag,
)

check_data >> run_spark >> verify
