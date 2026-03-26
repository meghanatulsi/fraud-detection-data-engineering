"""
Spark Batch Job — Quarterly Feature Engineering
================================================
Triggered by Airflow DAG on cron: 0 0 1 1,4,7,10 *

Reads raw_transactions from PostgreSQL, computes fraud-relevant
aggregations, and writes versioned snapshots to aggregated_features.

Features produced per (batch_quarter, category):
  - avg_amt           : average transaction amount
  - total_transactions: count of transactions
  - fraud_count       : number of fraudulent transactions
  - fraud_rate        : fraud_count / total_transactions
  - avg_city_pop      : average city population (proxy for geography)
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://fraud_user:fraud_pass@postgres:5432/fraud_db"
)
# Convert DSN to JDBC URL
JDBC_URL = POSTGRES_DSN.replace("postgresql://", "jdbc:postgresql://")
DB_PROPS = {
    "user":   os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_pass"),
    "driver": "org.postgresql.Driver",
}

# Quarter label derived from current date
now           = datetime.utcnow()
QUARTER       = f"{now.year}-Q{(now.month - 1) // 3 + 1}"
RUN_TIMESTAMP = now.isoformat()


def compute_quarter_bounds(quarter: str):
    """Return (start_date, end_date) for the given quarter string."""
    year, q = quarter.split("-Q")
    year = int(year)
    q    = int(q)
    month_start = (q - 1) * 3 + 1
    month_end   = month_start + 2
    import calendar
    last_day = calendar.monthrange(year, month_end)[1]
    return (
        datetime(year, month_start, 1),
        datetime(year, month_end, last_day, 23, 59, 59),
    )


def main():
    log.info("Spark Batch Job starting for quarter: %s", QUARTER)

    spark = (
        SparkSession.builder
        .appName(f"FraudFeatureEngineering_{QUARTER}")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read raw data from PostgreSQL ─────────────────────────────────────
    log.info("Reading raw_transactions from PostgreSQL…")
    raw_df = (
        spark.read
        .jdbc(url=JDBC_URL, table="raw_transactions", properties=DB_PROPS)
    )

    # ── 2. Filter to current quarter ─────────────────────────────────────────
    start_dt, end_dt = compute_quarter_bounds(QUARTER)
    log.info("Filtering to %s → %s", start_dt, end_dt)
    quarter_df = raw_df.filter(
        (F.col("trans_date_trans_time") >= F.lit(start_dt)) &
        (F.col("trans_date_trans_time") <= F.lit(end_dt))
    )
    log.info("Rows in quarter: %d", quarter_df.count())

    # ── 3. Feature engineering ───────────────────────────────────────────────
    features_df = (
        quarter_df
        .groupBy("category")
        .agg(
            F.round(F.avg("amt"),       4).alias("avg_amt"),
            F.count("*")                  .alias("total_transactions"),
            F.sum("is_fraud")             .alias("fraud_count"),
            F.round(
                F.sum("is_fraud") / F.count("*"), 6
            ).alias("fraud_rate"),
            F.round(F.avg("city_pop"),  2).alias("avg_city_pop"),
        )
        .withColumn("batch_quarter",  F.lit(QUARTER))
        .withColumn("run_timestamp",  F.lit(RUN_TIMESTAMP).cast("timestamp"))
    )

    features_df.show(20, truncate=False)

    # ── 4. Write to aggregated_features (append = versioned snapshots) ────────
    log.info("Writing aggregated features to PostgreSQL…")
    (
        features_df
        .select(
            "batch_quarter", "run_timestamp", "category",
            "avg_amt", "total_transactions", "fraud_count",
            "fraud_rate", "avg_city_pop"
        )
        .write
        .jdbc(url=JDBC_URL, table="aggregated_features",
              mode="append", properties=DB_PROPS)
    )

    log.info("✅  Feature engineering complete for %s", QUARTER)
    spark.stop()


if __name__ == "__main__":
    main()
