"""
Spark Batch Job — Quarterly Feature Engineering & Aggregation
Triggered by Airflow DAG on: 0 0 1 1,4,7,10 *
Reads raw_transactions, computes fraud features, writes aggregated_features.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os, logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_HOST  = os.getenv("DB_HOST", "postgres")
DB_PORT  = os.getenv("DB_PORT", "5432")
DB_NAME  = os.getenv("DB_NAME", "fraud_db")
DB_USER  = os.getenv("DB_USER", "fraud_user")
DB_PASS  = os.getenv("DB_PASS", "fraud_pass")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

def get_quarter_label():
    now = datetime.utcnow()
    q = (now.month - 1) // 3 + 1
    return f"{now.year}-Q{q}"

def main():
    spark = (SparkSession.builder
             .appName("FraudFeatureEngineering")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading raw_transactions from PostgreSQL...")
    df = spark.read.jdbc(JDBC_URL, "raw_transactions", properties=JDBC_PROPS)
    log.info(f"Loaded {df.count()} raw records.")

    # ── Feature 1: Fraud rate and avg amount by category & state ──
    agg = (df.groupBy("category", "state")
             .agg(
                 F.count("*").alias("total_transactions"),
                 F.round(F.avg("amt"), 4).alias("avg_amt"),
                 F.sum("is_fraud").alias("fraud_count"),
                 F.round(F.avg("is_fraud"), 4).alias("fraud_rate")
             ))

    # ── Feature 2: Transaction velocity per card (count of txns per cc_num) ──
    velocity = (df.groupBy("cc_num")
                  .agg(F.count("*").alias("card_txn_count")))
    avg_velocity = velocity.agg(
        F.round(F.avg("card_txn_count"), 2).alias("avg_velocity_per_card")
    ).collect()[0]["avg_velocity_per_card"]

    quarter_label = get_quarter_label()
    batch_run_ts  = datetime.utcnow().isoformat()

    result = agg.withColumn("quarter_label", F.lit(quarter_label)) \
                .withColumn("batch_run_ts", F.lit(batch_run_ts).cast("timestamp")) \
                .withColumn("avg_velocity_per_card", F.lit(float(avg_velocity)))

    log.info(f"Writing {result.count()} aggregated rows for {quarter_label}...")
    result.select(
        "quarter_label", "batch_run_ts", "category",
        "avg_amt", "total_transactions", "fraud_count",
        "fraud_rate", "avg_velocity_per_card", "state"
    ).write.jdbc(JDBC_URL, "aggregated_features", mode="append", properties=JDBC_PROPS)

    log.info("Batch job complete.")
    spark.stop()

if __name__ == "__main__":
    main()
