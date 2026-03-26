"""
Kafka Producer — Monthly CSV ingestion
Reads the Kaggle fraud CSV and publishes records to raw-transactions topic.
"""
import os, csv, json, time, logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC     = os.getenv("KAFKA_TOPIC", "raw-transactions")
CSV_PATH  = os.getenv("CSV_PATH", "/data/transactions.csv")
BATCH     = int(os.getenv("BATCH_SIZE", "1000"))

def wait_for_kafka(retries=15, delay=5):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                              value_serializer=lambda v: json.dumps(v).encode())
            log.info("Kafka connected.")
            return p
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retry {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries.")

def main():
    producer = wait_for_kafka()
    if not os.path.exists(CSV_PATH):
        log.error(f"Dataset not found at {CSV_PATH}. Place transactions.csv in ./data/")
        return

    sent = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, value=row)
            sent += 1
            if sent % BATCH == 0:
                producer.flush()
                log.info(f"Published {sent} records to {TOPIC}")

    producer.flush()
    log.info(f"Done. Total records published: {sent}")

if __name__ == "__main__":
    main()
