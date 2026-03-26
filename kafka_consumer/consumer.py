"""
Kafka Consumer — Writes raw transactions to PostgreSQL.
Commits offsets only after successful DB write (at-least-once delivery).
"""
import os, json, time, logging
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC     = os.getenv("KAFKA_TOPIC", "raw-transactions")
GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "fraud-consumer-group")
DB_HOST   = os.getenv("DB_HOST", "postgres")
DB_PORT   = int(os.getenv("DB_PORT", "5432"))
DB_NAME   = os.getenv("DB_NAME", "fraud_db")
DB_USER   = os.getenv("DB_USER", "fraud_user")
DB_PASS   = os.getenv("DB_PASS", "fraud_pass")

INSERT_SQL = """
INSERT INTO raw_transactions
  (trans_date_trans_time, cc_num, merchant, category, amt, first, last,
   gender, street, city, state, zip, lat, long, city_pop, job, dob,
   trans_num, unix_time, merch_lat, merch_long, is_fraud)
VALUES
  (%(trans_date_trans_time)s, %(cc_num)s, %(merchant)s, %(category)s, %(amt)s,
   %(first)s, %(last)s, %(gender)s, %(street)s, %(city)s, %(state)s, %(zip)s,
   %(lat)s, %(long)s, %(city_pop)s, %(job)s, %(dob)s, %(trans_num)s,
   %(unix_time)s, %(merch_lat)s, %(merch_long)s, %(is_fraud)s)
ON CONFLICT (trans_num) DO NOTHING;
"""

def safe_int(val):
    """Convert scientific notation strings like '3.56637E+15' to int safely."""
    try:
        return int(float(val)) if val not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None

def get_db():
    for _ in range(10):
        try:
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                    user=DB_USER, password=DB_PASS)
            log.info("PostgreSQL connected.")
            return conn
        except Exception as e:
            log.warning(f"DB not ready: {e}. Retrying...")
            time.sleep(5)
    raise RuntimeError("Could not connect to PostgreSQL.")

def get_consumer():
    for _ in range(15):
        try:
            c = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP,
                              group_id=GROUP_ID,
                              auto_offset_reset="earliest",
                              enable_auto_commit=False,
                              value_deserializer=lambda m: json.loads(m.decode()))
            log.info("Kafka consumer ready.")
            return c
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka.")

def main():
    conn = get_db()
    consumer = get_consumer()
    processed = 0

    for msg in consumer:
        row = msg.value
        try:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, {
                    "trans_date_trans_time": row.get("trans_date_trans_time"),
                    "cc_num":    safe_int(row.get("cc_num")),
                    "merchant":  row.get("merchant"),
                    "category":  row.get("category"),
                    "amt":       row.get("amt"),
                    "first":     row.get("first"),
                    "last":      row.get("last"),
                    "gender":    row.get("gender"),
                    "street":    row.get("street"),
                    "city":      row.get("city"),
                    "state":     row.get("state"),
                    "zip":       row.get("zip"),
                    "lat":       row.get("lat"),
                    "long":      row.get("long"),
                    "city_pop":  row.get("city_pop"),
                    "job":       row.get("job"),
                    "dob":       row.get("dob"),
                    "trans_num": row.get("trans_num"),
                    "unix_time": safe_int(row.get("unix_time")),
                    "merch_lat": row.get("merch_lat"),
                    "merch_long":row.get("merch_long"),
                    "is_fraud":  row.get("is_fraud"),
                })
            conn.commit()
            consumer.commit()   # Only commit offset after successful DB write
            processed += 1
            if processed % 1000 == 0:
                log.info(f"Consumed {processed} records.")
        except Exception as e:
            conn.rollback()
            log.error(f"Error inserting record: {e}")

if __name__ == "__main__":
    main()
