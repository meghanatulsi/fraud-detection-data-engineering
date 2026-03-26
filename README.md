# DLMDSEDE02 — Batch-Processing Data Architecture
## Financial Transactions & Fraud Detection
**Author:** Meghana Tulsi | **Matriculation:** 10249575

---

## Prerequisites
- Docker Desktop (≥ 4.x) with at least 8 GB RAM allocated
- Docker Compose v2
- VS Code (optional, recommended)
- Git

---

## Dataset Setup
1. Download the dataset from Kaggle:
   **Credit Card Transactions Fraud Detection Dataset** (Sparkov / CC0 license)
   https://www.kaggle.com/datasets/kartik2112/fraud-detection
2. Place the CSV file at:
   ```
   fraud-detection-pipeline/data/transactions.csv
   ```

---

## Running the System

### Clone & Start
```bash
git clone https://github.com/YOUR_USERNAME/dlmdsede02-fraud-pipeline.git
cd dlmdsede02-fraud-pipeline
docker-compose up --build
```

### Service URLs
| Service | URL |
|---------|-----|
| FastAPI (features endpoint) | http://localhost:8000/docs |
| Airflow (DAG dashboard) | http://localhost:8081 (admin/admin) |
| Spark Master UI | http://localhost:8080 |

### Running the Spark Job Manually
```bash
docker exec spark spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /opt/spark_jobs/batch_feature_engineering.py
```

### Test the API
```bash
# Health check
curl http://localhost:8000/health

# Get latest quarterly features
curl http://localhost:8000/features/latest

# Filter by quarter and state
curl "http://localhost:8000/features?quarter=2024-Q1&state=CA"
```

### Stop All Services
```bash
docker-compose down
# To also remove volumes (wipes DB data):
docker-compose down -v
```

---

## Architecture Summary
```
[Kaggle CSV] → [Kafka Producer] → [Kafka Topic: raw-transactions]
                                         ↓
                                  [Kafka Consumer]
                                         ↓
                               [PostgreSQL: raw_transactions]
                                         ↓ (quarterly, Airflow trigger)
                                   [Apache Spark]
                                         ↓
                           [PostgreSQL: aggregated_features]
                                         ↓
                                  [FastAPI /features]
                                         ↓
                           [Fraud Detection ML Application]
```

---

## Project Structure
```
fraud-detection-pipeline/
├── docker-compose.yml          # Infrastructure as Code
├── README.md
├── data/                       # Place transactions.csv here
├── postgres/
│   └── init/01_schema.sql      # DB schema (auto-runs on first start)
├── kafka_producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── kafka_consumer/
│   ├── consumer.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark_jobs/
│   └── batch_feature_engineering.py
├── airflow/
│   └── dags/
│       └── quarterly_fraud_pipeline.py
└── fastapi_app/
    ├── main.py
    ├── requirements.txt
    └── Dockerfile
```
