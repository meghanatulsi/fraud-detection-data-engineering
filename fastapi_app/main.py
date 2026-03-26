"""
FastAPI — REST Data Delivery Service
Exposes read-only /features endpoint for the Fraud Detection ML application.
"""
from fastapi import FastAPI, Query, HTTPException
import psycopg2, os, logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(
    title="Fraud Detection Feature Store API",
    description="Read-only REST API for quarterly aggregated fraud features.",
    version="1.0.0"
)

DB_CONN = dict(
    host=os.getenv("DB_HOST", "postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "fraud_db"),
    user=os.getenv("DB_USER", "fraud_user"),
    password=os.getenv("DB_PASS", "fraud_pass")
)

def get_conn():
    return psycopg2.connect(**DB_CONN)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/features")
def get_features(
    quarter: Optional[str] = Query(None, description="e.g. 2024-Q1"),
    state: Optional[str] = Query(None, description="Two-letter US state code"),
    limit: int = Query(500, le=5000)
):
    """Return aggregated fraud features, optionally filtered by quarter and state."""
    where_clauses = []
    params = []
    if quarter:
        where_clauses.append("quarter_label = %s")
        params.append(quarter)
    if state:
        where_clauses.append("state = %s")
        params.append(state.upper())

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"""
        SELECT quarter_label, batch_run_ts, category, state,
               avg_amt, total_transactions, fraud_count,
               fraud_rate, avg_velocity_per_card
        FROM aggregated_features
        {where_sql}
        ORDER BY batch_run_ts DESC
        LIMIT %s;
    """
    params.append(limit)

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        return {"count": len(rows), "data": rows}
    except Exception as e:
        log.error(f"DB error: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

@app.get("/features/latest")
def get_latest_quarter():
    """Return the most recent batch run's aggregated features."""
    sql = """
        SELECT quarter_label, batch_run_ts, category, state,
               avg_amt, total_transactions, fraud_count,
               fraud_rate, avg_velocity_per_card
        FROM aggregated_features
        WHERE batch_run_ts = (SELECT MAX(batch_run_ts) FROM aggregated_features)
        ORDER BY category, state;
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        return {"quarter": rows[0]["quarter_label"] if rows else None,
                "count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
