-- fraud_db schema initialisation
-- Runs automatically on first container start

-- ── RAW DATA LAKE ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    trans_date_trans_time TIMESTAMP NOT NULL,
    cc_num              TEXT,
    merchant            TEXT,
    category            TEXT,
    amt                 NUMERIC(12, 2),
    first               TEXT,
    last                TEXT,
    gender              TEXT,
    street              TEXT,
    city                TEXT,
    state               TEXT,
    zip                 TEXT,
    lat                 DOUBLE PRECISION,
    long                DOUBLE PRECISION,
    city_pop            INTEGER,
    job                 TEXT,
    dob                 DATE,
    trans_num           TEXT UNIQUE,
    unix_time           BIGINT,
    merch_lat           DOUBLE PRECISION,
    merch_long          DOUBLE PRECISION,
    is_fraud            SMALLINT,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_trans_date  ON raw_transactions (trans_date_trans_time);
CREATE INDEX IF NOT EXISTS idx_raw_is_fraud    ON raw_transactions (is_fraud);
CREATE INDEX IF NOT EXISTS idx_raw_cc_num      ON raw_transactions (cc_num);

-- ── AGGREGATED FEATURE STORE ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS aggregated_features (
    id                  BIGSERIAL PRIMARY KEY,
    batch_quarter       TEXT NOT NULL,          -- e.g. '2024-Q1'
    run_timestamp       TIMESTAMP NOT NULL,
    category            TEXT,
    avg_amt             NUMERIC(12, 4),
    total_transactions  INTEGER,
    fraud_count         INTEGER,
    fraud_rate          NUMERIC(8, 6),
    avg_city_pop        NUMERIC(12, 2),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feat_quarter ON aggregated_features (batch_quarter);
