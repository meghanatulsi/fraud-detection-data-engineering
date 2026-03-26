-- DLMDSEDE02 | Schema Initialisation

CREATE TABLE IF NOT EXISTS raw_transactions (
    id                    SERIAL PRIMARY KEY,
    trans_date_trans_time TIMESTAMP,
    cc_num                BIGINT,
    merchant              TEXT,
    category              TEXT,
    amt                   NUMERIC(10,2),
    first                 TEXT,
    last                  TEXT,
    gender                CHAR(1),
    street                TEXT,
    city                  TEXT,
    state                 CHAR(2),
    zip                   TEXT,
    lat                   NUMERIC(9,6),
    long                  NUMERIC(9,6),
    city_pop              INTEGER,
    job                   TEXT,
    dob                   DATE,
    trans_num             TEXT UNIQUE,
    unix_time             BIGINT,
    merch_lat             NUMERIC(9,6),
    merch_long            NUMERIC(9,6),
    is_fraud              SMALLINT,
    ingested_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aggregated_features (
    id                      SERIAL PRIMARY KEY,
    quarter_label           TEXT NOT NULL,
    batch_run_ts            TIMESTAMP NOT NULL,
    category                TEXT,
    avg_amt                 NUMERIC(10,4),
    total_transactions      INTEGER,
    fraud_count             INTEGER,
    fraud_rate              NUMERIC(6,4),
    avg_velocity_per_card   NUMERIC(8,2),
    state                   CHAR(2)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'api_reader') THEN
    CREATE ROLE api_reader LOGIN PASSWORD 'api_reader_pass';
  END IF;
END$$;

GRANT CONNECT ON DATABASE fraud_db TO api_reader;
GRANT SELECT ON aggregated_features TO api_reader;
