CREATE DATABASE airflow;
CREATE DATABASE fraud_detection;

\connect fraud_detection

CREATE TABLE IF NOT EXISTS labeled_transactions (
    transaction_id UUID PRIMARY KEY,
    source_key TEXT NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    name_1 TEXT,
    name_2 TEXT,
    gender TEXT,
    street TEXT,
    one_city TEXT,
    us_state TEXT NOT NULL,
    post_code TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    population_city DOUBLE PRECISION,
    jobs TEXT,
    merchant_lat DOUBLE PRECISION,
    merchant_lon DOUBLE PRECISION,
    target SMALLINT NOT NULL CHECK (target IN (0, 1)),
    batch_month DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_key, batch_month)
);

CREATE INDEX IF NOT EXISTS idx_labeled_transactions_month
    ON labeled_transactions (batch_month, transaction_time);

CREATE TABLE IF NOT EXISTS pipeline_state (
    state_id SMALLINT PRIMARY KEY CHECK (state_id = 1),
    last_trained_month DATE NOT NULL,
    model_version TEXT NOT NULL,
    validation_roc_auc DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id UUID PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0.0 AND 1.0),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    us_state TEXT NOT NULL DEFAULT '',
    merch TEXT NOT NULL DEFAULT '',
    cat_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_created_at
    ON transaction_scores (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_dimensions
    ON transaction_scores (us_state, merch, created_at DESC);
