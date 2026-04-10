CREATE TABLE IF NOT EXISTS idempotency_keys (
    transaction_key TEXT PRIMARY KEY,
    source_filename TEXT NOT NULL,
    run_id UUID NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);