CREATE TABLE IF NOT EXISTS import_records (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    source_filename TEXT NOT NULL,
    source_line INTEGER NOT NULL,
    transaction_key TEXT,
    donor_identifier TEXT,
    amount NUMERIC(12, 2),
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);