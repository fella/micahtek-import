CREATE TABLE IF NOT EXISTS import_runs (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    source_filename TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    records_found INTEGER NOT NULL DEFAULT 0,
    records_succeeded INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);