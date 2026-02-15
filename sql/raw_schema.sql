\connect warehouse

CREATE SCHEMA IF NOT EXISTS raw;

ALTER SCHEMA raw OWNER TO warehouse;

CREATE TABLE IF NOT EXISTS raw.ingestion_batches (
  batch_id UUID PRIMARY KEY,
  source TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  requested_start DATE NOT NULL,
  requested_end DATE NOT NULL,
  locations JSONB NOT NULL,
  status TEXT NOT NULL,
  http_success_count INTEGER NOT NULL DEFAULT 0,
  http_failure_count INTEGER NOT NULL DEFAULT 0,
  total_payload_bytes BIGINT NOT NULL DEFAULT 0
);

ALTER TABLE raw.ingestion_batches OWNER TO warehouse;

CREATE TABLE IF NOT EXISTS raw.open_meteo_responses (
  ingestion_id UUID PRIMARY KEY,
  batch_id UUID NOT NULL REFERENCES raw.ingestion_batches(batch_id),
  ingested_at TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  city TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  requested_start DATE NOT NULL,
  requested_end DATE NOT NULL,
  http_status INTEGER NOT NULL,
  payload JSONB,
  payload_bytes INTEGER NOT NULL
);

ALTER TABLE raw.open_meteo_responses OWNER TO warehouse;

CREATE INDEX IF NOT EXISTS idx_open_meteo_responses_batch_id
  ON raw.open_meteo_responses(batch_id);
