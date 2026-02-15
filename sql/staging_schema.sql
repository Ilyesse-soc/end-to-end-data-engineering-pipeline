\connect warehouse

CREATE SCHEMA IF NOT EXISTS staging;

ALTER SCHEMA staging OWNER TO warehouse;

CREATE TABLE IF NOT EXISTS staging.weather_hourly (
  batch_id UUID NOT NULL,
  city TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  temperature_c DOUBLE PRECISION NOT NULL,
  relative_humidity_pct INTEGER NOT NULL,
  precipitation_mm DOUBLE PRECISION NOT NULL,
  wind_speed_kmh DOUBLE PRECISION NOT NULL,
  source_ingested_at TIMESTAMPTZ NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT weather_hourly_pk PRIMARY KEY (city, ts_utc)
);

ALTER TABLE staging.weather_hourly OWNER TO warehouse;

CREATE TABLE IF NOT EXISTS staging.quality_check_results (
  check_run_id UUID PRIMARY KEY,
  batch_id UUID NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  parquet_path TEXT NOT NULL,
  status TEXT NOT NULL,
  total_rows BIGINT NOT NULL,
  duplicate_rows BIGINT NOT NULL,
  null_violations JSONB NOT NULL,
  range_violations JSONB NOT NULL,
  details JSONB
);

ALTER TABLE staging.quality_check_results OWNER TO warehouse;
