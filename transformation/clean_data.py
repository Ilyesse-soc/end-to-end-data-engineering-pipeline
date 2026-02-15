import logging
import logging.config
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import yaml


LOGGER_NAME = "pipeline.transformation"


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def _setup_logging() -> None:
    config_path = os.getenv("LOG_CONFIG_PATH")
    if not config_path or not os.path.exists(config_path):
        logging.basicConfig(level=logging.INFO)
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)


def _warehouse_config_from_env() -> WarehouseConfig:
    return WarehouseConfig(
        host=os.getenv("WAREHOUSE_HOST", "postgres"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
        user=os.getenv("WAREHOUSE_USER", "warehouse"),
        password=os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
    )


def _connect_warehouse(cfg: WarehouseConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )


def _flatten_open_meteo_payload(city: str, lat: float, lon: float, ingested_at: datetime, payload: dict[str, Any]) -> pl.DataFrame:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []

    df = pl.DataFrame(
        {
            "city": [city] * len(times),
            "latitude": [lat] * len(times),
            "longitude": [lon] * len(times),
            "ts_utc": times,
            "temperature_c": hourly.get("temperature_2m") or [],
            "relative_humidity_pct": hourly.get("relative_humidity_2m") or [],
            "precipitation_mm": hourly.get("precipitation") or [],
            "wind_speed_kmh": hourly.get("wind_speed_10m") or [],
            "source_ingested_at": [ingested_at.isoformat()] * len(times),
        }
    )

    df = df.with_columns(
        pl.col("ts_utc").cast(pl.Utf8).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M", strict=False),
        pl.col("source_ingested_at").cast(pl.Utf8).str.strptime(pl.Datetime, strict=False),
        pl.col("temperature_c").cast(pl.Float64, strict=False),
        pl.col("relative_humidity_pct").cast(pl.Int64, strict=False),
        pl.col("precipitation_mm").cast(pl.Float64, strict=False),
        pl.col("wind_speed_kmh").cast(pl.Float64, strict=False),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("city").cast(pl.Utf8),
    )

    return df


def transform(batch_id: str, output_dir: str = "/opt/airflow/data/transformed") -> str:
    _setup_logging()
    log = logging.getLogger(LOGGER_NAME)

    cfg = _warehouse_config_from_env()

    log.info("Transform start batch_id=%s", batch_id)

    frames: list[pl.DataFrame] = []

    with _connect_warehouse(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT city, latitude, longitude, ingested_at, http_status, payload
                FROM raw.open_meteo_responses
                WHERE batch_id = %s
                ORDER BY city
                """,
                (batch_id,),
            )

            rows = cur.fetchall()

    for city, lat, lon, ingested_at, http_status, payload in rows:
        if int(http_status) != 200 or payload is None:
            continue

        try:
            df = _flatten_open_meteo_payload(city, float(lat), float(lon), ingested_at, payload)
            frames.append(df)
        except Exception as e:
            log.exception("Failed to flatten payload for city=%s: %s", city, e)
            raise

    if not frames:
        raise RuntimeError("No successful payloads to transform")

    df_all = pl.concat(frames, how="vertical")

    df_all = (
        df_all
        .with_columns(
            pl.lit(batch_id).cast(pl.Utf8).alias("batch_id"),
        )
        .select(
            "batch_id",
            "city",
            "latitude",
            "longitude",
            "ts_utc",
            "temperature_c",
            "relative_humidity_pct",
            "precipitation_mm",
            "wind_speed_kmh",
            "source_ingested_at",
        )
        .drop_nulls()
        .unique(subset=["city", "ts_utc"], keep="first")
        .sort(["city", "ts_utc"])
    )

    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, f"weather_hourly_{batch_id}.parquet")
    df_all.write_parquet(parquet_path)

    log.info("Transform done batch_id=%s rows=%s parquet=%s", batch_id, df_all.height, parquet_path)
    return parquet_path


def load(parquet_path: str) -> int:
    _setup_logging()
    log = logging.getLogger(LOGGER_NAME)

    cfg = _warehouse_config_from_env()

    if not os.path.exists(parquet_path):
        raise FileNotFoundError(parquet_path)

    df = pl.read_parquet(parquet_path)

    required_cols = {
        "batch_id",
        "city",
        "latitude",
        "longitude",
        "ts_utc",
        "temperature_c",
        "relative_humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
        "source_ingested_at",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Parquet is missing columns: {sorted(missing)}")

    df = df.with_columns(
        pl.col("relative_humidity_pct").cast(pl.Int64, strict=False),
        pl.col("ts_utc").cast(pl.Datetime, strict=False),
        pl.col("source_ingested_at").cast(pl.Datetime, strict=False),
    )

    def _to_utc(dt: Any) -> datetime:
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        raise TypeError(f"Unexpected datetime type: {type(dt)}")

    records = []
    for row in df.iter_rows(named=True):
        records.append(
            (
                str(row["batch_id"]),
                row["city"],
                float(row["latitude"]),
                float(row["longitude"]),
                _to_utc(row["ts_utc"]),
                float(row["temperature_c"]),
                int(row["relative_humidity_pct"]),
                float(row["precipitation_mm"]),
                float(row["wind_speed_kmh"]),
                _to_utc(row["source_ingested_at"]),
            )
        )

    if not records:
        raise RuntimeError("No records to load")

    upsert_sql = """
        INSERT INTO staging.weather_hourly(
          batch_id, city, latitude, longitude, ts_utc,
          temperature_c, relative_humidity_pct, precipitation_mm, wind_speed_kmh,
          source_ingested_at
        ) VALUES %s
        ON CONFLICT (city, ts_utc) DO UPDATE SET
          batch_id = EXCLUDED.batch_id,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          temperature_c = EXCLUDED.temperature_c,
          relative_humidity_pct = EXCLUDED.relative_humidity_pct,
          precipitation_mm = EXCLUDED.precipitation_mm,
          wind_speed_kmh = EXCLUDED.wind_speed_kmh,
          source_ingested_at = EXCLUDED.source_ingested_at,
          loaded_at = NOW()
    """

    with _connect_warehouse(cfg) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, records, page_size=1000)
        conn.commit()

    log.info("Loaded records=%s parquet=%s", len(records), parquet_path)
    return len(records)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["transform", "load"], required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--parquet-path")
    args = parser.parse_args()

    if args.mode == "transform":
        if not args.batch_id:
            raise SystemExit("--batch-id is required for transform")
        print(transform(args.batch_id))
    else:
        if not args.parquet_path:
            raise SystemExit("--parquet-path is required for load")
        print(load(args.parquet_path))
