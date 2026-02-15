import json
import logging
import logging.config
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml


LOGGER_NAME = "pipeline.ingestion"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


DEFAULT_LOCATIONS = [
    {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]


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


def _requests_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _parse_locations() -> list[dict[str, Any]]:
    raw = os.getenv("PIPELINE_LOCATIONS")
    if not raw:
        return DEFAULT_LOCATIONS

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError("PIPELINE_LOCATIONS must be valid JSON") from e

    if not isinstance(parsed, list) or not parsed:
        raise ValueError("PIPELINE_LOCATIONS must be a non-empty JSON list")

    required = {"city", "latitude", "longitude"}
    for idx, item in enumerate(parsed):
        if not isinstance(item, dict) or not required.issubset(item.keys()):
            raise ValueError(f"PIPELINE_LOCATIONS[{idx}] must contain {sorted(required)}")

    return parsed


def _requested_date_range() -> tuple[date, date]:
    past_days = int(os.getenv("PIPELINE_PAST_DAYS", "7"))
    if past_days <= 0 or past_days > 120:
        raise ValueError("PIPELINE_PAST_DAYS must be between 1 and 120")

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=past_days - 1)
    return start, end


def run_ingestion() -> str:
    _setup_logging()
    log = logging.getLogger(LOGGER_NAME)

    cfg = _warehouse_config_from_env()
    locations = _parse_locations()
    start_date, end_date = _requested_date_range()

    batch_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)

    session = _requests_session()

    log.info(
        "Starting ingestion batch_id=%s locations=%s start=%s end=%s",
        batch_id,
        len(locations),
        start_date,
        end_date,
    )

    http_success = 0
    http_failure = 0
    total_bytes = 0
    per_city_metrics: list[dict[str, Any]] = []

    with _connect_warehouse(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.ingestion_batches(
                  batch_id, source, started_at, requested_start, requested_end, locations, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(batch_id),
                    "open-meteo-archive",
                    started_at,
                    start_date,
                    end_date,
                    Json(locations),
                    "RUNNING",
                ),
            )

        conn.commit()

        for loc in locations:
            ingestion_id = uuid.uuid4()

            params = {
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                "timezone": "UTC",
            }

            resp = session.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
            payload_bytes = len(resp.content or b"")
            total_bytes += payload_bytes

            payload: Any | None
            try:
                payload = resp.json() if resp.status_code == 200 else None
            except ValueError:
                payload = None

            if resp.status_code == 200 and payload is not None:
                http_success += 1
            else:
                http_failure += 1

            ingested_at = datetime.now(timezone.utc)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.open_meteo_responses(
                      ingestion_id, batch_id, ingested_at, source, city, latitude, longitude,
                      requested_start, requested_end, http_status, payload, payload_bytes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(ingestion_id),
                        str(batch_id),
                        ingested_at,
                        "open-meteo-archive",
                        loc["city"],
                        float(loc["latitude"]),
                        float(loc["longitude"]),
                        start_date,
                        end_date,
                        int(resp.status_code),
                        Json(payload) if payload is not None else None,
                        int(payload_bytes),
                    ),
                )

            conn.commit()

            per_city_metrics.append(
                {
                    "city": loc["city"],
                    "http_status": int(resp.status_code),
                    "payload_bytes": int(payload_bytes),
                }
            )
            log.info(
                "Fetched city=%s status=%s bytes=%s",
                loc["city"],
                resp.status_code,
                payload_bytes,
            )

        finished_at = datetime.now(timezone.utc)
        metrics_df = pd.DataFrame(per_city_metrics)
        if not metrics_df.empty:
            status_counts = metrics_df["http_status"].value_counts().to_dict()
            log.info("Ingestion HTTP status distribution: %s", status_counts)

        status = "SUCCESS" if http_failure == 0 else "PARTIAL_FAILURE"

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE raw.ingestion_batches
                SET finished_at=%s,
                    status=%s,
                    http_success_count=%s,
                    http_failure_count=%s,
                    total_payload_bytes=%s
                WHERE batch_id=%s
                """,
                (
                    finished_at,
                    status,
                    http_success,
                    http_failure,
                    total_bytes,
                    str(batch_id),
                ),
            )

        conn.commit()

    log.info(
        "Finished ingestion batch_id=%s status=%s success=%s failure=%s total_bytes=%s",
        batch_id,
        status,
        http_success,
        http_failure,
        total_bytes,
    )

    if http_success == 0:
        raise RuntimeError("Ingestion failed: no successful API responses")

    return str(batch_id)


if __name__ == "__main__":
    print(run_ingestion())
