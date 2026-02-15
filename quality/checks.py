import json
import logging
import logging.config
import os
import uuid
from dataclasses import dataclass
from datetime import datetime

import polars as pl
import psycopg2
from psycopg2.extras import Json
import yaml


LOGGER_NAME = "pipeline.quality"


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


def run_quality_checks(batch_id: str, parquet_path: str) -> None:
    _setup_logging()
    log = logging.getLogger(LOGGER_NAME)

    cfg = _warehouse_config_from_env()

    if not os.path.exists(parquet_path):
        raise FileNotFoundError(parquet_path)

    df = pl.read_parquet(parquet_path)

    total_rows = df.height
    if total_rows == 0:
        raise RuntimeError("Quality failed: empty dataset")

    required_cols = [
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
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Quality failed: missing columns {missing}")

    null_counts = {c: int(df.select(pl.col(c).null_count()).item()) for c in required_cols}

    dup_df = (
        df.group_by(["city", "ts_utc"])  # type: ignore[attr-defined]
        .len()
        .filter(pl.col("len") > 1)
    )
    duplicate_rows = int(dup_df.select((pl.col("len") - 1).sum()).item() or 0)

    range_violations = {
        "temperature_c": int(df.filter((pl.col("temperature_c") < -90) | (pl.col("temperature_c") > 60)).height),
        "relative_humidity_pct": int(
            df.filter((pl.col("relative_humidity_pct") < 0) | (pl.col("relative_humidity_pct") > 100)).height
        ),
        "precipitation_mm": int(df.filter((pl.col("precipitation_mm") < 0) | (pl.col("precipitation_mm") > 500)).height),
        "wind_speed_kmh": int(df.filter((pl.col("wind_speed_kmh") < 0) | (pl.col("wind_speed_kmh") > 200)).height),
    }

    has_nulls = any(v > 0 for v in null_counts.values())
    has_dupes = duplicate_rows > 0
    has_range_issues = any(v > 0 for v in range_violations.values())

    status = "PASS" if (not has_nulls and not has_dupes and not has_range_issues) else "FAIL"

    check_run_id = uuid.uuid4()

    details = {
        "null_counts": null_counts,
        "duplicate_rows": duplicate_rows,
        "range_violations": range_violations,
    }

    with _connect_warehouse(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.quality_check_results(
                  check_run_id, batch_id, parquet_path, status, total_rows, duplicate_rows,
                  null_violations, range_violations, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(check_run_id),
                    batch_id,
                    parquet_path,
                    status,
                    int(total_rows),
                    int(duplicate_rows),
                    Json(null_counts),
                    Json(range_violations),
                    Json(details),
                ),
            )
        conn.commit()

    log.info(
        "Quality status=%s batch_id=%s rows=%s dupes=%s nulls=%s range=%s",
        status,
        batch_id,
        total_rows,
        duplicate_rows,
        json.dumps({k: v for k, v in null_counts.items() if v > 0}),
        json.dumps({k: v for k, v in range_violations.items() if v > 0}),
    )

    if status != "PASS":
        raise RuntimeError("Quality checks failed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--parquet-path", required=True)
    args = parser.parse_args()

    run_quality_checks(args.batch_id, args.parquet_path)
