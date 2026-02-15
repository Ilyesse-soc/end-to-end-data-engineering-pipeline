with w as (
    select
        batch_id,
        city,
        latitude,
        longitude,
        ts_utc,
        temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        source_ingested_at,
        loaded_at
    from {{ ref('stg_weather_hourly') }}
)

select
    md5(city || '|' || latitude::text || '|' || longitude::text) as location_id,
    ts_utc,
    ts_utc::date as date_id,
    temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    batch_id,
    source_ingested_at,
    loaded_at
from w
