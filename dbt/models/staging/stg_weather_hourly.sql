with src as (
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
    from {{ source('staging', 'weather_hourly') }}
)

select * from src
