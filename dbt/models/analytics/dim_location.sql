with base as (
    select distinct
        city,
        latitude,
        longitude
    from {{ ref('stg_weather_hourly') }}
)

select
    md5(city || '|' || latitude::text || '|' || longitude::text) as location_id,
    city,
    latitude,
    longitude
from base
