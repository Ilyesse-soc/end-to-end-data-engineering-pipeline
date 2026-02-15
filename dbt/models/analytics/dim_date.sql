with base as (
    select distinct
        (ts_utc::date) as date_id
    from {{ ref('stg_weather_hourly') }}
)

select
    date_id,
    extract(isodow from date_id)::int as iso_day_of_week,
    extract(week from date_id)::int as iso_week,
    extract(month from date_id)::int as month,
    extract(year from date_id)::int as year
from base
