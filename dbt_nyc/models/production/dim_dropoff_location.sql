{{config (materialized = 'table', schema = 'production')}}
with dropoff_location_tmp as(
    select distinct
        dropoff_location_id,
        dropoff_latitude,
        dropoff_longitude
    from 
        staging.nyc_taxi
    where
        year = '2023'

)
select
    *
from    
    dropoff_location_tmp
where
    dropoff_location_id is not null
order by
    dropoff_location_id asc