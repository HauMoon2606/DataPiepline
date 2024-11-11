{{config(materialized = 'table')}}
with pickup_location_tmp as(
    select distinct 
        pickup_location_id,
        pickup_latitude,
        pickup_longitude
    from 
        staging.nyc_taxi
    where 
        year = '2023'
)

select
    *
from 
    pickup_location_tmp
where
    pickup_location_id is not null
order by 
    pickup_location_id asc