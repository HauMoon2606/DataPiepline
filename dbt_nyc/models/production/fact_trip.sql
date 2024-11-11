{{ config (materialized = 'table')}}
with trip_tmp as (
    select 
        {{dbt_utils.generate_surrogate_key(['f.vendor_id','f.rate_code_id','f.pickup_location_id',
        'f.dropoff_location_id','f.payment_type_id','f.service_type','f.pickup_datetime','f.dropoff_datetime'])}} as trip_key,
        dv.vendor_key,
        dr.rate_code_key,
        dp.payment_type_key,
        f.pickup_location_id,
        f.dropoff_location_id,
        f.service_type as service_type_id,
        -- timestamp
        f.pickup_datetime,
        f.dropoff_datetime,
        --trip info
        f.passenger_count,
        f.trip_distance,
        --payment info
        f.extra,
        f.mta_tax,
        f.fare_amount,
        f.tip_amount,
        f.tolls_amount,
        f.total_amount,
        f.improvement_surcharge,
        f.congestion_surcharge
    from
        staging.nyc_taxi as f
    join 
        production.dim_vendor as dv  on f.vendor_id = dv.vendor_id
    join
        production.dim_rate_code as dr on f.rate_code_id = dr.rate_code_id
    join 
        production.dim_payment as dp on f.payment_type_id = dp.payment_type_id
    join 
        production.dim_service as ds on f.service_type = ds.service_type_id
    where year = '2023'
)
select 
    *
from 
    trip_tmp