{{config(materialized = 'table', schema = 'production')}}
with service_type_tmp as (
    select distinct 
        service_type
    from
        staging.nyc_taxi
    where 
        year = '2023'
)
select 
    {{dbt_utils.generate_surrogate_key(['service_type'])}} as service_type_key,
    service_type as service_type_id,
    {{get_service_description('service_type')}} as service_description
from 
    service_type_tmp
where
    service_type is not null
order by
    service_type asc