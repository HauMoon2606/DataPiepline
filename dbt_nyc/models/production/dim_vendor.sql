{{ config(materialized = 'table', schema = 'production')}}
with vendor_tmp as(
    select distinct 
        vendor_id
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
        and 
        year = '2023'
)

select
    {{dbt_utils.generate_surrogate_key(['vendor_id'])}} as vendor_key,
    vendor_id,
    {{ get_vendor_description('vendor_id')}} as vendor_name
from 
    vendor_tmp
where 
    vendor_id is not null
    and
    cast(vendor_id as integer) < 3
order by 
    vendor_id asc
