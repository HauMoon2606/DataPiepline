{{config(materialized = 'table')}}

with payment_staing as (
    select distinct
        payment_type_id
    from
        staging.nyc_taxi
    where
        year = '2023'
)
select 
    {{dbt_utils.generate_surrogate_key(['payment_type_id'])}} as payment_type_key,
    payment_type_id,
    {{get_payment_description('payment_type_id')}} as payment_description
from 
    payment_staing
where 
    payment_type_id is not null
order by 
    payment_type_id asc