{{config(meterialized = 'table', schema = 'production')}}
with rate_code_tmp as(
    select distinct 
        rate_code_id
    from 
        staging.nyc_taxi
    where 
        rate_code_id is not null
        and
        year='2023'
)

select 
    {{dbt_utils.generate_surrogate_key(['rate_code_id'])}} as rate_code_key,
    rate_code_id,
    {{get_rate_code_description('rate_code_id')}} as rate_code_description
from 
    rate_code_tmp
where 
    rate_code_id is not null
    and
    cast(rate_code_id as integer) <7
order by 
    rate_code_id asc
