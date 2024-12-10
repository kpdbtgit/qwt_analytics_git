{{
    config(
        materialized='incremental',
        unique_key='orderid',
        schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')
    )
}}

select * from {{ source('qwt_raw', 'orders') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where orderdate > (select max(orderdate) from {{ this }}) 
{% endif %}