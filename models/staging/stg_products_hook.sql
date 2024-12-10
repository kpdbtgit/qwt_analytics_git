{{
    config(
        materialized='table',
        pre_hook="use warehouse loading_wh;",
        post_hook="create or replace transient table stg_prd_cloned clone stg_products;" ,
        schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')
    )
}}

select * 
from
{{ source('qwt_raw', 'products') }}