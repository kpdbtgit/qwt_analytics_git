{{
    config(
        materialized='table',
        schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')
    )
}}

select 
ORDERID,
LINENO,
SHIPPERID,
CUSTOMERID,
PRODUCTID,
EMPLOYEEID,
split_part(SHIPMENTDATE,' ',0):: date as SHIPMENTDATE,
STATUS,
from
{{ source('qwt_raw', 'shipments') }}