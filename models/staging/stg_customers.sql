{{ config(materialized= 'table', schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select 
CUSTOMERID, 
COMPANYNAME,
CONTACTNAME,
CITY,
COUNTRY,
DIVISIONID,
ADDRESS,
FAX,
PHONE,
POSTALCODE,
STATEPROVINCE as state
from 
{{ source('qwt_raw', 'customers') }}