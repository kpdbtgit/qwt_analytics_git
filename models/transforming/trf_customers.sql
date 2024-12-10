{{
    config(
        materialized='table',
        schema='transforming'

    )
}}

select 
c.CUSTOMERID,
c.COMPANYNAME, 
c.CONTACTNAME,
c.CITY,
c.COUNTRY, 
d.divisionname,
c.ADDRESS,
c.FAX, 
c.PHONE, 
c.POSTALCODE, 
IFF(c.state = '', 'NA', c.state) as statename
from
{{ ref('stg_customers') }} as c 
inner join 
{{ ref('lkp_divisions') }} as d 
on c.DIVISIONID=d.DIVISIONID