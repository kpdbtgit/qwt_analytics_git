{{ config(materialized= 'table', schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select 
EMPID,
"Last Name",
"First Name",
TITLE,
"Hire Date",
OFFICE,
EXTENSION,
"Reports To",
"Year Salary"
from 
{{ source('qwt_raw', 'employees') }}