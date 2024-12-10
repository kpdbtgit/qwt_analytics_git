{{
    config(
        materialized='table',
        schema='transforming'
    )
}}

select 
emp.EMPID,
emp."Last Name",
emp."First Name",
emp.title,
emp."Hire Date",
emp.EXTENSION,
iff(mgr."First Name" is NULL,emp."First Name",mgr."First Name") as manager,
emp."Year Salary",
lkp.officecity,
lkp.officecountry
from
{{ ref('stg_employees') }} as emp 
left join
{{ ref('stg_employees') }} as mgr 
on emp."Reports To"=mgr.EMPID
inner join
{{ ref('lkp_offices') }} as lkp 
on emp.OFFICE=lkp.OFFICE

