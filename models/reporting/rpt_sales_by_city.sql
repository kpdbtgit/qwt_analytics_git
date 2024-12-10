{{
    config(
        materialized='view',
        schema='reporting'
    )
}}

select 
d.COMPANYNAME,
d.CONTACTNAME,
d.city,
sum(f.linesaleamount) as sales ,
sum(f.quantity) as quantity,
avg(f.margin) as margin
from
{{ ref('dim_customers') }} as d
inner join
{{ ref('fct_orders') }} as f
on d.customerid=f.customerid 
where
d.city={{ var('vcity',"'London'") }}
group by d.COMPANYNAME,d.CONTACTNAME,d.city
order by sales desc