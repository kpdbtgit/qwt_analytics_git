{{
    config(
        materialized='view',
        schema='reporting'
    )
}}

select 
c.companyname,
c.contactname,
min(o.orderdate) as first_order_date,
max(o.orderdate) as recent_order_date,
count(o.orderid)  as order_count,
sum(f.linesaleamount) as sales,
avg (f.margin) as avg_margin
from 
{{ ref('fct_orders') }} f 
inner join
{{ ref('dim_customers') }} c
on f.customerid =c.customerid
inner join
{{ ref('stg_orders') }} o
on f.orderid=o.orderid
group by c.companyname,c.contactname