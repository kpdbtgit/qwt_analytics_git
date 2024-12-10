{{
    config(
        materialized='table',
        schema='transforming'
    )
}}

select 
o.orderid,
od.lineno,
o.CUSTOMERID,
o.employeeid,
o.shipperid,
od.productid,
o.freight,
od.quantity,
od.unitprice,
od.discount,
o.orderdate,
(od.UnitPrice * od.Quantity) * (1-od.Discount) as linesaleamount,
p.unitcost * od.quantity as costofgoodssold,
((od.UnitPrice * od.Quantity)* (1-od.Discount)) - (p.UnitCost * od.Quantity) as margin
from
{{ ref('stg_orders') }} as o 
inner join
{{ ref('stg_orderdetails') }} as od 
on o.orderid=od.orderid
inner join
{{ ref('stg_products') }} as p 
on od.productid=p.productid