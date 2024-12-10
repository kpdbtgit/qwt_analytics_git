select
orderid,
sum(linesaleamount) as sales
from
{{ ref('fct_orders') }}
group by orderid
having sales < 0