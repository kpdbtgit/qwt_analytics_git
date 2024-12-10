import snowflake.snowpark.functions as F

def avgsale(x,y):
    return y/x

def model(dbt, session):

    dbt.config(materialized = "table",schema = "reporting")
    dim_cust_df = dbt.ref("dim_customers")
    fct_ord_df= dbt.ref("fct_orders")

    cust_ord_df= (  fct_ord_df
                    .group_by('customerid')
                    .agg 
                    (
                    F.min(F.col('orderdate')).alias('first_order_date'),
                    F.max(F.col('orderdate')).alias('Recent_order_date'),
                    F.count(F.col('orderid')).alias('order_count'),
                    F.sum(F.col('linesaleamount')).alias('sales')

                    )
                )

    final_df = (    dim_cust_df
                    .join(cust_ord_df,dim_cust_df.customerid == cust_ord_df.customerid, 'left')
                    .select
                    (
                    dim_cust_df.companyname.alias('companyname'),
                    dim_cust_df.contactname.alias('contactname'),
                    cust_ord_df.first_order_date.alias('first_order_date'),
                    cust_ord_df.Recent_order_date.alias('Recent_order_date'),
                    cust_ord_df.order_count.alias('order_count'),
                    cust_ord_df.sales.alias('sales')
                    )
                )

    final_df=final_df.withColumn("avg_salesvalue",avgsale(final_df["order_count"],final_df["sales"]))

    return final_df