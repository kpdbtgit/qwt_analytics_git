import snowflake.snowpark.functions as F

def model(dbt, session):
    dbt.config(materialized = "incremental",unique_key= "orderid")
    df = dbt.source("qwt_raw", "orders")

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_from_this = f"select max(orderdate) from {dbt.this}"
        df = df.filter(df.orderdate > session.sql(max_from_this).collect()[0][0])

    return df