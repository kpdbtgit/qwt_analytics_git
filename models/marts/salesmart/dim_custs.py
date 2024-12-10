def model(dbt, session):
    dbt.config(schema = "salesmart")
    df = dbt.ref("trf_customers")

    return df