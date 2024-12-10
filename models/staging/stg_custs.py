def model(dbt, session):
    df = dbt.source("qwt_raw", "customers")

    return df