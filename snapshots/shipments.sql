{% snapshot shipments_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key="orderid||'-'||lineno",
            strategy='timestamp',
            updated_at='SHIPMENTDATE'
        )
    }}

    select * from {{ ref('stg_shipments') }}
 {% endsnapshot %}