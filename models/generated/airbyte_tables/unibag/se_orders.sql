{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "unibag",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('se_orders_ab3') }}
select
    _id,
    {{ adapter.quote('client') }}::json->>'code' AS client_code,
    cast({{ adapter.quote('client') }}::json->'fee'->>'total' AS numeric) AS client_fee_total,
    cast({{ adapter.quote('client') }}::json->'fee'->>'shipping' AS numeric) AS client_fee_shipping,
    cast({{ adapter.quote('client') }}::json->'fee'->>'other' AS numeric) AS client_fee_other,
    {{ adapter.quote('courier') }}::json->>'code' AS courier_code,
    cast({{ adapter.quote('courier') }}::json->'fee'->>'total' AS numeric) AS courier_fee_total,
    cast({{ adapter.quote('courier') }}::json->'fee'->>'shipping' AS numeric) AS courier_fee_shipping,
    cast({{ adapter.quote('courier') }}::json->'fee'->>'other' AS numeric) AS courier_fee_other,
    cast(weight AS numeric) AS weight,
    volume,
    code,
    status,
    cast({{ adapter.quote('value') }} AS numeric) AS value,
    createdat::timestamp AS created_at,
    updatedat::timestamp AS updated_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_se_orders_hashid
from {{ ref('se_orders_ab3') }}
-- se_orders from {{ source('unibag', '_airbyte_raw_se_orders') }}
where 1 = 1

