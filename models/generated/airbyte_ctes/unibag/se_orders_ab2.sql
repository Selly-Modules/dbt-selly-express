{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_unibag",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('se_orders_ab1') }}
select
    cast({{ adapter.quote('to') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('to') }},
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(cod as {{ dbt_utils.type_float() }}) as cod,
    cast(code as {{ dbt_utils.type_string() }}) as code,
    cast({{ adapter.quote('from') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('from') }},
    cast(note as {{ dbt_utils.type_string() }}) as note,
    cast({{ adapter.quote('value') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('value') }},
    cast(client as {{ dbt_utils.type_string() }}) as client,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(volume as {{ dbt_utils.type_string() }}) as volume,
    cast(weight as {{ dbt_utils.type_float() }}) as weight,
    cast(courier as {{ dbt_utils.type_string() }}) as courier,
    cast(distance as {{ dbt_utils.type_float() }}) as distance,
    cast(createdat as {{ dbt_utils.type_string() }}) as createdat,
    cast(updatedat as {{ dbt_utils.type_string() }}) as updatedat,
    cast(updatedby as {{ dbt_utils.type_string() }}) as updatedby,
    cast(itemvolume as {{ dbt_utils.type_float() }}) as itemvolume,
    cast(searchstring as {{ dbt_utils.type_string() }}) as searchstring,
    extraservices,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('se_orders_ab1') }}
-- se_orders
where 1 = 1

